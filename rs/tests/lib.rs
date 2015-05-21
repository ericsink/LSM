
extern crate lsm;

fn tid() -> String {
    // TODO use the rand crate
    fn bytes() -> std::io::Result<[u8;16]> {
        use std::fs::OpenOptions;
        let mut f = try!(OpenOptions::new()
                .read(true)
                .open("/dev/urandom"));
        let mut ba = [0;16];
        try!(lsm::utils::ReadFully(&mut f, &mut ba));
        Ok(ba)
    }

    fn to_hex_string(ba: &[u8]) -> String {
        let strs: Vec<String> = ba.iter()
            .map(|b| format!("{:02X}", b))
            .collect();
        strs.connect("")
    }

    let ba = bytes().unwrap();
    to_hex_string(&ba)
}

fn tempfile(base: &str) -> String {
    std::fs::create_dir("tmp");
    let file = "tmp/".to_string() + base + "_" + &tid();
    file
}

fn to_utf8(s : &str) -> Box<[u8]> {
    s.to_string().into_bytes().into_boxed_slice()
}

fn from_utf8(a: Box<[u8]>) -> String {
    //let k = csr.Key();
    //let k = std::str::from_utf8(&k).unwrap();
    let k = std::string::String::from_utf8(a.into_iter().map(|b| *b).collect()).unwrap();
    k
}

fn insert_pair_string_string(d: &mut std::collections::HashMap<Box<[u8]>,Box<[u8]>>, k:&str, v:&str) {
    d.insert(to_utf8(k), to_utf8(v));
}

fn insert_pair_string_blob(d: &mut std::collections::HashMap<Box<[u8]>,lsm::Blob>, k:&str, v:lsm::Blob) {
    d.insert(to_utf8(k), v);
}

fn count_keys_forward(csr: &mut lsm::ICursor) -> usize {
    let mut r = 0;
    csr.First();
    while csr.IsValid() {
        r = r + 1;
        csr.Next();
    }
    r
}

fn count_keys_backward(csr: &mut lsm::ICursor) -> usize {
    let mut r = 0;
    csr.Last();
    while csr.IsValid() {
        r = r + 1;
        csr.Prev();
    }
    r
}

#[test]
fn hack() {
    fn f() -> std::io::Result<bool> {
        let mut db = try!(lsm::Database::db::new(&tempfile("hack"), lsm::DefaultSettings));

        const NUM : usize = 10000;

        let mut a = Vec::new();
        for i in 0 .. 10 {
            let g = try!(db.WriteSegmentFromSortedSequence(lsm::GenerateNumbers {cur: i * NUM, end: (i+1) * NUM, step: i+1}));
            println!("{:?}", g);
            a.push(g);
        }
        try!(db.commitSegments(a.clone()));
        println!("{}", "committed");
        let g3 = try!(db.merge(a));
        println!("{}", "merged");
        try!(db.commitMerge(g3));

        let res : std::io::Result<bool> = Ok(true);
        res
    }
    assert!(f().is_ok());
}

#[test]
fn empty_cursor() {
    fn f() -> std::io::Result<()> {
        let mut db = try!(lsm::Database::db::new(&tempfile("empty_cursor"), lsm::DefaultSettings));
        let mut csr = try!(db.OpenCursor());
        csr.First();
        assert!(!csr.IsValid());
        csr.Last();
        assert!(!csr.IsValid());
        Ok(())
    }
    assert!(f().is_ok());
}

#[test]
fn first_prev() {
    fn f() -> std::io::Result<()> {
        let mut db = try!(lsm::Database::db::new(&tempfile("first_prev"), lsm::DefaultSettings));
        let g = try!(db.WriteSegmentFromSortedSequence(lsm::GenerateNumbers {cur: 0, end: 100, step: 1}));
        try!(db.commitSegments(vec![g]));
        let mut csr = try!(db.OpenCursor());
        csr.First();
        assert!(csr.IsValid());
        csr.Prev();
        assert!(!csr.IsValid());
        Ok(())
    }
    assert!(f().is_ok());
}

#[test]
fn last_next() {
    fn f() -> std::io::Result<()> {
        let mut db = try!(lsm::Database::db::new(&tempfile("first_prev"), lsm::DefaultSettings));
        let g = try!(db.WriteSegmentFromSortedSequence(lsm::GenerateNumbers {cur: 0, end: 100, step: 1}));
        try!(db.commitSegments(vec![g]));
        let mut csr = try!(db.OpenCursor());
        csr.Last();
        assert!(csr.IsValid());
        csr.Next();
        assert!(!csr.IsValid());
        Ok(())
    }
    assert!(f().is_ok());
}

#[test]
fn seek() {
    fn f() -> std::io::Result<()> {
        let mut db = try!(lsm::Database::db::new(&tempfile("seek"), lsm::DefaultSettings));
        let g = try!(db.WriteSegmentFromSortedSequence(lsm::GenerateNumbers {cur: 0, end: 100, step: 1}));
        try!(db.commitSegments(vec![g]));
        let mut csr = try!(db.OpenCursor());
        csr.First();
        assert!(csr.IsValid());
        // TODO constructing the utf8 byte array seems convoluted

        let k = format!("{:08}", 42).into_bytes().into_boxed_slice();
        csr.Seek(&k, lsm::SeekOp::SEEK_EQ);
        assert!(csr.IsValid());

        let k = format!("{:08}", 105).into_bytes().into_boxed_slice();
        csr.Seek(&k, lsm::SeekOp::SEEK_EQ);
        assert!(!csr.IsValid());

        let k = format!("{:08}", 105).into_bytes().into_boxed_slice();
        csr.Seek(&k, lsm::SeekOp::SEEK_GE);
        assert!(!csr.IsValid());

        let k = format!("{:08}", 105).into_bytes().into_boxed_slice();
        csr.Seek(&k, lsm::SeekOp::SEEK_LE);
        assert!(csr.IsValid());
        // TODO get the key

        Ok(())
    }
    assert!(f().is_ok());
}

#[test]
fn lexographic() {
    fn f() -> std::io::Result<()> {
        let mut db = try!(lsm::Database::db::new(&tempfile("lexicographic"), lsm::DefaultSettings));
        let mut d = std::collections::HashMap::new();
        insert_pair_string_string(&mut d, "8", "");
        insert_pair_string_string(&mut d, "10", "");
        insert_pair_string_string(&mut d, "20", "");
        let g = try!(db.WriteSegment(d));
        try!(db.commitSegments(vec![g]));
        let mut csr = try!(db.OpenCursor());
        csr.First();
        assert!(csr.IsValid());
        assert_eq!(from_utf8(csr.Key()), "10");

        csr.Next();
        assert!(csr.IsValid());
        assert_eq!(from_utf8(csr.Key()), "20");

        csr.Next();
        assert!(csr.IsValid());
        assert_eq!(from_utf8(csr.Key()), "8");

        csr.Next();
        assert!(!csr.IsValid());

        // --------
        csr.Last();
        assert!(csr.IsValid());
        assert_eq!(from_utf8(csr.Key()), "8");

        csr.Prev();
        assert!(csr.IsValid());
        assert_eq!(from_utf8(csr.Key()), "20");

        csr.Prev();
        assert!(csr.IsValid());
        assert_eq!(from_utf8(csr.Key()), "10");

        csr.Prev();
        assert!(!csr.IsValid());

        Ok(())
    }
    assert!(f().is_ok());
}

#[test]
fn seek_cur() {
    fn f() -> std::io::Result<()> {
        let mut db = try!(lsm::Database::db::new(&tempfile("seek_cur"), lsm::DefaultSettings));
        let mut t1 = std::collections::HashMap::new();
        for i in 0 .. 100 {
            let sk = format!("{:03}", i);
            let sv = format!("{}", i);
            insert_pair_string_string(&mut t1, &sk, &sv);
        }
        let mut t2 = std::collections::HashMap::new();
        for i in 0 .. 1000 {
            let sk = format!("{:05}", i);
            let sv = format!("{}", i);
            insert_pair_string_string(&mut t2, &sk, &sv);
        }
        let g1 = try!(db.WriteSegment(t1));
        let g2 = try!(db.WriteSegment(t2));
        try!(db.commitSegments(vec![g1]));
        try!(db.commitSegments(vec![g2]));
        let mut csr = try!(db.OpenCursor());
        csr.Seek(&to_utf8("00001"), lsm::SeekOp::SEEK_EQ);;
        assert!(csr.IsValid());
        Ok(())
    }
    assert!(f().is_ok());
}

#[test]
fn weird() {
    fn f() -> std::io::Result<()> {
        let mut db = try!(lsm::Database::db::new(&tempfile("weird"), lsm::DefaultSettings));
        let mut t1 = std::collections::HashMap::new();
        for i in 0 .. 100 {
            let sk = format!("{:03}", i);
            let sv = format!("{}", i);
            insert_pair_string_string(&mut t1, &sk, &sv);
        }
        let mut t2 = std::collections::HashMap::new();
        for i in 0 .. 1000 {
            let sk = format!("{:05}", i);
            let sv = format!("{}", i);
            insert_pair_string_string(&mut t2, &sk, &sv);
        }
        let g1 = try!(db.WriteSegment(t1));
        let g2 = try!(db.WriteSegment(t2));
        try!(db.commitSegments(vec![g1]));
        try!(db.commitSegments(vec![g2]));
        let mut csr = try!(db.OpenCursor());
        csr.First();
        for _ in 0 .. 100 {
            csr.Next();
            assert!(csr.IsValid());
        }
        for _ in 0 .. 50 {
            csr.Prev();
            assert!(csr.IsValid());
        }
        for _ in 0 .. 100 {
            csr.Next();
            assert!(csr.IsValid());
            csr.Next();
            assert!(csr.IsValid());
            csr.Prev();
            assert!(csr.IsValid());
        }
        println!("{:?}", csr.Key());
        for _ in 0 .. 50 {
            let k = csr.Key();
            println!("{:?}", k);
            csr.Seek(&k, lsm::SeekOp::SEEK_EQ);;
            assert!(csr.IsValid());
            csr.Next();
            assert!(csr.IsValid());
        }
        for _ in 0 .. 50 {
            let k = csr.Key();
            csr.Seek(&k, lsm::SeekOp::SEEK_EQ);;
            assert!(csr.IsValid());
            csr.Prev();
            assert!(csr.IsValid());
        }
        for _ in 0 .. 50 {
            let k = csr.Key();
            csr.Seek(&k, lsm::SeekOp::SEEK_LE);;
            assert!(csr.IsValid());
            csr.Prev();
            assert!(csr.IsValid());
        }
        for _ in 0 .. 50 {
            let k = csr.Key();
            csr.Seek(&k, lsm::SeekOp::SEEK_GE);;
            assert!(csr.IsValid());
            csr.Next();
            assert!(csr.IsValid());
        }
        // got the following value from the debugger.
        // just want to make sure that it doesn't change
        // and all combos give the same answer.
        assert_eq!(from_utf8(csr.Key()), "00148");
        Ok(())
    }
    assert!(f().is_ok());
}

#[test]
fn no_le_ge_multicursor() {
    fn f() -> std::io::Result<()> {
        let mut db = try!(lsm::Database::db::new(&tempfile("no_le_ge_multicursor"), lsm::DefaultSettings));

        let mut t1 = std::collections::HashMap::new();
        insert_pair_string_string(&mut t1, "c", "3");
        insert_pair_string_string(&mut t1, "g", "7");
        let g1 = try!(db.WriteSegment(t1));
        try!(db.commitSegments(vec![g1]));

        let mut t2 = std::collections::HashMap::new();
        insert_pair_string_string(&mut t2, "e", "5");
        let g2 = try!(db.WriteSegment(t2));
        try!(db.commitSegments(vec![g2]));

        let mut csr = try!(db.OpenCursor());

        csr.Seek(&to_utf8("a"), lsm::SeekOp::SEEK_LE);;
        assert!(!csr.IsValid());

        csr.Seek(&to_utf8("d"), lsm::SeekOp::SEEK_LE);;
        assert!(csr.IsValid());

        csr.Seek(&to_utf8("f"), lsm::SeekOp::SEEK_GE);;
        assert!(csr.IsValid());

        csr.Seek(&to_utf8("h"), lsm::SeekOp::SEEK_GE);;
        assert!(!csr.IsValid());

        Ok(())
    }
    assert!(f().is_ok());
}

#[test]
fn empty_val() {
    fn f() -> std::io::Result<()> {
        let mut db = try!(lsm::Database::db::new(&tempfile("empty_val"), lsm::DefaultSettings));

        let mut t1 = std::collections::HashMap::new();
        insert_pair_string_string(&mut t1, "_", "");
        let g1 = try!(db.WriteSegment(t1));
        try!(db.commitSegments(vec![g1]));
        let mut csr = try!(db.OpenCursor());
        csr.Seek(&to_utf8("_"), lsm::SeekOp::SEEK_EQ);;
        assert!(csr.IsValid());
        assert_eq!(0, csr.ValueLength().unwrap());

        Ok(())
    }
    assert!(f().is_ok());
}

#[test]
fn delete_not_there() {
    fn f() -> std::io::Result<()> {
        let mut db = try!(lsm::Database::db::new(&tempfile("delete_not_there"), lsm::DefaultSettings));

        let mut t1 = std::collections::HashMap::new();
        insert_pair_string_string(&mut t1, "a", "1");
        insert_pair_string_string(&mut t1, "b", "2");
        insert_pair_string_string(&mut t1, "c", "3");
        insert_pair_string_string(&mut t1, "d", "4");
        let g1 = try!(db.WriteSegment(t1));
        try!(db.commitSegments(vec![g1]));

        let mut t2 = std::collections::HashMap::new();
        insert_pair_string_blob(&mut t2, "e", lsm::Blob::Tombstone);
        let g2 = try!(db.WriteSegment2(t2));
        try!(db.commitSegments(vec![g2]));

        let mut csr = try!(db.OpenCursor());
        assert_eq!(4, count_keys_forward(&mut *csr));
        assert_eq!(4, count_keys_backward(&mut *csr));

        Ok(())
    }
    assert!(f().is_ok());
}

#[test]
fn simple_tombstone() {
    fn f(del: &str) -> std::io::Result<()> {
        let mut db = try!(lsm::Database::db::new(&tempfile("simple_tombstone"), lsm::DefaultSettings));

        let mut t1 = std::collections::HashMap::new();
        insert_pair_string_string(&mut t1, "a", "1");
        insert_pair_string_string(&mut t1, "b", "2");
        insert_pair_string_string(&mut t1, "c", "3");
        insert_pair_string_string(&mut t1, "d", "4");
        let g1 = try!(db.WriteSegment(t1));
        try!(db.commitSegments(vec![g1]));

        let mut t2 = std::collections::HashMap::new();
        insert_pair_string_blob(&mut t2, del, lsm::Blob::Tombstone);
        let g2 = try!(db.WriteSegment2(t2));
        try!(db.commitSegments(vec![g2]));

        let mut csr = try!(db.OpenCursor());
        assert_eq!(3, count_keys_forward(&mut *csr));
        assert_eq!(3, count_keys_backward(&mut *csr));

        Ok(())
    }
    assert!(f("a").is_ok());
    assert!(f("b").is_ok());
    assert!(f("c").is_ok());
    assert!(f("d").is_ok());
}

