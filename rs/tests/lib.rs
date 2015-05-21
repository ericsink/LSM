
extern crate lsm;

fn tid() -> String {
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


