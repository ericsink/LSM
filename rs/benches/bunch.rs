
#![feature(test)]

extern crate lsm;
extern crate test;

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

#[bench]
fn bunch(b: &mut test::Bencher) {
    fn f() -> std::io::Result<bool> {
        //println!("running");
        let mut db = try!(lsm::db::new(&tempfile("bunch"), lsm::DEFAULT_SETTINGS));

        const NUM : usize = 10000;

        let mut a = Vec::new();
        for i in 0 .. 10 {
            let g = try!(db.WriteSegmentFromSortedSequence(lsm::GenerateNumbers {cur: i * NUM, end: (i+1) * NUM, step: i+1}));
            a.push(g);
        }
        try!(db.commitSegments(a.clone()));
        let g3 = try!(db.merge(a));
        try!(db.commitMerge(g3));

        let res : std::io::Result<bool> = Ok(true);
        res
    }
    b.iter(|| assert!(f().is_ok()) );
}

