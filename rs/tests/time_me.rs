
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

#[test]
#[ignore]
fn time_me() {
    fn f() -> lsm::Result<bool> {
        //println!("running");
        let db = try!(lsm::db::new(tempfile("time_me"), lsm::DEFAULT_SETTINGS));

        const NUM : usize = 100000;

        let mut a = Vec::new();
        for i in 0 .. 10 {
            let g = try!(db.WriteSegmentFromSortedSequence(lsm::GenerateNumbers {cur: i * NUM, end: (i+1) * NUM, step: i+1}));
            a.push(g);
        }
        {
            let lck = try!(db.GetWriteLock());
            try!(lck.commitSegments(a.clone()));
        }
        let g3 = try!(db.merge(a));
        {
            let lck = try!(db.GetWriteLock());
            try!(lck.commitMerge(g3));
        }

        let res : lsm::Result<bool> = Ok(true);
        res
    }
    assert!(f().is_ok());
}

