
extern crate lsm;

fn hack() -> std::io::Result<bool> {
    let DefaultSettings = 
        lsm::DbSettings
        {
            AutoMergeEnabled : true,
            AutoMergeMinimumPages : 4,
            DefaultPageSize : 4096,
            PagesPerBlock : 256,
        };

    let mut db = try!(lsm::Database::db::new("data.bin", DefaultSettings));

    let mut a = Vec::new();
    for i in 0 .. 10 {
        let g = try!(db.WriteSegmentFromSortedSequence(lsm::GenerateNumbers {cur: i * 100000, end: (i+1) * 100000, step: i+1}));
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

#[test]
fn hack2() {
    hack();
}

