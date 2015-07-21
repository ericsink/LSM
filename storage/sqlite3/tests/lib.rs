
extern crate elmo;
extern crate storage_sqlite3;

#[test]
fn just_connect() {
    fn f() -> elmo::Result<()> {
        let db = try!(storage_sqlite3::connect());
        drop(db);
        Ok(())
    }
    let r = f();
    println!("{:?}", r);
    assert!(r.is_ok());
}

