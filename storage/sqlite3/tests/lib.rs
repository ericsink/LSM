
extern crate misc;
extern crate elmo;
extern crate storage_sqlite3;

#[test]
fn just_connect() {
    fn f() -> elmo::Result<()> {
        let db = try!(storage_sqlite3::connect(&misc::tempfile("just_connect")));
        drop(db);
        Ok(())
    }
    let r = f();
    println!("{:?}", r);
    assert!(r.is_ok());
}

#[test]
fn prepare_write() {
    fn f() -> elmo::Result<()> {
        let mut db = try!(storage_sqlite3::connect(&misc::tempfile("prepare_write")));
        try!(db.prepare_write("foo", "bar"));
        drop(db);
        Ok(())
    }
    let r = f();
    println!("{:?}", r);
    assert!(r.is_ok());
}

