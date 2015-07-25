
extern crate misc;
extern crate bson;
extern crate elmo;
extern crate storage_sqlite3;

use bson::BsonValue;

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
        try!(db.unprepare_write());
        drop(db);
        Ok(())
    }
    let r = f();
    println!("{:?}", r);
    assert!(r.is_ok());
}

#[test]
fn insert() {
    fn f() -> elmo::Result<()> {
        let mut db = try!(storage_sqlite3::connect(&misc::tempfile("insert")));
        try!(db.prepare_write("foo", "bar"));

        let mut pairs = Vec::new();
        pairs.push((String::from("_id"), BsonValue::BString(misc::tid())));
        pairs.push((String::from("ok"), BsonValue::BInt32(0)));
        let doc = BsonValue::BDocument(pairs);

        try!(db.insert(doc));
        try!(db.unprepare_write());
        drop(db);
        Ok(())
    }
    let r = f();
    println!("{:?}", r);
    assert!(r.is_ok());
}

