
extern crate misc;
extern crate bson;
extern crate elmo;
extern crate elmo_sqlite3;

#[test]
fn just_connect() {
    fn f() -> elmo::Result<()> {
        let db = try!(elmo_sqlite3::connect(&misc::tempfile("just_connect")));
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
        let db = try!(elmo_sqlite3::connect(&misc::tempfile("prepare_write")));
        {
            let tx = try!(db.begin_write());
            {
                let _p = try!(tx.get_collection_writer("foo", "bar"));
            }
        }
        Ok(())
    }
    let r = f();
    println!("{:?}", r);
    assert!(r.is_ok());
}

#[test]
fn insert() {
    fn f() -> elmo::Result<()> {
        let db = try!(elmo_sqlite3::connect(&misc::tempfile("insert")));
        {
            let tx = try!(db.begin_write());
            {
                let mut p = try!(tx.get_collection_writer("foo", "bar"));

                let mut doc = bson::Document::new_empty();
                doc.set_string("_id", misc::tid());
                doc.set_i32("ok", 0);

                try!(p.insert(&doc));
            }
            try!(tx.commit());
        }
        drop(db);
        Ok(())
    }
    let r = f();
    println!("{:?}", r);
    assert!(r.is_ok());
}

