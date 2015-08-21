extern crate sqlite3;

use sqlite3::{
    DatabaseConnection,
    SqliteResult,
};

fn convenience_exec() -> SqliteResult<DatabaseConnection> {
    let mut conn = try!(DatabaseConnection::in_memory());

    try!(conn.exec("
       create table items (
                   id integer,
                   description varchar(40),
                   price integer
                   )"));

    Ok(conn)
}

fn typical_usage(conn: &mut DatabaseConnection) -> SqliteResult<String> {
    {
        let mut stmt = try!(conn.prepare(
            "insert into items (id, description, price)
           values (1, 'stuff', 10)"));
        match try!(stmt.step()) {
            None => (),
            Some(_) => panic!("row from insert?!"),
        };
    }
    assert_eq!(conn.changes(), 1);
    assert_eq!(conn.last_insert_rowid(), 1);
    {
        let mut stmt = try!(conn.prepare(
            "select * from items"));
        let res = match stmt.step() {
            Ok(Some(ref mut row1)) => {
                let id = row1.column_int(0);
                let desc_opt = row1.column_text(1).expect("desc_opt should be non-null");
                let price = row1.column_int(2);

                assert_eq!(id, 1);
                assert_eq!(desc_opt, format!("stuff"));
                assert_eq!(price, 10);

                Ok(format!("row: {}, {}, {}", id, desc_opt, price))
            },
            Err(oops) => panic!(oops),
            Ok(None) => panic!("where did our row go?")
        };
        res
    }
}

pub fn main() {
    match convenience_exec() {
        Ok(ref mut db) => {
            match typical_usage(db) {
                Ok(txt) => println!("item: {}", txt),
                Err(oops) => {
                    panic!("error: {:?} msg: {}", oops,
                           db.errmsg())
                }
            }
        },
        Err(oops) => panic!(oops)
    }
}
