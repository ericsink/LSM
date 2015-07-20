/*
    Copyright 2014-2015 Zumero, LLC

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#![feature(core)]
#![feature(collections)]
#![feature(box_syntax)]
#![feature(convert)]
#![feature(collections_drain)]
#![feature(associated_consts)]
#![feature(vec_push_all)]
#![feature(clone_from_slice)]
#![feature(drain)]
#![feature(iter_arith)]

// TODO turn the following warnings back on later
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

extern crate misc;

use misc::endian;
use misc::bufndx;
use misc::varint;

extern crate bson;
use bson::BsonValue;

extern crate elmo;

use std::io;
use std::io::Seek;
use std::io::Read;
use std::io::Write;
use std::io::SeekFrom;
use std::cmp::Ordering;
use std::fs::File;
use std::fs::OpenOptions;
use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;

extern crate sqlite3;

struct MyConn {
    conn: sqlite3::DatabaseConnection,
}

struct MyWriter {
    x: i32
}

impl MyConn {
    fn getTableNameForCollection(db: &str, coll: &str) -> String { 
        // TODO cleanse?
        format!("docs.{}.{}", db, coll) 
    }

    fn getCollectionOptions(&self, db: &str, coll: &str) -> elmo::Result<Option<BsonValue>> {
        let mut stmt = try!(self.conn.prepare("SELECT options FROM \"collections\" WHERE dbName=? AND collName=?").map_err(elmo::wrap_err));
        stmt.bind_text(1,db);
        stmt.bind_text(2,coll);
        // TODO alternative to step() and SQLITE_ROW/DONE, etc
        let mut r = stmt.execute();
        match try!(r.step().map_err(elmo::wrap_err)) {
            None => Ok(None),
            Some(r) => {
                match r.column_blob(0) {
                    Some(b) => {
                        let v = try!(BsonValue::from_bson(&b));
                        Ok(Some(v))
                    },
                    None => {
                        // this should not be null
                        // TODO corrupt file
                        // TODO or is this column NOT NULL?
                        // which means this could be assert not reached
                        Err(elmo::Error::Misc("wrong"))
                    },
                }
            },
        }
    }

    fn base_create_collection(&mut self, db: &str, coll: &str, options: BsonValue) -> elmo::Result<bool> {
        match try!(self.getCollectionOptions(db, coll)) {
            Some(_) => Ok(false),
            None => {
                let mut baOptions = Vec::new();
                options.to_bson(&mut baOptions);
                let mut stmt = try!(self.conn.prepare("INSERT INTO \"collections\" (dbName,collName,options) VALUES (?,?,?)").map_err(elmo::wrap_err));
                stmt.bind_text(1,db);
                stmt.bind_text(2,coll);
                stmt.bind_blob(3,&baOptions);
                // TODO same as step_done?
                stmt.execute();
                let collTable = Self::getTableNameForCollection(db, coll);
                try!(self.conn.exec(&format!("CREATE TABLE \"{}\" (did INTEGER PRIMARY KEY, bson BLOB NOT NULL)", collTable)).map_err(elmo::wrap_err));
                // TODO match bson.tryGetValueForKey options "autoIndexId" with
                Ok(true)
            },
        }
    }

    // TODO not sure this func is worth the trouble
    fn exec(&mut self, sql: &str) -> elmo::Result<()> {
        self.conn.exec(sql).map_err(elmo::wrap_err)
    }

    fn begin_tx(&mut self) -> elmo::Result<()> {
        try!(self.conn.exec("BEGIN TRANSACTION").map_err(elmo::wrap_err));
        Ok(())
    }

    fn finish_tx<T>(&mut self, r: elmo::Result<T>) -> elmo::Result<T> {
        if r.is_ok() {
            try!(self.conn.exec("COMMIT TRANSACTION").map_err(elmo::wrap_err));
            r
        } else {
            self.conn.exec("ROLLBACK TRANSACTION");
            r
        }
    }
}

impl elmo::StorageConnection for MyConn {
    fn createCollection(&mut self, db: &str, coll: &str, options: BsonValue) -> elmo::Result<bool> {
        self.begin_tx();
        let r = self.base_create_collection(db, coll, options);
        self.finish_tx(r)
    }

    fn beginWrite(&self, db: &str, coll: &str) -> elmo::Result<Box<elmo::StorageWriter>> {
        let w = MyWriter {
            x: 5
        };

        Ok(box w)
    }
}

impl elmo::StorageWriter for MyWriter {
    fn insert(&self, v: BsonValue) -> elmo::Result<()> {
        Ok(())
    }

    fn commit(&self) -> elmo::Result<()> {
        Ok(())
    }

}

fn base_connect() -> sqlite3::SqliteResult<sqlite3::DatabaseConnection> {
    // TODO allow a different filename to be specified
    let access = sqlite3::access::ByFilename { flags: sqlite3::access::flags::OPEN_READWRITE, filename: "elmodata.db" };
    let mut conn = try!(sqlite3::DatabaseConnection::new(access));
    try!(conn.exec("PRAGMA journal_mode=WAL"));
    try!(conn.exec("PRAGMA foreign_keys=ON"));
    try!(conn.exec("CREATE TABLE IF NOT EXISTS \"collections\" (dbName TEXT NOT NULL, collName TEXT NOT NULL, options BLOB NOT NULL, PRIMARY KEY (dbName,collName))"));
    try!(conn.exec("CREATE TABLE IF NOT EXISTS \"indexes\" (dbName TEXT NOT NULL, collName TEXT NOT NULL, ndxName TEXT NOT NULL, spec BLOB NOT NULL, options BLOB NOT NULL, PRIMARY KEY (dbName, collName, ndxName), FOREIGN KEY (dbName,collName) REFERENCES \"collections\" ON DELETE CASCADE ON UPDATE CASCADE, UNIQUE (spec,dbName,collName))"));

    Ok(conn)
}

fn connect() -> elmo::Result<Box<elmo::StorageConnection>> {
    let conn = try!(base_connect().map_err(elmo::wrap_err));
    let c = MyConn {
        conn: conn
    };
    Ok(box c)
}


