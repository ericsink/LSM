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

struct ConnStuff {
    conn: sqlite3::DatabaseConnection,
}

impl ConnStuff {
    fn base_create_collection(&mut self, db: &str, coll: &str, options: BsonValue) -> elmo::Result<bool> {
        // TODO
        Ok(true)
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

impl elmo::ElmoStorage for ConnStuff {
    fn createCollection(&mut self, db: &str, coll: &str, options: BsonValue) -> elmo::Result<bool> {
        self.begin_tx();
        let r = self.base_create_collection(db, coll, options);
        self.finish_tx(r)
    }

    //fn beginWrite(&self, db: &str, coll: &str) -> elmo::Result<Box<elmo::ElmoWriter>> {
    //}
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

fn connect() -> elmo::Result<Box<elmo::ElmoStorage>> {
    let conn = try!(base_connect().map_err(elmo::wrap_err));
    let c = ConnStuff {
        conn: conn
    };
    Ok(box c)
}


