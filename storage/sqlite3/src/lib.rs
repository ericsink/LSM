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

#![feature(box_syntax)]
#![feature(associated_consts)]

extern crate bson;
use bson::BsonValue;

extern crate elmo;

extern crate sqlite3;

struct MyStatements {
    insert: sqlite3::PreparedStatement,
    delete: sqlite3::PreparedStatement,
    find_rowid: Option<sqlite3::PreparedStatement>,
}

struct MyConn {
    conn: sqlite3::DatabaseConnection,
    statements: Option<MyStatements>,
}

impl MyConn {
    fn get_table_name_for_collection(db: &str, coll: &str) -> String { 
        // TODO cleanse?
        format!("docs.{}.{}", db, coll) 
    }

    fn get_table_name_for_index(db: &str, coll: &str, name: &str) -> String { 
        // TODO cleanse?
        format!("ndx.{}.{}.{}", db, coll, name) 
    }

    fn get_collection_options(&self, db: &str, coll: &str) -> elmo::Result<Option<BsonValue>> {
        let mut stmt = try!(self.conn.prepare("SELECT options FROM \"collections\" WHERE dbName=? AND collName=?").map_err(elmo::wrap_err));
        try!(stmt.bind_text(1, db).map_err(elmo::wrap_err));
        try!(stmt.bind_text(2, coll).map_err(elmo::wrap_err));
        // TODO this is apparently an alternative to step() and SQLITE_ROW/DONE, etc
        let mut r = stmt.execute();
        match try!(r.step().map_err(elmo::wrap_err)) {
            None => Ok(None),
            Some(r) => {
                let b = r.column_blob(0).expect("NOT NULL");
                let v = try!(BsonValue::from_bson(&b));
                Ok(Some(v))
            },
        }
    }

    fn create_index(&mut self, info: IndexInfo) -> elmo::Result<bool> {
        // TODO create_collection
        match try!(self.get_index_info(&info.db, &info.coll, &info.name)) {
            Some(already) => {
                if already.spec != info.spec {
                    // note that we do not compare the options.
                    // I think mongo does it this way too.
                    Err(elmo::Error::Misc("index already exists with different keys"))
                } else {
                    Ok(false)
                }
            },
            None => {
                // TODO if we already have a text index (where any of its spec keys are text)
                // then fail.

                let ba_spec = info.spec.to_bson_array();
                let ba_options = info.options.to_bson_array();
                let mut stmt = try!(self.conn.prepare("INSERT INTO \"indexes\" (dbName,collName,ndxName,spec,options) VALUES (?,?,?,?,?)").map_err(elmo::wrap_err));
                try!(stmt.bind_text(1, &info.db).map_err(elmo::wrap_err));
                try!(stmt.bind_text(2, &info.coll).map_err(elmo::wrap_err));
                try!(stmt.bind_text(3, &info.name).map_err(elmo::wrap_err));
                try!(stmt.bind_blob(4, &ba_spec).map_err(elmo::wrap_err));
                try!(stmt.bind_blob(5, &ba_options).map_err(elmo::wrap_err));
                let mut r = stmt.execute();
                match try!(r.step().map_err(elmo::wrap_err)) {
                    None => {
                        // TODO
                        let tbl_coll = Self::get_table_name_for_collection(&info.db, &info.coll);
                        let tbl_ndx = Self::get_table_name_for_index(&info.db, &info.coll, &info.name);
                        let s =
                        match info.options.tryGetValueForKey("unique") {
                            Some(&BsonValue::BBoolean(true)) => {
                                format!("CREATE TABLE \"{}\" (k BLOB NOT NULL, doc_rowid int NOT NULL REFERENCES \"{}\"(did) ON DELETE CASCADE, PRIMARY KEY (k))", tbl_ndx, tbl_coll)
                            },
                            _ => {
                                format!("CREATE TABLE \"{}\" (k BLOB NOT NULL, doc_rowid int NOT NULL REFERENCES \"{}\"(did) ON DELETE CASCADE, PRIMARY KEY (k,doc_rowid))", tbl_ndx, tbl_coll)
                            },
                        };
                        try!(self.exec(&s));
                        try!(self.exec(&format!("CREATE INDEX \"childndx_{}\" ON \"{}\" (doc_rowid)", tbl_ndx, tbl_ndx)));
/*
                // now insert index entries for every doc that already exists
                let (normspec,weights) = getNormalizedSpec info
                //printfn "normspec in createIndex: %A" normspec
                //printfn "weights in createIndex: %A" weights
                use stmt2 = conn.prepare(sprintf "SELECT did,bson FROM \"%s\"" collTable)
                use stmt_insert = prepareIndexInsert ndxTable
                while raw.SQLITE_ROW = stmt2.step() do
                    let doc_rowid = stmt2.column_int64(0)
                    let newDoc = stmt2.column_blob(1) |> BinReader.ReadDocument

                    let entries = ResizeArray<_>()
                    let f vals = entries.Add(vals)

                    getIndexEntries newDoc normspec weights info.options f

                    let entries = entries.ToArray() |> Set.ofArray |> Set.toArray
                    //printfn "entries for index: %A" entries
                    Array.iter (fun vals ->
                        //printfn "for index: %A" vals
                        let k = bson.encodeMultiForIndex vals
                        indexInsertStep stmt_insert k doc_rowid
                    ) entries

*/
                        Ok(true)
                    },
                    Some(_) => {
                        Err(elmo::Error::Misc("insert stmt step() returned a row"))
                    },
                }
            },
        }
    }

    fn base_create_collection(&mut self, db: &str, coll: &str, options: BsonValue) -> elmo::Result<bool> {
        match try!(self.get_collection_options(db, coll)) {
            Some(_) => Ok(false),
            None => {
                let v_options = options.to_bson_array();
                let mut stmt = try!(self.conn.prepare("INSERT INTO \"collections\" (dbName,collName,options) VALUES (?,?,?)").map_err(elmo::wrap_err));
                try!(stmt.bind_text(1, db).map_err(elmo::wrap_err));
                try!(stmt.bind_text(2, coll).map_err(elmo::wrap_err));
                try!(stmt.bind_blob(3, &v_options).map_err(elmo::wrap_err));
                let mut r = stmt.execute();
                match try!(r.step().map_err(elmo::wrap_err)) {
                    None => {
                        let tbl = Self::get_table_name_for_collection(db, coll);
                        try!(self.conn.exec(&format!("CREATE TABLE \"{}\" (did INTEGER PRIMARY KEY, bson BLOB NOT NULL)", tbl)).map_err(elmo::wrap_err));
                        match options.tryGetValueForKey("autoIndexId") {
                            Some(&BsonValue::BBoolean(false)) => (),
                            _ => {
                                let info = IndexInfo {
                                    db: String::from(db),
                                    coll: String::from(coll),
                                    name: String::from("_id_"),
                                    spec: BsonValue::BDocument(vec![(String::from("_id"), BsonValue::BInt32(1))]),
                                    options: BsonValue::BDocument(vec![(String::from("unique"), BsonValue::BBoolean(true))]),
                                };
                                let _created = self.create_index(info);
                            },
                        }
                        Ok(true)
                    },
                    Some(_) => {
                        Err(elmo::Error::Misc("insert stmt step() returned a row"))
                    },
                }
            },
        }
    }

    // TODO not sure this func is worth the trouble.
    // or do we need more like this one?
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
            let _ = self.conn.exec("ROLLBACK TRANSACTION");
            r
        }
    }

    fn find_rowid(&mut self, v: BsonValue) -> elmo::Result<Option<i64>> {
        match self.statements {
            None => Err(elmo::Error::Misc("must prepare_write()")),
            Some(ref mut statements) => {
                match statements.find_rowid {
                    None => Ok(None),
                    Some(ref mut stmt) => {
                        stmt.clear_bindings();
                        // TODO encode for index, not just bson
                        let ba = v.to_bson_array();
                        try!(stmt.bind_blob(1, &ba).map_err(elmo::wrap_err));
                        let mut r = stmt.execute();
                        match try!(r.step().map_err(elmo::wrap_err)) {
                            None => Ok(None),
                            Some(r) => {
                                let rowid = r.column_int64(0);
                                Ok(Some(rowid))
                            },
                        }
                    },
                }
            }
        }
    }

    fn get_index_from_row(r: &sqlite3::ResultRow) -> elmo::Result<IndexInfo> {
        let name = r.column_text(0).expect("NOT NULL");
        let spec = try!(BsonValue::from_bson(&r.column_blob(1).expect("NOT NULL")));
        let options = try!(BsonValue::from_bson(&r.column_blob(2).expect("NOT NULL")));
        let db = r.column_text(3).expect("NOT NULL");
        let coll = r.column_text(4).expect("NOT NULL");
        let info = IndexInfo {
            db: String::from(db),
            coll: String::from(coll),
            name: String::from(name),
            spec: spec,
            options: options,
        };
        Ok(info)
    }

    fn get_index_info(&mut self, db: &str, coll: &str, name: &str) -> elmo::Result<Option<IndexInfo>> {
        // TODO DRY this string
        let mut stmt = try!(self.conn.prepare("SELECT ndxName, spec, options, dbName, collName FROM \"indexes\" WHERE dbName=? AND collName=? AND ndxName=?").map_err(elmo::wrap_err));
        try!(stmt.bind_text(1, db).map_err(elmo::wrap_err));
        try!(stmt.bind_text(2, coll).map_err(elmo::wrap_err));
        try!(stmt.bind_text(3, name).map_err(elmo::wrap_err));
        let mut r = stmt.execute();
        match try!(r.step().map_err(elmo::wrap_err)) {
            None => Ok(None),
            Some(row) => {
                let info = try!(Self::get_index_from_row(&row));
                Ok(Some(info))
            },
        }
    }

    fn list_indexes(&mut self) -> elmo::Result<Vec<IndexInfo>> {
        let mut stmt = try!(self.conn.prepare("SELECT ndxName, spec, options, dbName, collName FROM \"indexes\"").map_err(elmo::wrap_err));
        let mut r = stmt.execute();
        let mut v = Vec::new();
        loop {
            match try!(r.step().map_err(elmo::wrap_err)) {
                None => break,
                Some(row) => {
                    let info = try!(Self::get_index_from_row(&row));
                    v.push(info);
                },
            }
        }
        Ok(v)
    }

}

struct IndexInfo {
    db: String,
    coll: String,
    name: String,
    spec: BsonValue,
    options: BsonValue,
}

impl elmo::StorageConnection for MyConn {
    fn create_collection(&mut self, db: &str, coll: &str, options: BsonValue) -> elmo::Result<bool> {
        try!(self.begin_tx());
        let r = self.base_create_collection(db, coll, options);
        self.finish_tx(r)
    }

    fn begin_tx(&mut self) -> elmo::Result<()> {
        try!(self.conn.exec("BEGIN IMMEDIATE TRANSACTION").map_err(elmo::wrap_err));
        Ok(())
    }

    fn prepare_write(&mut self, db: &str, coll: &str) -> elmo::Result<()> {
        // TODO make sure a tx is open?
        // TODO create collection
        let tbl = Self::get_table_name_for_collection(db, coll);
        let stmt_insert = try!(self.conn.prepare(&format!("INSERT INTO \"{}\" (bson) VALUES (?)", tbl)).map_err(elmo::wrap_err));
        let stmt_delete = try!(self.conn.prepare(&format!("DELETE FROM \"{}\" WHERE rowid=?", tbl)).map_err(elmo::wrap_err));
        let indexes = try!(self.list_indexes());
        let mut find_rowid = None;
        for info in &indexes {
            if info.name == "_id_" {
                let tbl = Self::get_table_name_for_index(db, coll, &info.name);
                find_rowid = Some(try!(self.conn.prepare(&format!("SELECT doc_rowid FROM \"{}\" WHERE k=?", tbl)).map_err(elmo::wrap_err)));
                break;
            }
        }
        let c = MyStatements {
            insert: stmt_insert,
            delete: stmt_delete,
            find_rowid: find_rowid,
        };
        // we assume that assigning to self.statements will replace
        // any existing value there which will cause those existing
        // statements to be finalized.
        self.statements = Some(c);
        Ok(())
    }

    fn unprepare_write(&mut self) -> elmo::Result<()> {
        self.statements = None;
        Ok(())
    }

    fn delete(&mut self, v: BsonValue) -> elmo::Result<bool> {
        match try!(self.find_rowid(v).map_err(elmo::wrap_err)) {
            None => Ok(false),
            Some(rowid) => {
                match self.statements {
                    None => Err(elmo::Error::Misc("must prepare_write()")),
                    Some(ref mut statements) => {
                        statements.delete.clear_bindings();
                        try!(statements.delete.bind_int64(1, rowid).map_err(elmo::wrap_err));
                        let mut r = statements.delete.execute();
                        match try!(r.step().map_err(elmo::wrap_err)) {
                            Some(_) => {
                                Err(elmo::Error::Misc("insert stmt step() returned a row"))
                            },
                            None => {
                                let count = self.conn.changes();
                                if count == 1 {
                                    // TODO update indexes
                                    Ok(true)
                                } else if count == 0 {
                                    Ok(false)
                                } else {
                                    Err(elmo::Error::Misc("changes() after delete is wrong"))
                                }
                            },
                        }
                    },
                }
            },
        }
    }

    fn insert(&mut self, v: BsonValue) -> elmo::Result<()> {
        match self.statements {
            None => Err(elmo::Error::Misc("must prepare_write()")),
            Some(ref mut statements) => {
                let ba = v.to_bson_array();
                statements.insert.clear_bindings();
                try!(statements.insert.bind_blob(1,&ba).map_err(elmo::wrap_err));
                let mut r = statements.insert.execute();
                match try!(r.step().map_err(elmo::wrap_err)) {
                    None => {
                        //let _rowid = self.conn.last_insert_rowid();
                        if self.conn.changes() == 1 {
                            // TODO update indexes
                            Ok(())
                        } else {
                            Err(elmo::Error::Misc("changes() after insert must be 1"))
                        }
                    },
                    Some(_) => {
                        Err(elmo::Error::Misc("insert stmt step() returned a row"))
                    },
                }
            }
        }
    }

    fn commit_tx(&mut self) -> elmo::Result<()> {
        try!(self.conn.exec("COMMIT TRANSACTION").map_err(elmo::wrap_err));
        Ok(())
    }

}

fn base_connect() -> sqlite3::SqliteResult<sqlite3::DatabaseConnection> {
    // TODO allow a different filename to be specified
    let access = sqlite3::access::ByFilename { flags: sqlite3::access::flags::OPEN_READWRITE | sqlite3::access::flags::OPEN_CREATE, filename: "elmodata.db" };
    let mut conn = try!(sqlite3::DatabaseConnection::new(access));
    try!(conn.exec("PRAGMA journal_mode=WAL"));
    try!(conn.exec("PRAGMA foreign_keys=ON"));
    try!(conn.exec("CREATE TABLE IF NOT EXISTS \"collections\" (dbName TEXT NOT NULL, collName TEXT NOT NULL, options BLOB NOT NULL, PRIMARY KEY (dbName,collName))"));
    try!(conn.exec("CREATE TABLE IF NOT EXISTS \"indexes\" (dbName TEXT NOT NULL, collName TEXT NOT NULL, ndxName TEXT NOT NULL, spec BLOB NOT NULL, options BLOB NOT NULL, PRIMARY KEY (dbName, collName, ndxName), FOREIGN KEY (dbName,collName) REFERENCES \"collections\" ON DELETE CASCADE ON UPDATE CASCADE, UNIQUE (spec,dbName,collName))"));

    Ok(conn)
}

pub fn connect() -> elmo::Result<Box<elmo::StorageConnection>> {
    let conn = try!(base_connect().map_err(elmo::wrap_err));
    let c = MyConn {
        conn: conn,
        statements: None,
    };
    Ok(box c)
}


