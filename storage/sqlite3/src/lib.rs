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

// TODO I'm not sure this type is worth the trouble anymore.
// maybe we should go back to just keeping a bool that specifies
// whether we need to negate or not.
#[derive(PartialEq,Copy,Clone)]
enum IndexType {
    Forward,
    Backward,
    Geo2d,
}

fn decode_index_type(v: &BsonValue) -> IndexType {
    match v {
        &BsonValue::BInt32(n) => if n<0 { IndexType::Backward } else { IndexType::Forward },
        &BsonValue::BInt64(n) => if n<0 { IndexType::Backward } else { IndexType::Forward },
        &BsonValue::BDouble(n) => if n<0.0 { IndexType::Backward } else { IndexType::Forward },
        &BsonValue::BString(ref s) => if s == "2d" { 
            IndexType::Geo2d 
        } else { 
            panic!("decode_index_type")
        },
        _ => panic!("decode_index_type")
    }
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

    fn prepare_index_insert(&mut self, tbl: &str) -> elmo::Result<sqlite3::PreparedStatement> {
        let stmt = try!(self.conn.prepare(&format!("INSERT INTO \"{}\" (k,doc_rowid) VALUES (?,?)",tbl)).map_err(elmo::wrap_err));
        Ok(stmt)
    }

    fn get_index_entries(new_doc: &BsonValue, normspec: &Vec<(String, IndexType)>, weights: &Option<std::collections::HashMap<String,i32>>, options: &BsonValue, entries: &mut Vec<Vec<(BsonValue,bool)>>) -> elmo::Result<()> {
        fn find_index_entry_vals(normspec: &Vec<(String, IndexType)>, new_doc: &BsonValue, sparse: bool) -> Vec<(BsonValue,bool)> {
            let mut r = Vec::new();
            for t in normspec {
                let k = &t.0;
                let typ = t.1;
                let mut v = new_doc.find_path(k);

                // now we replace any BUndefined with BNull.  this seems, well,
                // kinda wrong, as it effectively encodes the index entries to
                // contain information that is slightly incorrect, since BNull
                // means "it was present and explicitly null", whereas BUndefined
                // means "it was absent".  Still, this appears to be the exact
                // behavior of Mongo.  Note that this only affects index entries.
                // The matcher can and must still distinguish between null and
                // undefined.

                let keep =
                    if sparse {
                        match v {
                            BsonValue::BUndefined => false,
                            _ => true,
                        }
                    } else {
                        true
                    };
                if keep {
                    v.replace_undefined();
                    let neg = IndexType::Backward == typ;
                    r.push((v,neg));
                }
            }
            r
        }

        // TODO what should the name of this func actually be?
        fn q(vals: &Vec<(BsonValue, bool)>, w: i32, s: String, entries: &mut Vec<Vec<(BsonValue,bool)>>) {
            // TODO tokenize properly
            let a = s.split(" ");
            // TODO let a = a |> Set.ofArray |> Set.toArray
            for s in a {
                let s = String::from(s);
                let v = BsonValue::BArray(vec![BsonValue::BString(s), BsonValue::BInt32(w)]);
                // TODO clone is ugly
                let mut vals = vals.clone();
                vals.push((v, false));
                entries.push(vals);
            }
        }

        fn maybe_text(vals: &Vec<(BsonValue, bool)>, new_doc: &BsonValue, weights: &Option<std::collections::HashMap<String,i32>>, entries: &mut Vec<Vec<(BsonValue,bool)>>) {
            match weights {
                &Some(ref weights) => {
                    for k in weights.keys() {
                        if k == "&**" {
                            // TODO bson.forAllStrings newDoc (fun s -> q vals w s)
                        } else {
                            match new_doc.find_path(k) {
                                BsonValue::BUndefined => (),
                                v => {
                                    match v {
                                        BsonValue::BString(s) => q(&vals, weights[k], s, entries),
                                        BsonValue::BArray(a) => {
                                            // TODO let a = a |> Set.ofArray |> Set.toArray
                                            for v in a {
                                                match v {
                                                    BsonValue::BString(s) => q(&vals, weights[k], s, entries),
                                                    _ => (),
                                                }
                                            }
                                        },
                                        _ => (),
                                    }
                                },
                            }
                        }
                    }
                },
                &None => {
                    // TODO clone is ugly
                    entries.push(vals.clone());
                },
            }
        }

        fn replace_array_element<T:Clone>(vals: &Vec<T>, i: usize, v: T) -> Vec<T> {
            let mut v2 = vals.clone();
            v2[i] = v;
            v2
        }

        fn maybe_array(vals: &Vec<(BsonValue, bool)>, new_doc: &BsonValue, weights: &Option<std::collections::HashMap<String,i32>>, entries: &mut Vec<Vec<(BsonValue,bool)>>) {
            maybe_text(vals, new_doc, weights, entries);

            // now, if any of the vals in the key are an array, we need
            // to generate more index entries for this document, one
            // for each item in the array.  Mongo calls this a
            // multikey index.

            for i in 0 .. vals.len() {
                let t = &vals[i];
                let v = &t.0;
                let typ = t.1;
                match v {
                    &BsonValue::BArray(ref a) => {
                        // TODO let a = a |> Set.ofArray |> Set.toArray
                        for av in a {
                            // TODO clone is ugly
                            let replaced = replace_array_element(vals, i, (av.clone(), typ));
                            maybe_array(&replaced, new_doc, weights, entries);
                        }
                    },
                    _ => ()
                }
            }
        }

        let sparse = match options.tryGetValueForKey("sparse") {
            Some(&BsonValue::BBoolean(b)) => b,
            _ => false,
        };

        let vals = find_index_entry_vals(normspec, new_doc, sparse);
        maybe_array(&vals, new_doc, weights, entries);

        Ok(())
    }

    fn step_done(stmt: &mut sqlite3::PreparedStatement) -> elmo::Result<()> {
        let mut r = stmt.execute();
        match try!(r.step().map_err(elmo::wrap_err)) {
            Some(_) => {
                Err(elmo::Error::Misc("step_done() returned a row"))
            },
            None => {
                Ok(())
            },
        }
    }

    fn verify_changes(conn: &sqlite3::DatabaseConnection, shouldbe: u64) -> elmo::Result<()> {
        if conn.changes() == shouldbe {
            Ok(())
        } else {
            // TODO or should this be an assert?
            Err(elmo::Error::Misc("changes() is wrong"))
        }
    }

    fn index_insert_step(&self, stmt: &mut sqlite3::PreparedStatement, k: Vec<u8>, doc_rowid: i64) -> elmo::Result<()> {
        stmt.clear_bindings();
        try!(stmt.bind_blob(1, &k).map_err(elmo::wrap_err));
        try!(stmt.bind_int64(2, doc_rowid).map_err(elmo::wrap_err));
        try!(Self::step_done(stmt));
        try!(Self::verify_changes(&self.conn, 1));
        Ok(())
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
                        // now insert index entries for every doc that already exists
                        let (normspec, weights) = try!(Self::get_normalized_spec(&info));
                        let mut stmt2 = try!(self.conn.prepare(&format!("SELECT did,bson FROM \"{}\"", tbl_coll)).map_err(elmo::wrap_err));
                        let mut stmt_insert = try!(self.prepare_index_insert(&tbl_ndx));
                        let mut r = stmt2.execute();
                        loop {
                            match try!(r.step().map_err(elmo::wrap_err)) {
                                None => break,
                                Some(row) => {
                                    let doc_rowid = row.column_int64(0);
                                    let new_doc = try!(BsonValue::from_bson(&row.column_blob(1).expect("NOT NULL")));
                                    let mut entries = Vec::new();
                                    try!(Self::get_index_entries(&new_doc, &normspec, &weights, &info.options, &mut entries));
                                    // TODO let entries = entries.ToArray() |> Set.ofArray |> Set.toArray
                                    for vals in entries {
                                        let k = BsonValue::encode_multi_for_index(vals);
                                        try!(self.index_insert_step(&mut stmt_insert, k, doc_rowid));
                                    }
                                },
                            }
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
                        let ba = BsonValue::encode_one_for_index(&v, false);
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

    fn slice_find(pairs: &[(String, BsonValue)], s: &str) -> Option<usize> {
        for i in 0 .. pairs.len() {
            match pairs[0].1 {
                BsonValue::BString(ref t) => {
                    if t == s {
                        return Some(i);
                    }
                },
                _ => (),
            }
        }
        None
    }

    // this function gets the index spec (its keys) into a form that
    // is simplified and cleaned up.
    //
    // if there are text indexes in index.spec, they are removed
    //
    // all text indexes, including any that were in index.spec, and
    // anything implied by options.weights, are stored in a new Map<string,int>
    // called weights.
    //
    // any non-text indexes that appeared in spec AFTER any text
    // indexes are discarded.  I *think* Mongo keeps these, but only
    // for the purpose of grabbing their data later when used as a covering
    // index, which we're ignoring.
    //
    fn get_normalized_spec(info: &IndexInfo) -> elmo::Result<(Vec<(String,IndexType)>,Option<std::collections::HashMap<String,i32>>)> {
        //printfn "info: %A" info
        let keys = try!(info.spec.getDocument());
        let first_text = Self::slice_find(&keys, "text");
        let w1 = info.options.tryGetValueForKey("weights");
        match (first_text, w1) {
            (None, None) => {
                let decoded = keys.iter().map(|&(ref k, ref v)| (k.clone(), decode_index_type(v))).collect::<Vec<(String,IndexType)>>();
                //printfn "no text index: %A" decoded
                Ok((decoded, None))
            },
            _ => {
                let (scalar_keys, text_keys) = 
                    match first_text {
                        Some(i) => {
                            let scalar_keys = &keys[0 .. i];
                            // note that any non-text index after the first text index is getting discarded
                            let mut text_keys = Vec::new();
                            for t in keys {
                                match t.1 {
                                    BsonValue::BString(ref s) => {
                                        if s == "text" {
                                            text_keys.push(t.0.clone());
                                        }
                                    },
                                    _ => (),
                                }
                            }
                            (scalar_keys, text_keys)
                        },
                        None => (&keys[0 ..], Vec::new())
                    };
                let mut weights = std::collections::HashMap::new();
                match w1 {
                    Some(&BsonValue::BDocument(ref a)) => {
                        for t in a {
                            let n = 
                                match &t.1 {
                                    &BsonValue::BInt32(n) => n,
                                    &BsonValue::BInt64(n) => n as i32,
                                    &BsonValue::BDouble(n) => n as i32,
                                    _ => panic!("weight must be numeric")
                                };
                            weights.insert(t.0.clone(), n);
                        }
                    },
                    Some(_) => panic!( "weights must be a document"),
                    None => (),
                };
                for k in text_keys {
                    if !weights.contains_key(&k) {
                        weights.insert(String::from(k), 1);
                    }
                }
                // TODO if the wildcard is present, remove everything else?
                let decoded = scalar_keys.iter().map(|&(ref k, ref v)| (k.clone(), decode_index_type(v))).collect::<Vec<(String,IndexType)>>();
                let r = Ok((decoded, Some(weights)));
                //printfn "%A" r
                r
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
                        try!(Self::step_done(&mut statements.delete));
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
    }

    fn insert(&mut self, v: BsonValue) -> elmo::Result<()> {
        match self.statements {
            None => Err(elmo::Error::Misc("must prepare_write()")),
            Some(ref mut statements) => {
                let ba = v.to_bson_array();
                statements.insert.clear_bindings();
                try!(statements.insert.bind_blob(1,&ba).map_err(elmo::wrap_err));
                try!(Self::step_done(&mut statements.insert));
                try!(Self::verify_changes(&self.conn, 1));
                // TODO update indexes
                Ok(())
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

/*

look at the non-allocating alternatives to column_text() and column_blob()

*/

