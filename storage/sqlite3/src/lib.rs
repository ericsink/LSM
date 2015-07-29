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
#![feature(vec_push_all)]

extern crate bson;
use bson::BsonValue;

extern crate elmo;

pub type Result<T> = elmo::Result<T>;

extern crate sqlite3;

struct IndexPrep {
    info: elmo::IndexInfo,
    stmt_insert: sqlite3::PreparedStatement,
    stmt_delete: sqlite3::PreparedStatement,
}

struct MyCollectionWriter<'a> {
    insert: sqlite3::PreparedStatement,
    delete: sqlite3::PreparedStatement,
    update: sqlite3::PreparedStatement,
    stmt_find_rowid: Option<sqlite3::PreparedStatement>,
    indexes: Vec<IndexPrep>,
    mywriter: &'a MyWriter<'a>,
}

struct MyNormalCollectionReader {
    stmt: sqlite3::PreparedStatement,
    // TODO need counts here

    // this struct does not have a reference to a parent.
    // such as a MyReader.
    // this is because its parent might be a MyReader, or it
    // might be a MyWriter.
    // and also because such a link doesn't seem necessary.
}

struct MyTextCollectionReader {
    stmt: sqlite3::PreparedStatement,
    // TODO need counts here

    // this struct does not have a reference to a parent.
    // such as a MyReader.
    // this is because its parent might be a MyReader, or it
    // might be a MyWriter.
    // and also because such a link doesn't seem necessary.
}

struct MyEmptyCollectionReader;

struct MyReader<'a> {
    myconn: &'a MyConn,
}

struct MyWriter<'a> {
    myconn: &'a MyConn,
}

struct MyConn {
    conn: sqlite3::DatabaseConnection,
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

fn step_done(stmt: &mut sqlite3::PreparedStatement) -> Result<()> {
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

fn verify_changes(stmt: &sqlite3::PreparedStatement, shouldbe: u64) -> Result<()> {
    if stmt.changes() == shouldbe {
        Ok(())
    } else {
        // TODO or should this be an assert?
        Err(elmo::Error::Misc("changes() is wrong"))
    }
}

fn get_dirs(normspec: &Vec<(String, IndexType)>, vals: Vec<BsonValue>) -> Vec<(BsonValue, bool)> {
    // TODO if normspec.len() < vals.len() then panic?
    let mut a = Vec::new();
    for (i,v) in vals.into_iter().enumerate() {
        let neg = normspec[i].1 == IndexType::Backward;
        a.push((v, neg));
    }
    a
}

fn get_table_name_for_collection(db: &str, coll: &str) -> String { 
    // TODO cleanse?
    format!("docs.{}.{}", db, coll) 
}

fn get_table_name_for_index(db: &str, coll: &str, name: &str) -> String { 
    // TODO cleanse?
    format!("ndx.{}.{}.{}", db, coll, name) 
}

fn get_index_entries(new_doc: &BsonValue, normspec: &Vec<(String, IndexType)>, weights: &Option<std::collections::HashMap<String,i32>>, options: &BsonValue, entries: &mut Vec<Vec<(BsonValue,bool)>>) -> Result<()> {
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
        let a = a.into_iter().collect::<std::collections::HashSet<_>>();
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
                                        let a = a.into_iter().collect::<std::collections::HashSet<_>>();
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
        // first do the index entries for the document without considering arrays
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
                    let a = a.into_iter().collect::<std::collections::HashSet<_>>();
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
fn get_normalized_spec(info: &elmo::IndexInfo) -> Result<(Vec<(String,IndexType)>,Option<std::collections::HashMap<String,i32>>)> {
    //printfn "info: %A" info
    let keys = try!(info.spec.getDocument());
    let first_text = slice_find(&keys, "text");
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

fn get_index_info_from_row(r: &sqlite3::ResultRow) -> Result<elmo::IndexInfo> {
    let name = r.column_text(0).expect("NOT NULL");
    let spec = try!(BsonValue::from_bson(&r.column_slice(1).expect("NOT NULL")));
    let options = try!(BsonValue::from_bson(&r.column_slice(2).expect("NOT NULL")));
    let db = r.column_text(3).expect("NOT NULL");
    let coll = r.column_text(4).expect("NOT NULL");
    let info = elmo::IndexInfo {
        db: String::from(db),
        coll: String::from(coll),
        name: String::from(name),
        spec: spec,
        options: options,
    };
    Ok(info)
}

fn index_insert_step(stmt: &mut sqlite3::PreparedStatement, k: Vec<u8>, doc_rowid: i64) -> Result<()> {
    stmt.clear_bindings();
    try!(stmt.bind_blob(1, &k).map_err(elmo::wrap_err));
    try!(stmt.bind_int64(2, doc_rowid).map_err(elmo::wrap_err));
    try!(step_done(stmt));
    try!(verify_changes(stmt, 1));
    Ok(())
}

impl MyConn {
    fn get_collection_options(&self, db: &str, coll: &str) -> Result<Option<BsonValue>> {
        let mut stmt = try!(self.conn.prepare("SELECT options FROM \"collections\" WHERE dbName=? AND collName=?").map_err(elmo::wrap_err));
        try!(stmt.bind_text(1, db).map_err(elmo::wrap_err));
        try!(stmt.bind_text(2, coll).map_err(elmo::wrap_err));
        // TODO step_row() ?
        let mut r = stmt.execute();
        match try!(r.step().map_err(elmo::wrap_err)) {
            None => Ok(None),
            Some(r) => {
                let v = try!(BsonValue::from_bson(&r.column_slice(0).expect("NOT NULL")));
                Ok(Some(v))
            },
        }
    }

    fn get_stmt_for_index_scan(&self, plan: elmo::QueryPlan) -> Result<sqlite3::PreparedStatement> {
        let tbl_coll = get_table_name_for_collection(&plan.ndx.db, &plan.ndx.coll);
        let tbl_ndx = get_table_name_for_index(&plan.ndx.db, &plan.ndx.coll, &plan.ndx.name);

        // TODO the following is way too heavy.  all we need is the index types
        // so we can tell if they're supposed to be backwards or not.
        let (normspec, _weights) = try!(get_normalized_spec(&plan.ndx));

        // note that one of the reasons we need to do DISTINCT here is because a
        // single index in a single document can produce multiple index entries,
        // because, for example, when a value is an array, we don't just index
        // the array as a value, but we also index each of its elements.
        //
        // TODO it would be nice if the DISTINCT here was happening on the rowids, not on the blobs

        fn add_one(ba: &Vec<u8>) -> Vec<u8> {
            let a = ba.clone();
            // TODO add one
            a
        }

        let f_twok = |kmin: Vec<u8>, kmax: Vec<u8>, op1: &str, op2: &str| -> Result<sqlite3::PreparedStatement> {
            let sql = format!("SELECT DISTINCT d.bson FROM \"{}\" d INNER JOIN \"{}\" i ON (d.did = i.doc_rowid) WHERE k {} ? AND k {} ?", tbl_coll, tbl_ndx, op1, op2);
            let mut stmt = try!(self.conn.prepare(&sql).map_err(elmo::wrap_err));
            try!(stmt.bind_blob(1, &kmin).map_err(elmo::wrap_err));
            try!(stmt.bind_blob(2, &kmax).map_err(elmo::wrap_err));
            Ok(stmt)
        };

        let f_two = |minvals: elmo::QueryKey, maxvals: elmo::QueryKey, op1: &str, op2: &str| -> Result<sqlite3::PreparedStatement> {
            let kmin = BsonValue::encode_multi_for_index(get_dirs(&normspec, minvals));
            let kmax = BsonValue::encode_multi_for_index(get_dirs(&normspec, maxvals));
            f_twok(kmin, kmax, op1, op2)
        };

        let f_one = |vals: elmo::QueryKey, op: &str| -> Result<sqlite3::PreparedStatement> {
            let k = BsonValue::encode_multi_for_index(get_dirs(&normspec, vals));
            let sql = format!("SELECT DISTINCT d.bson FROM \"{}\" d INNER JOIN \"{}\" i ON (d.did = i.doc_rowid) WHERE k {} ?", tbl_coll, tbl_ndx, op);
            let mut stmt = try!(self.conn.prepare(&sql).map_err(elmo::wrap_err));
            try!(stmt.bind_blob(1, &k).map_err(elmo::wrap_err));
            Ok(stmt)
        };

        match plan.bounds {
            elmo::QueryBounds::Text(_,_) => unreachable!(),
            elmo::QueryBounds::GT(vals) => f_one(vals, ">"),
            elmo::QueryBounds::LT(vals) => f_one(vals, "<"),
            elmo::QueryBounds::GTE(vals) => f_one(vals, ">="),
            elmo::QueryBounds::LTE(vals) => f_one(vals, "<="),
            elmo::QueryBounds::GT_LT(minvals, maxvals) => f_two(minvals, maxvals, ">", "<"),
            elmo::QueryBounds::GTE_LT(minvals, maxvals) => f_two(minvals, maxvals, ">=", "<"),
            elmo::QueryBounds::GT_LTE(minvals, maxvals) => f_two(minvals, maxvals, ">", "<="),
            elmo::QueryBounds::GTE_LTE(minvals, maxvals) => f_two(minvals, maxvals, ">=", "<="),
            elmo::QueryBounds::EQ(vals) => {
                let kmin = BsonValue::encode_multi_for_index(get_dirs(&normspec, vals));
                let kmax = add_one(&kmin);
                f_twok(kmin, kmax, ">=", "<")
            },
        }
    }

    fn get_table_scan_reader(&self, db: &str, coll: &str) -> Result<MyNormalCollectionReader> {
        let tbl = get_table_name_for_collection(db, coll);
        let stmt = try!(self.conn.prepare(&format!("SELECT bson FROM \"{}\"", tbl)).map_err(elmo::wrap_err));
        // TODO keep track of total keys examined, etc.
        let rdr = MyNormalCollectionReader {
            stmt: stmt,
        };
        Ok(rdr)
    }

    fn get_nontext_index_scan_reader(&self, plan: elmo::QueryPlan) -> Result<MyNormalCollectionReader> {
        let stmt = try!(self.get_stmt_for_index_scan(plan));

        // TODO keep track of total keys examined, etc.
        let rdr = MyNormalCollectionReader {
            stmt: stmt,
        };
        Ok(rdr)
    }

    fn get_text_index_scan_reader(&self, ndx: &elmo::IndexInfo,  eq: elmo::QueryKey, terms: Vec<elmo::TextQueryTerm>) -> Result<MyTextCollectionReader> {
        let tbl_coll = get_table_name_for_collection(&ndx.db, &ndx.coll);
        let tbl_ndx = get_table_name_for_index(&ndx.db, &ndx.coll, &ndx.name);
        let (normspec, weights) = try!(get_normalized_spec(&ndx));
        let weights = 
            match weights {
                None => return Err(elmo::Error::Misc("non text index")),
                Some(w) => w,
            };

        fn lookup(stmt: &mut sqlite3::PreparedStatement, vals: &Vec<(BsonValue, bool)>, word: &str) -> Result<Vec<(i64,i32)>> {
            // TODO if we just search for the word without the weight, we could
            // use the add_one trick from EQ.  Probably need key encoding of an array
            // to omit the array length.  See comment there.
            let vmin = BsonValue::BArray(vec![BsonValue::BString(String::from(word)), BsonValue::BInt32(0)]);
            let vmax = BsonValue::BArray(vec![BsonValue::BString(String::from(word)), BsonValue::BInt32(100000)]);

            let mut minvals = vals.clone();
            minvals.push((vmin,false));

            let mut maxvals = vals.clone();
            maxvals.push((vmax,false));

            let kmin = BsonValue::encode_multi_for_index(minvals);
            let kmax = BsonValue::encode_multi_for_index(maxvals);
            stmt.clear_bindings();
            try!(stmt.bind_blob(1, &kmin).map_err(elmo::wrap_err));
            try!(stmt.bind_blob(2, &kmax).map_err(elmo::wrap_err));
            let mut r = stmt.execute();
            let mut entries = Vec::new();
            loop {
                match try!(r.step().map_err(elmo::wrap_err)) {
                    None => break,
                    Some(row) => {
                        let k = row.column_slice(0).expect("NOT NULL");
                        let w = try!(BsonValue::get_weight_from_index_entry(k));
                        let did = row.column_int64(1);
                        entries.push((did,w));
                    },
                }
            }
            Ok(entries)
        };

        let vals = get_dirs(&normspec, eq);

        let sql = format!("SELECT k, doc_rowid FROM \"{}\" i WHERE k > ? AND k < ?", tbl_ndx);
        let mut stmt = try!(self.conn.prepare(&sql).map_err(elmo::wrap_err));

        let mut found = Vec::new();
        for term in &terms {
            let entries = 
                match term {
                    &elmo::TextQueryTerm::Word(neg, ref s) => {
                        let entries = try!(lookup(&mut stmt, &vals, &s));
                        entries
                    },
                    &elmo::TextQueryTerm::Phrase(neg, ref s) => {
                        // TODO tokenize properly
                        let words = s.split(" ");
                        let mut entries = Vec::new();
                        for w in words {
                            entries.push_all(&try!(lookup(&mut stmt, &vals, w)));
                        }
                        entries
                    },
                };
            let v = (term, entries);
            found.push(v);
        };

        fn contains_phrase(weights: &std::collections::HashMap<String, i32>, doc: &BsonValue, p: &str) -> bool {
            for k in weights.keys() {
                let found = 
                    match doc.find_path(k) {
                        BsonValue::BUndefined => false,
                        v => match v {
                            BsonValue::BString(s) => s.find(p).is_some(),
                            _ => false,
                        },
                    };
                if found {
                    return true;
                }
            }
            return false;
        }

        fn check_phrase(terms: &Vec<elmo::TextQueryTerm>, weights: &std::collections::HashMap<String, i32>, doc: &BsonValue) -> bool {
            for term in terms {
                let b = 
                    match term {
                        &elmo::TextQueryTerm::Word(neg, ref s) => true,
                        &elmo::TextQueryTerm::Phrase(neg, ref s) => {
                            let has = contains_phrase(weights, doc, s);
                            if neg {
                                !has
                            } else {
                                has
                            }
                        },
                    };
                if !b {
                    return false;
                }
            }
            return true;
        }

        let mut pos_entries = Vec::new();
        let mut neg_entries = Vec::new();
        for e in found {
            let (term, entries) = e;
            match term {
                &elmo::TextQueryTerm::Word(neg, ref s) => {
                    if neg {
                        neg_entries.push_all(&entries);
                    } else {
                        pos_entries.push_all(&entries);
                    }
                },
                &elmo::TextQueryTerm::Phrase(neg, ref s) => {
                    if neg {
                        // TODO probably should not negate a doc just because it contains one of the words in a negated phrase
                        // neg_entries.push_all(&entries);
                    } else {
                        pos_entries.push_all(&entries);
                    }
                },
            };
        }

        let neg_docids = neg_entries.into_iter().map(|t| t.0).collect::<std::collections::HashSet<_>>();
        let mut remaining = Vec::new();
        for t in pos_entries {
            let (did, w) = t;
            if !neg_docids.contains(&did) {
                remaining.push((did, w));
            }
        }

        let mut doc_weights: std::collections::HashMap<i64, Vec<i32>> = std::collections::HashMap::new();
        for t in remaining {
            let (did, w) = t;
            if doc_weights.contains_key(&did) {
                let v = doc_weights.get_mut(&did).expect("just checked this");
                v.push(w);
            } else {
                doc_weights.insert(did, vec![w]);
            }
        }

        unimplemented!();

/*

            let sql = sprintf "SELECT bson FROM \"%s\" WHERE did=?" tblColl
            let stmt = conn.prepare(sql)
            let count = ref 0
            let s = 
                seq {
                    for (did,w) in Map.toSeq doc_weights do
                        stmt.clear_bindings()
                        stmt.bind_int64(1, did)
                        stmt.step_row()
                        let doc = stmt.column_blob(0) |> BinReader.ReadDocument
                        count := !count + 1
                        printfn "got_SQLITE_ROW"
                        stmt.reset()
                        let keep = check_phrase doc
                        if keep then
                            //printfn "weights for this doc: %A" w
                            let score = List.sum w |> float // TODO this is not the way mongo does this calculation
                            yield {doc=doc;score=Some score;pos=None}
                }
            let killFunc() =
                if tx then conn.exec("COMMIT TRANSACTION")
                stmt.sqlite3_finalize()

            {
                docs=s
                totalKeysExamined=fun () -> 0
                totalDocsExamined=fun () -> !count
                funk=killFunc
            }


*/

    }

    fn get_collection_reader<'b>(&'b self, db: &str, coll: &str, plan: Option<elmo::QueryPlan>) -> Result<Box<elmo::StorageCollectionReader<Item=elmo::Result<BsonValue>> + 'b>> {
        match try!(self.get_collection_options(db, coll)) {
            None => {
                let rdr = MyEmptyCollectionReader;
                Ok(box rdr)
            },
            Some(_) => {
                match plan {
                    Some(plan) => {
                        match plan.bounds {
                            elmo::QueryBounds::Text(eq,terms) => {
                                let rdr = try!(self.get_text_index_scan_reader(&plan.ndx, eq, terms));
                                return Ok(box rdr);
                            },
                            _ => {
                                let rdr = try!(self.get_nontext_index_scan_reader(plan));
                                return Ok(box rdr);
                            },
                        }
                    },
                    None => {
                        let rdr = try!(self.get_table_scan_reader(db, coll));
                        return Ok(box rdr);
                    },
                };
            },
        }
    }

    fn get_index_info(&self, db: &str, coll: &str, name: &str) -> Result<Option<elmo::IndexInfo>> {
        // TODO DRY this string
        let mut stmt = try!(self.conn.prepare("SELECT ndxName, spec, options, dbName, collName FROM \"indexes\" WHERE dbName=? AND collName=? AND ndxName=?").map_err(elmo::wrap_err));
        try!(stmt.bind_text(1, db).map_err(elmo::wrap_err));
        try!(stmt.bind_text(2, coll).map_err(elmo::wrap_err));
        try!(stmt.bind_text(3, name).map_err(elmo::wrap_err));
        let mut r = stmt.execute();
        match try!(r.step().map_err(elmo::wrap_err)) {
            None => Ok(None),
            Some(row) => {
                let info = try!(get_index_info_from_row(&row));
                Ok(Some(info))
            },
        }
    }

    fn base_list_indexes(&self) -> Result<Vec<elmo::IndexInfo>> {
        let mut stmt = try!(self.conn.prepare("SELECT ndxName, spec, options, dbName, collName FROM \"indexes\"").map_err(elmo::wrap_err));
        let mut r = stmt.execute();
        let mut v = Vec::new();
        loop {
            match try!(r.step().map_err(elmo::wrap_err)) {
                None => break,
                Some(row) => {
                    let info = try!(get_index_info_from_row(&row));
                    v.push(info);
                },
            }
        }
        Ok(v)
    }

    fn base_list_collections(&self) -> Result<Vec<(String, String, BsonValue)>> {
        let mut stmt = try!(self.conn.prepare("SELECT dbName, collName, options FROM \"collections\" ORDER BY collName ASC").map_err(elmo::wrap_err));
        let mut r = stmt.execute();
        let mut v = Vec::new();
        loop {
            match try!(r.step().map_err(elmo::wrap_err)) {
                None => break,
                Some(row) => {
                    let db = row.column_text(0).expect("NOT NULL");
                    let coll = row.column_text(1).expect("NOT NULL");
                    let options = try!(BsonValue::from_bson(&row.column_slice(2).expect("NOT NULL")));
                    let t = (db, coll, options);
                    v.push(t);
                },
            }
        }
        Ok(v)
    }

}

impl<'a> MyCollectionWriter<'a> {
    fn find_rowid(&mut self, v: &BsonValue) -> Result<Option<i64>> {
                match self.stmt_find_rowid {
                    None => Ok(None),
                    Some(ref mut stmt) => {
                        stmt.clear_bindings();
                        let ba = BsonValue::encode_one_for_index(v, false);
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

    fn update_indexes_delete(indexes: &mut Vec<IndexPrep>, rowid: i64) -> Result<()> {
        for t in indexes {
            t.stmt_delete.clear_bindings();
            try!(t.stmt_delete.bind_int64(1, rowid).map_err(elmo::wrap_err));
            try!(step_done(&mut t.stmt_delete));
        }
        Ok(())
    }

    fn update_indexes_insert(indexes: &mut Vec<IndexPrep>, rowid: i64, v: &BsonValue) -> Result<()> {
        for t in indexes {
            let (normspec, weights) = try!(get_normalized_spec(&t.info));
            let mut entries = Vec::new();
            try!(get_index_entries(&v, &normspec, &weights, &t.info.options, &mut entries));
            let entries = entries.into_iter().collect::<std::collections::HashSet<_>>();
            for vals in entries {
                let k = BsonValue::encode_multi_for_index(vals);
                try!(index_insert_step(&mut t.stmt_insert, k, rowid));
            }
        }
        Ok(())
    }

}

impl<'a> elmo::StorageCollectionWriter for MyCollectionWriter<'a> {
    fn update(&mut self, v: BsonValue) -> Result<()> {
        match v.tryGetValueForKey("_id") {
            None => Err(elmo::Error::Misc("cannot update without _id")),
            Some(id) => {
                match try!(self.find_rowid(&id).map_err(elmo::wrap_err)) {
                    None => Err(elmo::Error::Misc("update but does not exist")),
                    Some(rowid) => {
                                let ba = v.to_bson_array();
                                self.update.clear_bindings();
                                try!(self.update.bind_blob(1,&ba).map_err(elmo::wrap_err));
                                try!(self.update.bind_int64(2, rowid).map_err(elmo::wrap_err));
                                try!(step_done(&mut self.update));
                                try!(verify_changes(&self.update, 1));
                                try!(Self::update_indexes_delete(&mut self.indexes, rowid));
                                try!(Self::update_indexes_insert(&mut self.indexes, rowid, &v));
                                Ok(())
                            },
                }
            },
        }
    }

    fn delete(&mut self, v: BsonValue) -> Result<bool> {
        // TODO is v supposed to be the id?
        match try!(self.find_rowid(&v).map_err(elmo::wrap_err)) {
            None => Ok(false),
            Some(rowid) => {
                        self.delete.clear_bindings();
                        try!(self.delete.bind_int64(1, rowid).map_err(elmo::wrap_err));
                        try!(step_done(&mut self.delete));
                        let count = self.mywriter.myconn.conn.changes();
                        if count == 1 {
                            // TODO might not need index update here.  foreign key cascade?
                            try!(Self::update_indexes_delete(&mut self.indexes, rowid));
                            Ok(true)
                        } else if count == 0 {
                            Ok(false)
                        } else {
                            Err(elmo::Error::Misc("changes() after delete is wrong"))
                        }
                    },
                }
    }

    fn insert(&mut self, v: BsonValue) -> Result<()> {
                let ba = v.to_bson_array();
                self.insert.clear_bindings();
                try!(self.insert.bind_blob(1,&ba).map_err(elmo::wrap_err));
                try!(step_done(&mut self.insert));
                try!(verify_changes(&self.insert, 1));
                let rowid = self.mywriter.myconn.conn.last_insert_rowid();
                try!(Self::update_indexes_delete(&mut self.indexes, rowid));
                try!(Self::update_indexes_insert(&mut self.indexes, rowid, &v));
                Ok(())
    }

}

impl<'a> MyWriter<'a> {
    fn prepare_index_insert(&self, tbl: &str) -> Result<sqlite3::PreparedStatement> {
        let stmt = try!(self.myconn.conn.prepare(&format!("INSERT INTO \"{}\" (k,doc_rowid) VALUES (?,?)",tbl)).map_err(elmo::wrap_err));
        Ok(stmt)
    }

    fn create_index(&self, info: elmo::IndexInfo) -> Result<bool> {
        let _created = try!(self.base_create_collection(&info.db, &info.coll, BsonValue::BArray(Vec::new())));
        match try!(self.myconn.get_index_info(&info.db, &info.coll, &info.name)) {
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
                let mut stmt = try!(self.myconn.conn.prepare("INSERT INTO \"indexes\" (dbName,collName,ndxName,spec,options) VALUES (?,?,?,?,?)").map_err(elmo::wrap_err));
                try!(stmt.bind_text(1, &info.db).map_err(elmo::wrap_err));
                try!(stmt.bind_text(2, &info.coll).map_err(elmo::wrap_err));
                try!(stmt.bind_text(3, &info.name).map_err(elmo::wrap_err));
                try!(stmt.bind_blob(4, &ba_spec).map_err(elmo::wrap_err));
                try!(stmt.bind_blob(5, &ba_options).map_err(elmo::wrap_err));
                let mut r = stmt.execute();
                match try!(r.step().map_err(elmo::wrap_err)) {
                    None => {
                        let tbl_coll = get_table_name_for_collection(&info.db, &info.coll);
                        let tbl_ndx = get_table_name_for_index(&info.db, &info.coll, &info.name);
                        let s =
                        match info.options.tryGetValueForKey("unique") {
                            Some(&BsonValue::BBoolean(true)) => {
                                format!("CREATE TABLE \"{}\" (k BLOB NOT NULL, doc_rowid int NOT NULL REFERENCES \"{}\"(did) ON DELETE CASCADE, PRIMARY KEY (k))", tbl_ndx, tbl_coll)
                            },
                            _ => {
                                format!("CREATE TABLE \"{}\" (k BLOB NOT NULL, doc_rowid int NOT NULL REFERENCES \"{}\"(did) ON DELETE CASCADE, PRIMARY KEY (k,doc_rowid))", tbl_ndx, tbl_coll)
                            },
                        };
                        try!(self.myconn.conn.exec(&s).map_err(elmo::wrap_err));
                        try!(self.myconn.conn.exec(&format!("CREATE INDEX \"childndx_{}\" ON \"{}\" (doc_rowid)", tbl_ndx, tbl_ndx)).map_err(elmo::wrap_err));
                        // now insert index entries for every doc that already exists
                        let (normspec, weights) = try!(get_normalized_spec(&info));
                        let mut stmt2 = try!(self.myconn.conn.prepare(&format!("SELECT did,bson FROM \"{}\"", tbl_coll)).map_err(elmo::wrap_err));
                        let mut stmt_insert = try!(self.prepare_index_insert(&tbl_ndx));
                        let mut r = stmt2.execute();
                        loop {
                            match try!(r.step().map_err(elmo::wrap_err)) {
                                None => break,
                                Some(row) => {
                                    let doc_rowid = row.column_int64(0);
                                    let new_doc = try!(BsonValue::from_bson(&row.column_slice(1).expect("NOT NULL")));
                                    let mut entries = Vec::new();
                                    try!(get_index_entries(&new_doc, &normspec, &weights, &info.options, &mut entries));
                                    let entries = entries.into_iter().collect::<std::collections::HashSet<_>>();
                                    for vals in entries {
                                        let k = BsonValue::encode_multi_for_index(vals);
                                        try!(index_insert_step(&mut stmt_insert, k, doc_rowid));
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

    fn base_clear_collection(&self, db: &str, coll: &str) -> Result<bool> {
        match try!(self.myconn.get_collection_options(db, coll)) {
            None => {
                let created = try!(self.base_create_collection(db, coll, BsonValue::BArray(Vec::new())));
                Ok(created)
            },
            Some(_) => {
                let tbl = get_table_name_for_collection(db, coll);
                try!(self.myconn.conn.exec(&format!("DROP TABLE \"{}\"", tbl)).map_err(elmo::wrap_err));
                Ok(false)
            },
        }
    }

    fn base_rename_collection(&self, old_name: &str, new_name: &str, drop_target: bool) -> Result<bool> {
        let (old_db, old_coll) = bson::split_name(old_name);
        let (new_db, new_coll) = bson::split_name(new_name);

        // jstests/core/rename8.js seems to think that renaming to/from a system collection is illegal unless
        // that collection is system.users, which is "whitelisted".  for now, we emulate this behavior, even
        // though system.users isn't supported.
        if old_coll != "system.users" && old_coll.starts_with("system.") {
            return Err(elmo::Error::Misc("renameCollection with a system collection not allowed."))
        }
        if new_coll != "system.users" && new_coll.starts_with("system.") {
            return Err(elmo::Error::Misc("renameCollection with a system collection not allowed."))
        }

        if drop_target {
            let _deleted = try!(self.base_drop_collection(new_db, new_coll));
        }

        match try!(self.myconn.get_collection_options(old_db, old_coll)) {
            None => {
                let created = try!(self.base_create_collection(new_db, new_coll, BsonValue::BArray(Vec::new())));
                Ok(created)
            },
            Some(_) => {
                let old_tbl = get_table_name_for_collection(old_db, old_coll);
                let new_tbl = get_table_name_for_collection(new_db, new_coll);

                let mut stmt = try!(self.myconn.conn.prepare("UPDATE \"collections\" SET dbName=?, collName=? WHERE dbName=? AND collName=?").map_err(elmo::wrap_err));
                try!(stmt.bind_text(1, new_db).map_err(elmo::wrap_err));
                try!(stmt.bind_text(2, new_coll).map_err(elmo::wrap_err));
                try!(stmt.bind_text(3, old_db).map_err(elmo::wrap_err));
                try!(stmt.bind_text(4, old_coll).map_err(elmo::wrap_err));
                try!(step_done(&mut stmt));

                try!(self.myconn.conn.exec(&format!("ALTER TABLE \"{}\" RENAME TO \"{}\"", old_tbl, new_tbl)).map_err(elmo::wrap_err));

                let indexes = try!(self.myconn.base_list_indexes());
                for info in indexes {
                    if info.db == old_db && info.coll == old_coll {
                        let old_ndx_tbl = get_table_name_for_index(old_db, old_coll, &info.name);
                        let new_ndx_tbl = get_table_name_for_index(new_db, new_coll, &info.name);
                        try!(self.myconn.conn.exec(&format!("ALTER TABLE \"{}\" RENAME TO \"{}\"", old_ndx_tbl, new_ndx_tbl)).map_err(elmo::wrap_err));
                    }
                }
                Ok(false)
            },
        }
    }

    fn base_create_collection(&self, db: &str, coll: &str, options: BsonValue) -> Result<bool> {
        match try!(self.myconn.get_collection_options(db, coll)) {
            Some(_) => Ok(false),
            None => {
                let v_options = options.to_bson_array();
                let mut stmt = try!(self.myconn.conn.prepare("INSERT INTO \"collections\" (dbName,collName,options) VALUES (?,?,?)").map_err(elmo::wrap_err));
                try!(stmt.bind_text(1, db).map_err(elmo::wrap_err));
                try!(stmt.bind_text(2, coll).map_err(elmo::wrap_err));
                try!(stmt.bind_blob(3, &v_options).map_err(elmo::wrap_err));
                let mut r = stmt.execute();
                match try!(r.step().map_err(elmo::wrap_err)) {
                    None => {
                        let tbl = get_table_name_for_collection(db, coll);
                        try!(self.myconn.conn.exec(&format!("CREATE TABLE \"{}\" (did INTEGER PRIMARY KEY, bson BLOB NOT NULL)", tbl)).map_err(elmo::wrap_err));
                        // now create mongo index for _id
                        match options.tryGetValueForKey("autoIndexId") {
                            Some(&BsonValue::BBoolean(false)) => (),
                            _ => {
                                let info = elmo::IndexInfo {
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

    fn base_create_indexes(&self, what: Vec<elmo::IndexInfo>) -> Result<Vec<bool>> {
        let mut v = Vec::new();
        for info in what {
            let b = try!(self.create_index(info));
            v.push(b);
        }
        Ok(v)
    }

    fn base_drop_index(&self, db: &str, coll: &str, name: &str) -> Result<bool> {
        match try!(self.myconn.get_index_info(db, coll, name)) {
            None => Ok(false),
            Some(_) => {
                let mut stmt = try!(self.myconn.conn.prepare("DELETE FROM \"indexes\" WHERE dbName=? AND collName=? AND ndxName=?").map_err(elmo::wrap_err));
                try!(stmt.bind_text(1, db).map_err(elmo::wrap_err));
                try!(stmt.bind_text(2, coll).map_err(elmo::wrap_err));
                try!(stmt.bind_text(3, name).map_err(elmo::wrap_err));
                try!(step_done(&mut stmt));
                try!(verify_changes(&stmt, 1));
                let tbl = get_table_name_for_index(db, coll, name);
                try!(self.myconn.conn.exec(&format!("DROP TABLE \"{}\"", tbl)).map_err(elmo::wrap_err));
                Ok(true)
            },
        }
    }

    fn base_drop_database(&self, db: &str) -> Result<bool> {
        let collections = try!(self.myconn.base_list_collections());
        let mut b = false;
        for t in collections {
            if t.0 == db {
                let _deleted = try!(self.base_drop_collection(&t.0, &t.1));
                assert!(_deleted);
                b = true;
            }
        }
        Ok(b)
    }

    fn base_drop_collection(&self, db: &str, coll: &str) -> Result<bool> {
        match try!(self.myconn.get_collection_options(db, coll)) {
            None => Ok(false),
            Some(_) => {
                let indexes = try!(self.myconn.base_list_indexes());
                for info in indexes {
                    if info.db == db && info.coll == coll {
                        try!(self.base_drop_index(&info.db, &info.coll, &info.name));
                    }
                }
                let mut stmt = try!(self.myconn.conn.prepare("DELETE FROM \"collections\" WHERE dbName=? AND collName=?").map_err(elmo::wrap_err));
                try!(stmt.bind_text(1, db).map_err(elmo::wrap_err));
                try!(stmt.bind_text(2, coll).map_err(elmo::wrap_err));
                try!(step_done(&mut stmt));
                try!(verify_changes(&stmt, 1));
                let tbl = get_table_name_for_collection(db, coll);
                try!(self.myconn.conn.exec(&format!("DROP TABLE \"{}\"", tbl)).map_err(elmo::wrap_err));
                Ok(true)
            },
        }
    }

}

impl<'a> elmo::StorageWriter for MyWriter<'a> {
    fn get_collection_writer<'b>(&'b self, db: &str, coll: &str) -> Result<Box<elmo::StorageCollectionWriter + 'b>> {
        let _created = try!(self.base_create_collection(db, coll, BsonValue::BArray(Vec::new())));
        let tbl = get_table_name_for_collection(db, coll);
        let stmt_insert = try!(self.myconn.conn.prepare(&format!("INSERT INTO \"{}\" (bson) VALUES (?)", tbl)).map_err(elmo::wrap_err));
        let stmt_delete = try!(self.myconn.conn.prepare(&format!("DELETE FROM \"{}\" WHERE rowid=?", tbl)).map_err(elmo::wrap_err));
        let stmt_update = try!(self.myconn.conn.prepare(&format!("UPDATE \"{}\" SET bson=? WHERE rowid=?", tbl)).map_err(elmo::wrap_err));
        let indexes = try!(self.myconn.base_list_indexes());
        let mut find_rowid = None;
        for info in &indexes {
            if info.name == "_id_" {
                let tbl = get_table_name_for_index(db, coll, &info.name);
                find_rowid = Some(try!(self.myconn.conn.prepare(&format!("SELECT doc_rowid FROM \"{}\" WHERE k=?", tbl)).map_err(elmo::wrap_err)));
                break;
            }
        }
        let mut index_stmts = Vec::new();
        for info in indexes {
            let tbl_ndx = get_table_name_for_index(db, coll, &info.name);
            let stmt_insert = try!(self.prepare_index_insert(&tbl_ndx));
            let stmt_delete = try!(self.myconn.conn.prepare(&format!("DELETE FROM \"{}\" WHERE doc_rowid=?", tbl_ndx)).map_err(elmo::wrap_err));
            let t = IndexPrep {
                info: info, 
                stmt_insert: stmt_insert, 
                stmt_delete: stmt_delete
            };
            index_stmts.push(t);
        }
        let c = MyCollectionWriter {
            insert: stmt_insert,
            delete: stmt_delete,
            update: stmt_update,
            stmt_find_rowid: find_rowid,
            indexes: index_stmts,
            mywriter: self,
        };
        Ok(box c)
    }

    fn commit(self: Box<Self>) -> Result<()> {
        try!(self.myconn.conn.exec("COMMIT TRANSACTION").map_err(elmo::wrap_err));
        Ok(())
    }

    fn rollback(self: Box<Self>) -> Result<()> {
        try!(self.myconn.conn.exec("ROLLBACK TRANSACTION").map_err(elmo::wrap_err));
        Ok(())
    }

    // TODO maybe just move all the stuff below from the private section into here?

    fn create_collection(&self, db: &str, coll: &str, options: BsonValue) -> Result<bool> {
        self.base_create_collection(db, coll, options)
    }

    fn drop_collection(&self, db: &str, coll: &str) -> Result<bool> {
        self.base_drop_collection(db, coll)
    }

    fn create_indexes(&self, what: Vec<elmo::IndexInfo>) -> Result<Vec<bool>> {
        self.base_create_indexes(what)
    }

    fn rename_collection(&self, old_name: &str, new_name: &str, drop_target: bool) -> Result<bool> {
        self.base_rename_collection(old_name, new_name, drop_target)
    }

    fn drop_index(&self, db: &str, coll: &str, name: &str) -> Result<bool> {
        self.base_drop_index(db, coll, name)
    }

    fn drop_database(&self, db: &str) -> Result<bool> {
        self.base_drop_database(db)
    }

    fn clear_collection(&self, db: &str, coll: &str) -> Result<bool> {
        self.base_clear_collection(db, coll)
    }

}

// TODO do we need to declare that StorageWriter must implement Drop ?
impl<'a> Drop for MyWriter<'a> {
    fn drop(&mut self) {
        // TODO should rollback be the default here?  or commit?
        // TODO can't do this unless we know that commit() or rollback()
        // was not called.  Need a flag.
        let _ignored = self.myconn.conn.exec("ROLLBACK TRANSACTION");
    }
}

// TODO do we need to declare that StorageReader must implement Drop ?
impl<'a> Drop for MyReader<'a> {
    fn drop(&mut self) {
        // this transaction was [supposed to be] read-only, so it doesn't
        // matter in principle whether we commit or rollback.  in SQL Server,
        // if temp tables were created, commit is MUCH faster than rollback.
        // but this is sqlite.  anyway...
        let _ignored = self.myconn.conn.exec("COMMIT TRANSACTION");
    }
}

impl<'a> MyReader<'a> {
}

impl<'a> elmo::StorageReader for MyReader<'a> {
    fn get_collection_reader<'b>(&'b self, db: &str, coll: &str, plan: Option<elmo::QueryPlan>) -> Result<Box<elmo::StorageCollectionReader<Item=elmo::Result<BsonValue>> + 'b>> {
        let rdr = try!(self.myconn.get_collection_reader(db, coll, plan));
        Ok(rdr)
    }

    fn list_collections(&self) -> Result<Vec<(String, String, BsonValue)>> {
        self.myconn.base_list_collections()
    }

    fn list_indexes(&self) -> Result<Vec<elmo::IndexInfo>> {
        self.myconn.base_list_indexes()
    }

}

impl<'a> elmo::StorageReader for MyWriter<'a> {
    fn get_collection_reader<'b>(&'b self, db: &str, coll: &str, plan: Option<elmo::QueryPlan>) -> Result<Box<elmo::StorageCollectionReader<Item=elmo::Result<BsonValue>> + 'b>> {
        let rdr = try!(self.myconn.get_collection_reader(db, coll, plan));
        Ok(rdr)
    }

    fn list_collections(&self) -> Result<Vec<(String, String, BsonValue)>> {
        self.myconn.base_list_collections()
    }

    fn list_indexes(&self) -> Result<Vec<elmo::IndexInfo>> {
        self.myconn.base_list_indexes()
    }

}

impl elmo::StorageConnection for MyConn {
    fn begin_write<'a>(&'a self) -> Result<Box<elmo::StorageWriter + 'a>> {
        try!(self.conn.exec("BEGIN IMMEDIATE TRANSACTION").map_err(elmo::wrap_err));
        let w = MyWriter {
            myconn: self,
        };
        Ok(box w)
    }

    fn begin_read<'a>(&'a self) -> Result<Box<elmo::StorageReader + 'a>> {
        try!(self.conn.exec("BEGIN TRANSACTION").map_err(elmo::wrap_err));
        let r = MyReader {
            myconn: self,
        };
        Ok(box r)
    }
}

impl MyNormalCollectionReader {
    fn iter_next(&mut self) -> Result<Option<BsonValue>> {
        // TODO can't find a way to store the ResultSet from execute()
        // in the same struct as its statement, because the ResultSet()
        // contains a mut reference to the statement.  So we look at
        // the implementation of execute() and realize that it doesn't
        // actually do anything of substance, so we call it every
        // time.  Ugly.
        match try!(self.stmt.execute().step().map_err(elmo::wrap_err)) {
            None => Ok(None),
            Some(r) => {
                let b = r.column_blob(0).expect("NOT NULL");
                let v = try!(BsonValue::from_bson(&b));
                Ok(Some(v))
            },
        }
    }
}

impl MyTextCollectionReader {
    fn iter_next(&mut self) -> Result<Option<BsonValue>> {
        unimplemented!();
    }
}

impl elmo::StorageCollectionReader for MyNormalCollectionReader {
    fn get_total_keys_examined(&self) -> u64 {
        // TODO
        0
    }

}

impl elmo::StorageCollectionReader for MyEmptyCollectionReader {
    fn get_total_keys_examined(&self) -> u64 {
        0
    }

}

impl elmo::StorageCollectionReader for MyTextCollectionReader {
    fn get_total_keys_examined(&self) -> u64 {
        // TODO
        0
    }

}

impl Iterator for MyNormalCollectionReader {
    type Item = Result<BsonValue>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.iter_next() {
            Err(e) => {
                return Some(Err(e));
            },
            Ok(v) => {
                match v {
                    None => {
                        return None;
                    },
                    Some(v) => {
                        return Some(Ok(v));
                    }
                }
            },
        }
    }
}

impl Iterator for MyEmptyCollectionReader {
    type Item = Result<BsonValue>;
    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl Iterator for MyTextCollectionReader {
    type Item = Result<BsonValue>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.iter_next() {
            Err(e) => {
                return Some(Err(e));
            },
            Ok(v) => {
                match v {
                    None => {
                        return None;
                    },
                    Some(v) => {
                        return Some(Ok(v));
                    }
                }
            },
        }
    }
}

fn base_connect(name: &str) -> sqlite3::SqliteResult<sqlite3::DatabaseConnection> {
    let access = sqlite3::access::ByFilename { flags: sqlite3::access::flags::OPEN_READWRITE | sqlite3::access::flags::OPEN_CREATE, filename: name};
    let conn = try!(sqlite3::DatabaseConnection::new(access));
    try!(conn.exec("PRAGMA journal_mode=WAL"));
    try!(conn.exec("PRAGMA foreign_keys=ON"));
    try!(conn.exec("CREATE TABLE IF NOT EXISTS \"collections\" (dbName TEXT NOT NULL, collName TEXT NOT NULL, options BLOB NOT NULL, PRIMARY KEY (dbName,collName))"));
    try!(conn.exec("CREATE TABLE IF NOT EXISTS \"indexes\" (dbName TEXT NOT NULL, collName TEXT NOT NULL, ndxName TEXT NOT NULL, spec BLOB NOT NULL, options BLOB NOT NULL, PRIMARY KEY (dbName, collName, ndxName), FOREIGN KEY (dbName,collName) REFERENCES \"collections\" ON DELETE CASCADE ON UPDATE CASCADE, UNIQUE (spec,dbName,collName))"));

    Ok(conn)
}

pub fn connect(name: &str) -> Result<Box<elmo::StorageConnection>> {
    let conn = try!(base_connect(name).map_err(elmo::wrap_err));
    let c = MyConn {
        conn: conn,
    };
    Ok(box c)
}

/*

look at the non-allocating alternatives to column_text() and column_blob()

*/

