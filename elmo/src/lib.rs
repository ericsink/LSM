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

#![feature(convert)]
#![feature(box_syntax)]
#![feature(associated_consts)]

use std::collections::HashMap;
use std::collections::HashSet;
use std::cmp::Ordering;

extern crate misc;

use misc::endian;
use misc::bufndx;
use misc::varint;

extern crate bson;

#[derive(Debug)]
// TODO do we really want this public?
pub enum Error {
    // TODO remove Misc
    Misc(String),

    // TODO more detail within CorruptFile
    CorruptFile(&'static str),

    Bson(bson::Error),
    Io(std::io::Error),
    Utf8(std::str::Utf8Error),
    Whatever(Box<std::error::Error>),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Error::Bson(ref err) => write!(f, "bson error: {}", err),
            Error::Io(ref err) => write!(f, "IO error: {}", err),
            Error::Utf8(ref err) => write!(f, "Utf8 error: {}", err),
            Error::Whatever(ref err) => write!(f, "Other error: {}", err),
            Error::Misc(ref s) => write!(f, "Misc error: {}", s),
            Error::CorruptFile(s) => write!(f, "Corrupt file: {}", s),
        }
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Bson(ref err) => std::error::Error::description(err),
            Error::Io(ref err) => std::error::Error::description(err),
            Error::Utf8(ref err) => std::error::Error::description(err),
            Error::Whatever(ref err) => std::error::Error::description(&**err),
            Error::Misc(ref s) => s.as_str(),
            Error::CorruptFile(s) => s,
        }
    }

    // TODO cause
}

// TODO why is 'static needed here?  Doesn't this take ownership?
pub fn wrap_err<E: std::error::Error + 'static>(err: E) -> Error {
    Error::Whatever(box err)
}

impl From<bson::Error> for Error {
    fn from(err: bson::Error) -> Error {
        Error::Bson(err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::Io(err)
    }
}

// TODO not sure this is useful
impl From<Box<std::error::Error>> for Error {
    fn from(err: Box<std::error::Error>) -> Error {
        Error::Whatever(err)
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(err: std::str::Utf8Error) -> Error {
        Error::Utf8(err)
    }
}

/*
impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(_err: std::sync::PoisonError<T>) -> Error {
        Error::Poisoned
    }
}

impl<'a, E: Error + 'a> From<E> for Error {
    fn from(err: E) -> Error {
        Error::Whatever(err)
    }
}
*/

pub type Result<T> = std::result::Result<T, Error>;

mod matcher;

pub struct CollectionInfo {
    pub db: String,
    pub coll: String,
    pub options: bson::Document,
}

// TODO remove derive Clone later
#[derive(Clone,Debug)]
pub struct IndexInfo {
    pub db: String,
    pub coll: String,
    pub name: String,
    pub spec: bson::Document,
    pub options: bson::Document,
}

impl IndexInfo {
    pub fn full_collection_name(&self) -> String {
        format!("{}.{}", self.db, self.coll)
    }
}

pub type QueryKey = Vec<bson::Value>;

#[derive(Hash,PartialEq,Eq,Debug,Clone)]
pub enum TextQueryTerm {
    Word(bool, String),
    Phrase(bool, String),
}

pub enum QueryBounds {
    EQ(QueryKey),
    // TODO tempted to make the QueryKey in Text be an option
    Text(QueryKey,Vec<TextQueryTerm>),
    GT(QueryKey),
    GTE(QueryKey),
    LT(QueryKey),
    LTE(QueryKey),
    GT_LT(QueryKey, QueryKey),
    GT_LTE(QueryKey, QueryKey),
    GTE_LT(QueryKey, QueryKey),
    GTE_LTE(QueryKey, QueryKey),
}

pub struct QueryPlan {
    pub ndx: IndexInfo,
    pub bounds: QueryBounds,
}

#[derive(PartialEq,Copy,Clone)]
enum OpIneq {
    LT,
    GT,
    LTE,
    GTE,
}

impl OpIneq {
    fn is_gt(self) -> bool {
        match self {
            OpIneq::LT => false,
            OpIneq::LTE => false,
            OpIneq::GT => true,
            OpIneq::GTE => true,
        }
    }
}

#[derive(PartialEq,Copy,Clone)]
enum OpLt {
    LT,
    LTE,
}

#[derive(PartialEq,Copy,Clone)]
enum OpGt {
    GT,
    GTE,
}

// TODO I dislike the name of this.  also, consider making it a trait.
pub struct Row {
    // TODO I wish this were bson::Document.  but when you have a reference to a
    // bson::Document and what you need is a bson::Value, you can't get there,
    // because you need ownership and you don't have it.  So clone.  Which is
    // awful.
    pub doc: bson::Value,
    // TODO score
    // TODO pos
    // TODO stats for explain
}

pub fn cmp_row(d: &Row, lit: &Row) -> Ordering {
    matcher::cmp(&d.doc, &lit.doc)
}

enum UpdateOp {
    Min(String, bson::Value),
    Max(String, bson::Value),
    Inc(String, bson::Value),
    Mul(String, bson::Value),
    Set(String, bson::Value),
    PullValue(String, bson::Value),
    SetOnInsert(String, bson::Value),
    BitAnd(String, i64),
    BitOr(String, i64),
    BitXor(String, i64),
    Unset(String),
    Date(String),
    TimeStamp(String),
    Rename(String, String),
    AddToSet(String, Vec<bson::Value>),
    PullAll(String, Vec<bson::Value>),
    // TODO push
    PullQuery(String, matcher::QueryDoc),
    PullPredicates(String, Vec<matcher::Pred>),
    Pop(String, i32),
}

pub trait StorageBase {
    // TODO maybe these two should return an iterator
    // TODO maybe these two should accept params to limit the rows returned
    fn list_collections(&self) -> Result<Vec<CollectionInfo>>;
    fn list_indexes(&self) -> Result<Vec<IndexInfo>>;

    fn get_collection_reader(&self, db: &str, coll: &str, plan: Option<QueryPlan>) -> Result<Box<Iterator<Item=Result<Row>> + 'static>>;
}

pub trait StorageCollectionWriter {
    fn insert(&mut self, v: &bson::Document) -> Result<()>;
    fn update(&mut self, v: &bson::Document) -> Result<()>;
    // TODO arg to delete should be what?
    fn delete(&mut self, v: &bson::Value) -> Result<bool>;
}

// TODO should implement Drop = rollback
// TODO do we need to declare that StorageWriter must implement Drop ?
// TODO or is it enough that the actual implementation of this trait impl Drop?

pub trait StorageReader : StorageBase {
    fn into_collection_reader(self: Box<Self>, db: &str, coll: &str, plan: Option<QueryPlan>) -> Result<Box<Iterator<Item=Result<Row>> + 'static>>;
}

pub trait StorageWriter : StorageBase {
    fn create_collection(&self, db: &str, coll: &str, options: bson::Document) -> Result<bool>;
    fn rename_collection(&self, old_name: &str, new_name: &str, drop_target: bool) -> Result<bool>;
    fn clear_collection(&self, db: &str, coll: &str) -> Result<bool>;
    fn drop_collection(&self, db: &str, coll: &str) -> Result<bool>;

    fn create_indexes(&self, Vec<IndexInfo>) -> Result<Vec<bool>>;
    fn drop_index(&self, db: &str, coll: &str, name: &str) -> Result<bool>;

    fn drop_database(&self, db: &str) -> Result<bool>;

    fn get_collection_writer(&self, db: &str, coll: &str) -> Result<Box<StorageCollectionWriter + 'static>>;

    fn commit(self: Box<Self>) -> Result<()>;
    fn rollback(self: Box<Self>) -> Result<()>;

}

// TODO I'm not sure this type is worth the trouble anymore.
// maybe we should go back to just keeping a bool that specifies
// whether we need to negate or not.
#[derive(PartialEq,Copy,Clone)]
pub enum IndexType {
    Forward,
    Backward,
    Geo2d,
}

fn decode_index_type(v: &bson::Value) -> IndexType {
    match v {
        &bson::Value::BInt32(n) => if n<0 { IndexType::Backward } else { IndexType::Forward },
        &bson::Value::BInt64(n) => if n<0 { IndexType::Backward } else { IndexType::Forward },
        &bson::Value::BDouble(n) => if n<0.0 { IndexType::Backward } else { IndexType::Forward },
        &bson::Value::BString(ref s) => if s == "2d" { 
            IndexType::Geo2d 
        } else { 
            panic!("decode_index_type")
        },
        _ => panic!("decode_index_type")
    }
}

// TODO this is basically iter().position()
fn slice_find(pairs: &[(String, bson::Value)], s: &str) -> Option<usize> {
    for i in 0 .. pairs.len() {
        match pairs[0].1 {
            bson::Value::BString(ref t) => {
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
pub fn get_normalized_spec(info: &IndexInfo) -> Result<(Vec<(String,IndexType)>,Option<HashMap<String,i32>>)> {
    //printfn "info: %A" info
    let first_text = slice_find(&info.spec.pairs, "text");
    let w1 = info.options.get("weights");
    match (first_text, w1) {
        (None, None) => {
            let decoded = info.spec.pairs.iter().map(|&(ref k, ref v)| (k.clone(), decode_index_type(v))).collect::<Vec<(String,IndexType)>>();
            //printfn "no text index: %A" decoded
            Ok((decoded, None))
        },
        _ => {
            let (scalar_keys, text_keys) = 
                match first_text {
                    Some(i) => {
                        let scalar_keys = &info.spec.pairs[0 .. i];
                        // note that any non-text index after the first text index is getting discarded
                        let mut text_keys = Vec::new();
                        for t in &info.spec.pairs {
                            match t.1 {
                                bson::Value::BString(ref s) => {
                                    if s == "text" {
                                        text_keys.push(t.0.clone());
                                    }
                                },
                                _ => (),
                            }
                        }
                        (scalar_keys, text_keys)
                    },
                    None => (&info.spec.pairs[0 ..], Vec::new())
                };
            let mut weights = HashMap::new();
            match w1 {
                Some(&bson::Value::BDocument(ref bd)) => {
                    for t in &bd.pairs {
                        let n = 
                            match &t.1 {
                                &bson::Value::BInt32(n) => n,
                                &bson::Value::BInt64(n) => n as i32,
                                &bson::Value::BDouble(n) => n as i32,
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


pub trait StorageConnection {
    fn begin_write(&self) -> Result<Box<StorageWriter + 'static>>;
    fn begin_read(&self) -> Result<Box<StorageReader + 'static>>;
    // TODO note that only one tx can exist at a time per connection.

    // but it would be possible to have multiple iterators at the same time.
    // as long as they live within the same tx.
}

pub struct Connection {
    conn: Box<StorageConnection>,
}

impl Connection {
    pub fn new(conn: Box<StorageConnection>) -> Connection {
        Connection {
            conn: conn,
        }
    }

    fn apply_update_ops(doc: &mut bson::Document, ops: &Vec<UpdateOp>, is_upsert: bool, pos: Option<usize>) -> Result<()> {
        for op in ops {
            match op {
                &UpdateOp::Min(ref path, ref v) => {
                    match try!(doc.entry(path)) {
                        bson::Entry::Found(e) => {
                            let c = {
                                let cur = e.get();
                                matcher::cmp(v, cur)
                            };
                            if c == Ordering::Less {
                                // TODO clone
                                e.replace(v.clone());
                            }
                        },
                        bson::Entry::Absent(e) => {
                            // when the key isn't found, $min works like $set
                            // TODO clone
                            e.insert(v.clone());
                        },
                    }
                },
                &UpdateOp::Max(ref path, ref v) => {
                    panic!("TODO UpdateOp::Max");
                },
                &UpdateOp::Inc(ref path, ref v) => {
                    panic!("TODO UpdateOp::Inc");
                },
                &UpdateOp::Mul(ref path, ref v) => {
                    panic!("TODO UpdateOp::Mul");
                },
                &UpdateOp::Set(ref path, ref v) => {
                    panic!("TODO UpdateOp::Set");
                },
                &UpdateOp::PullValue(ref path, ref v) => {
                    panic!("TODO UpdateOp::PullValue");
                },
                &UpdateOp::SetOnInsert(ref path, ref v) => {
                    panic!("TODO UpdateOp::SetOnInsert");
                },
                &UpdateOp::BitAnd(ref path, v) => {
                    panic!("TODO UpdateOp::BitAnd");
                },
                &UpdateOp::BitOr(ref path, v) => {
                    panic!("TODO UpdateOp::BitOr");
                },
                &UpdateOp::BitXor(ref path, v) => {
                    panic!("TODO UpdateOp::BitXor");
                },
                &UpdateOp::Unset(ref path) => {
                    panic!("TODO UpdateOp::Unset");
                },
                &UpdateOp::Date(ref path) => {
                    panic!("TODO UpdateOp::Date");
                },
                &UpdateOp::TimeStamp(ref path) => {
                    panic!("TODO UpdateOp::Timestamp");
                },
                &UpdateOp::Rename(ref path, ref name) => {
                    panic!("TODO UpdateOp::Rename");
                },
                &UpdateOp::AddToSet(ref path, ref v) => {
                    panic!("TODO UpdateOp::AddToSet");
                },
                &UpdateOp::PullAll(ref path, ref v) => {
                    panic!("TODO UpdateOp::PullAll");
                },
                &UpdateOp::PullQuery(ref path, ref qd) => {
                    panic!("TODO UpdateOp::PullQuery");
                },
                &UpdateOp::PullPredicates(ref path, ref preds) => {
                    panic!("TODO UpdateOp::PullPredicates");
                },
                &UpdateOp::Pop(ref path, i) => {
                    panic!("TODO UpdateOp::Pop");
                },
            }
        }
        // TODO return what?
        Ok(())
    }

    fn parse_update_doc(d: bson::Document) -> Result<Vec<UpdateOp>> {
        // TODO benefit of map/collect over for loop is that it forces something for every item
        let mut result = vec![];
        for (k, v) in d.pairs {
            match k.as_str() {
                "$min" => {
                    for (path, v) in try!(v.into_document()).pairs {
                        result.push(UpdateOp::Min(path, v));
                    }
                },
                _ => return Err(Error::Misc(format!("unknown update op: {}", k))),
            }
        }
        Ok(result)
    }

    fn get_one_match(db: &str, coll: &str, w: &StorageWriter, m: &matcher::QueryDoc) -> Result<Option<Row>> {
        let indexes = try!(w.list_indexes()).into_iter().filter(
            |ndx| ndx.db == db && ndx.coll == coll
            ).collect::<Vec<_>>();
        let plan = try!(Self::choose_index(&indexes, &m, None));
        let mut seq: Box<Iterator<Item=Result<Row>>> = try!(w.get_collection_reader(db, coll, plan));
        seq = box seq
            .filter(
                move |r| {
                    if let &Ok(ref d) = r {
                        matcher::match_query(&m, &d.doc)
                    } else {
                        // TODO so when we have an error we just let it through?
                        true
                    }
                }
        );
        // TODO is take() the right thing here?
        let mut a = try!(seq.take(1).collect::<Result<Vec<_>>>());
        let d = misc::remove_first_if_exists(&mut a);
        Ok(d)
    }

    // TODO this func needs to return the 4-tuple
    // (count_matches, count_modified, Option<TODO>, Option<TODO>)
    pub fn update(&self, db: &str, coll: &str, updates: &mut Vec<bson::Document>) -> Result<Vec<Result<()>>> {
        // TODO need separate conn?
        let mut results = Vec::new();
        {
            let writer = try!(self.conn.begin_write());
            {
                let mut collwriter = try!(writer.get_collection_writer(db, coll));
                // TODO why does this closure need to be mut?
                let mut one_update_or_upsert = |upd: &mut bson::Document| -> Result<()> {
                    let q = try!(upd.must_remove_document("q"));
                    let mut u = try!(upd.must_remove_document("u"));
                    let multi = try!(upd.must_remove_bool("multi"));
                    let upsert = try!(upd.must_remove_bool("upsert"));
                    let m = try!(matcher::parse_query(q));
                    let has_update_operators = u.pairs.iter().any(|&(ref k, _)| !k.starts_with("$"));
                    if has_update_operators {
                        let ops = try!(Self::parse_update_doc(u));
                        let (count_matches, count_modified) =
                            if multi {
                                panic!("TODO update operators multi");
                            } else {
                                match try!(Self::get_one_match(db, coll, &*writer, &m)) {
                                    Some(row) => {
                                        let mut doc = try!(row.doc.into_document());
                                        Self::apply_update_ops(&mut doc, &ops, false, None);
                                        // TODO make sure _id did not change
                                        // TODO only do the actual update if a change happened.  clone and compare?
                                        // TODO basic_update, validate_keys
                                        collwriter.update(&doc);
                                        // TODO return (1, 0) if the change didn't happen
                                        (1, 1)
                                    },
                                    None => (0, 0),
                                }
                            };
                        if count_matches == 0 {
                            if upsert {
                                panic!("TODO update operators upsert");
                            } else {
                                Ok(())
                                //Ok((count_matches, count_modified, None, None))
                            }
                        } else {
                            Ok(())
                            //Ok((count_matches, count_modified, None, None))
                        }
                    } else {
                        // TODO what happens if the update document has no update operators
                        // but it has keys which are dotted?
                        if multi {
                            return Err(Error::Misc(String::from("multi update requires $ update operators")));
                        }
                        match try!(Self::get_one_match(db, coll, &*writer, &m)) {
                            Some(row) => {
                                let doc = try!(row.doc.as_document());
                                let id1 = try!(doc.get("_id").ok_or(Error::Misc(String::from("_id not found in doc being updated"))));
                                let id1 = try!(id1.as_objectid());
                                // TODO if u has _id, make sure it's the same
                                u.set_objectid("_id", id1);
                                // TODO basic_update, validate_keys
                                collwriter.update(&u);
                                // TODO return something
                                Ok(())
                            },
                            None => {
                                if upsert {
                                    panic!("TODO upsert");
                                } else {
                                    // TODO (0,0,None,None)
                                    panic!("TODO nothing updated");
                                }
                            },
                        }
                    }
                };

                for upd in updates {
                    let r = one_update_or_upsert(upd);
                    results.push(r);
                }
            }
            try!(writer.commit());
        }
        Ok(results)
    }

    pub fn insert(&self, db: &str, coll: &str, docs: &mut Vec<bson::Document>) -> Result<Vec<Result<()>>> {
        // make sure every doc has an _id
        for d in docs.iter_mut() {
            match d.get("_id") {
                Some(_) => {
                },
                None => {
                    d.set_objectid("_id", misc::new_bson_objectid_rand());
                },
            }
        }
        let mut results = Vec::new();
        {
            let writer = try!(self.conn.begin_write());
            {
                let mut collwriter = try!(writer.get_collection_writer(db, coll));
                for doc in docs {
                    // TODO basic_insert, validate_keys
                    let r = collwriter.insert(doc);
                    results.push(r);
                }
            }
            try!(writer.commit());
        }
        Ok(results)
    }

    pub fn list_collections(&self) -> Result<Vec<CollectionInfo>> {
        let reader = try!(self.conn.begin_read());
        let v = try!(reader.list_collections());
        Ok(v)
    }

    pub fn list_indexes(&self) -> Result<Vec<IndexInfo>> {
        let reader = try!(self.conn.begin_read());
        let v = try!(reader.list_indexes());
        Ok(v)
    }

    fn try_find_index_by_name_or_spec<'a>(indexes: &'a Vec<IndexInfo>, desc: &bson::Value) -> Option<&'a IndexInfo> {
        let mut a =
            match desc {
                &bson::Value::BString(ref s) => {
                    indexes.into_iter().filter(|ndx| ndx.name.as_str() == s.as_str()).collect::<Vec<_>>()
                },
                &bson::Value::BDocument(ref bd) => {
                    indexes.into_iter().filter(|ndx| ndx.spec == *bd).collect::<Vec<_>>()
                },
                _ => panic!("must be name or index spec doc"),
            };
        if a.len() > 1 {
            unreachable!();
        } else {
            a.pop()
        }
    }

    pub fn delete_indexes(&self, db: &str, coll: &str, index: &bson::Value) -> Result<(usize, usize)> {
        let writer = try!(self.conn.begin_write());
        // TODO make the following filter DRY
        let indexes = try!(writer.list_indexes()).into_iter().filter(
            |ndx| ndx.db == db && ndx.coll == coll
            ).collect::<Vec<_>>();
        let count_before = indexes.len();
        let indexes = 
            if index.is_string() && try!(index.as_str()) == "*" {
                indexes.iter().filter(
                    |ndx| ndx.name != "_id_"
                ).collect::<Vec<_>>()
            } else {
                // TODO we're supposed to disallow delete of _id_, right?
                // TODO if let
                match Self::try_find_index_by_name_or_spec(&indexes, index) {
                    Some(ndx) => vec![ndx],
                    None => vec![],
                }
            };
        try!(writer.commit());
        panic!("TODO delete_indexes");
    }

    pub fn create_indexes(&self, indexes: Vec<IndexInfo>) -> Result<Vec<bool>> {
        let writer = try!(self.conn.begin_write());
        let results = try!(writer.create_indexes(indexes));
        try!(writer.commit());
        Ok(results)
    }

    pub fn drop_collection(&self, db: &str, coll: &str) -> Result<bool> {
        let deleted = {
            let writer = try!(self.conn.begin_write());
            let deleted = try!(writer.drop_collection(db, coll));
            try!(writer.commit());
            deleted
        };
        Ok(deleted)
    }

    pub fn drop_database(&self, db: &str) -> Result<bool> {
        let deleted = {
            let writer = try!(self.conn.begin_write());
            let deleted = try!(writer.drop_database(db));
            try!(writer.commit());
            deleted
        };
        Ok(deleted)
    }

    pub fn delete(&self, db: &str, coll: &str, items: &Vec<bson::Value>) -> Result<usize> {
        let mut count = 0;
        {
            let writer = try!(self.conn.begin_write());
            {
                let mut collwriter = try!(writer.get_collection_writer(db, coll));
                for del in items {
                    // TODO
                    /*
                    let q = bson.getValueForKey upd "q"
                    let limit = bson.tryGetValueForKey upd "limit"
                    let m = Matcher.parseQuery q
                    // TODO is this safe?  or do we need two-conn isolation like update?
                    let indexes = w.getIndexes()
                    let plan = chooseIndex indexes m None
                    let {docs=s;funk=funk} = w.getSelect plan
                    try
                        s |> seqMatch m |> 
                            Seq.iter (fun {doc=doc} -> 
                                // TODO is it possible to delete from an autoIndexId=false collection?
                                let id = bson.getValueForKey doc "_id"
                                if basicDelete w id then
                                    count := !count + 1
                                )
                    finally
                        funk()
                    */
                }
            }
            try!(writer.commit());
        }
        Ok(count)
    }

    pub fn create_collection(&self, db: &str, coll: &str, options: bson::Document) -> Result<bool> {
        let writer = try!(self.conn.begin_write());
        let result = try!(writer.create_collection(db, coll, options));
        try!(writer.commit());
        Ok(result)
    }

    fn parse_index_min_max(v: bson::Value) -> Result<Vec<(String,bson::Value)>> {
        let v = try!(v.into_document());
        let matcher::QueryDoc::QueryDoc(items) = try!(matcher::parse_query(v));
        items.into_iter().map(
            |it| match it {
                // TODO wish we could pattern match on the vec.  can we?
                matcher::QueryItem::Compare(k, mut preds) => {
                    if preds.len() == 1 {
                        match preds.pop().expect("just checked") {
                            matcher::Pred::EQ(v) => {
                                Ok((k, v))
                            },
                            _ => {
                                Err(Error::Misc(String::from("bad min max")))
                            },
                        }
                    } else {
                        Err(Error::Misc(String::from("bad min max")))
                    }
                },
                _ => {
                    Err(Error::Misc(String::from("bad min max")))
                },
            }
        ).collect::<Result<Vec<(_,_)>>>()
    }

    fn find_compares_eq(m: &matcher::QueryDoc) -> Result<HashMap<&str, &bson::Value>> {
        fn find<'a>(m: &'a matcher::QueryDoc, dest: &mut Vec<(&'a str, &'a bson::Value)>) {
            let &matcher::QueryDoc::QueryDoc(ref a) = m;
            for it in a {
                match it {
                    &matcher::QueryItem::Compare(ref k, ref preds) => {
                        for p in preds {
                            match p {
                                &matcher::Pred::EQ(ref v) => dest.push((k,v)),
                                _ => (),
                            }
                        }
                    },
                    &matcher::QueryItem::AND(ref docs) => {
                        for d in docs {
                            find(d, dest);
                        }
                    },
                    _ => {
                    },
                }
            }
        }

        let mut comps = vec![];
        find(m, &mut comps);

        let mut mc = misc::group_by_key(comps);

        // query for x=3 && x=4 is legit in mongo.
        // it can match a doc where x is an array that contains both 3 and 4
        // {x:[1,2,3,4,5]}
        // in terms of choosing an index to use, we can pick either one.
        // the index will give us, for example, "all documents where x is 3",
        // which will include the one above, and the matcher will then also
        // make sure that the 4 is there as well.

        let mc = 
            try!(mc.into_iter().map(
                    |(k, mut v)| 
                    if v.len() == 0 {
                        unreachable!();
                    } else if v.len() == 1 {
                        let v = v.pop().expect("len() == 1");
                        Ok((k, v))
                    } else {
                        let count_distinct = {
                            let uniq : HashSet<_> = v.iter().collect();
                            uniq.len()
                        };
                        if count_distinct > 1 {
                            Err(Error::Misc(String::from("conflicting $eq")))
                        } else {
                            let v = v.pop().expect("len() > 0");
                            Ok((k, v))
                        }
                    }
                    ).collect::<Result<HashMap<_,_>>>()
                );

        Ok(mc)
    }

    fn find_compares_ineq(m: &matcher::QueryDoc) -> Result<HashMap<&str, (Option<(OpGt, &bson::Value)>, Option<(OpLt, &bson::Value)>)>> {
        fn find<'a>(m: &'a matcher::QueryDoc, dest: &mut Vec<(&'a str, (OpIneq, &'a bson::Value))>) {
            let &matcher::QueryDoc::QueryDoc(ref a) = m;
            for it in a {
                match it {
                    &matcher::QueryItem::Compare(ref k, ref preds) => {
                        for p in preds {
                            match p {
                                &matcher::Pred::LT(ref v) => dest.push((k, (OpIneq::LT,v))),
                                &matcher::Pred::GT(ref v) => dest.push((k, (OpIneq::GT,v))),
                                &matcher::Pred::LTE(ref v) => dest.push((k, (OpIneq::LTE,v))),
                                &matcher::Pred::GTE(ref v) => dest.push((k, (OpIneq::GTE,v))),
                                _ => (),
                            }
                        }
                    },
                    &matcher::QueryItem::AND(ref docs) => {
                        for d in docs {
                            find(d, dest);
                        }
                    },
                    _ => {
                    },
                }
            }
        }

        fn cmp_gt(t1: &(OpGt, &bson::Value), t2: &(OpGt, &bson::Value)) -> Ordering {
            let c = matcher::cmp(t1.1, t2.1);
            match c {
                Ordering::Less => c,
                Ordering::Greater => c,
                Ordering::Equal => {
                    match (t1.0, t2.0) {
                        (OpGt::GT, OpGt::GT) => Ordering::Equal,
                        (OpGt::GTE, OpGt::GTE) => Ordering::Equal,
                        (OpGt::GT, OpGt::GTE) => Ordering::Less,
                        (OpGt::GTE, OpGt::GT) => Ordering::Greater,
                    }
                },
            }
        }

        fn cmp_lt(t1: &(OpLt, &bson::Value), t2: &(OpLt, &bson::Value)) -> Ordering {
            let c = matcher::cmp(t1.1, t2.1);
            match c {
                Ordering::Less => c,
                Ordering::Greater => c,
                Ordering::Equal => {
                    match (t1.0, t2.0) {
                        (OpLt::LT, OpLt::LT) => Ordering::Equal,
                        (OpLt::LTE, OpLt::LTE) => Ordering::Equal,
                        (OpLt::LT, OpLt::LTE) => Ordering::Less,
                        (OpLt::LTE, OpLt::LT) => Ordering::Greater,
                    }
                },
            }
        }

        fn to_lt(op: OpIneq) -> Option<OpLt> {
            match op {
                OpIneq::LT => Some(OpLt::LT),
                OpIneq::LTE => Some(OpLt::LTE),
                OpIneq::GT => None,
                OpIneq::GTE => None,
            }
        }

        fn to_gt(op: OpIneq) -> Option<OpGt> {
            match op {
                OpIneq::LT => None,
                OpIneq::LTE => None,
                OpIneq::GT => Some(OpGt::GT),
                OpIneq::GTE => Some(OpGt::GTE),
            }
        }

        let mut comps = vec![];
        find(m, &mut comps);

        let mut mc = misc::group_by_key(comps);

        let mut m2 = HashMap::new();

        for (k, a) in mc {
            let (gt, lt): (Vec<_>, Vec<_>) = a.into_iter().partition(|&(op, _)| op.is_gt());

            let mut gt = 
                gt
                .into_iter()
                // TODO in the following line, since we already partitioned, else/None should be unreachable
                .filter_map(|(op, v)| if let Some(gt) = to_gt(op) { Some((gt, v)) } else { None })
                .collect::<Vec<_>>();
            let gt = {
                gt.sort_by(cmp_gt);
                misc::remove_first_if_exists(&mut gt)
            };
            
            let mut lt = 
                lt
                .into_iter()
                // TODO in the following line, since we already partitioned, else/None should be unreachable
                .filter_map(|(op, v)| if let Some(lt) = to_lt(op) { Some((lt, v)) } else { None })
                .collect::<Vec<_>>();
            let lt = {
                lt.sort_by(cmp_lt);
                misc::remove_first_if_exists(&mut lt)
            };
            
            // Note that if we wanted to disallow > and < the same value, this is
            // where we would do it, but mongo allows this according to test case
            // find8.js

            // TODO issue here of diving into elemMatch or not:
            // TODO we cannot do a query with both bounds unless the two
            // comparisons came from the same elemMatch.
            // for example:
            // {x:{$gt:2,$lt:5}}
            // this query has to match the following document:
            // {x:[1,7]}
            // because the array x has
            // something that matches x>2 (the 7)
            // AND
            // something that matches x<5 (the 1)
            // even those two somethings are not the same thing,
            // they came from the same x.
            // we can choose x>2 or x<5 as our index, but we can't choose both.
            // unless elemMatch.
            //
            // note that if we have to satisfy two gt on x, such as:
            // x>5
            // x>9
            // it doesn't really matter which one we choose for the index.
            // both will be correct.  but choosing x>9 will result in us reviewing
            // fewer documents.

            m2.insert(k, (gt, lt));
        }


        Ok(m2)
    }

    fn fit_index_to_query(
        ndx: &IndexInfo, 
        comps_eq: &HashMap<&str, &bson::Value>, 
        comps_ineq: &HashMap<&str, (Option<(OpGt, &bson::Value)>, Option<(OpLt, &bson::Value)>)>, 
        text_query: &Option<Vec<TextQueryTerm>>
        ) 
        -> Result<Option<QueryPlan>> 
    {
        let (scalar_keys, weights) = try!(get_normalized_spec(ndx));
        if weights.is_none() && text_query.is_some() {
            // if there is a textQuery but this is not a text index, give up now
            Ok(None)
        } else {
            // TODO this code assumes that everything is either scalar or text, which
            // will be wrong when geo comes along.
            if scalar_keys.len() == 0 {
                match weights {
                    Some(ref weights) => {
                        match text_query {
                            &None => {
                                // if there is no textQuery, give up
                                Ok(None)
                            },
                            &Some(ref text_query) => {
                                // TODO clone
                                let bounds = QueryBounds::Text(vec![], text_query.clone());
                                let plan = QueryPlan {
                                    // TODO clone
                                    ndx: ndx.clone(),
                                    bounds: bounds,
                                };
                                Ok(Some(plan))
                            },
                        }
                    },
                    None => {
                        // er, why are we here?
                        // index with no keys
                        // TODO or return Err?
                        unreachable!();
                    },
                }
            } else {
                // we have some scalar keys, and maybe a text index after it.
                // for every scalar key, find comparisons from the query.
                let matching_ineqs = 
                    scalar_keys.iter().map(
                        |&(ref k,_)| {
                            match comps_ineq.get(k.as_str()) {
                                Some(a) => Some(a),
                                None => None,
                            }
                        }
                        ).collect::<Vec<_>>();
                let mut first_no_eqs = None;
                let mut matching_eqs = vec![];
                for (i, &(ref k, _)) in scalar_keys.iter().enumerate() {
                    match comps_eq.get(k.as_str()) {
                        Some(a) => {
                            // TODO clone
                            matching_eqs.push((*a).clone());
                        },
                        None => {
                            first_no_eqs = Some(i);
                            break;
                        },
                    }
                }

                match text_query {
                    &Some(ref text_query) => {
                        match first_no_eqs {
                            Some(_) => {
                                // if there is a text index, we need an EQ for every scalar key.
                                // so this won't work.
                                Ok(None)
                            },
                            None => {
                                // we have an EQ for every key.  this index will work.
                                // TODO clone
                                let bounds = QueryBounds::Text(matching_eqs, text_query.clone());
                                let plan = QueryPlan {
                                    // TODO clone
                                    ndx: ndx.clone(),
                                    bounds: bounds,
                                };
                                Ok(Some(plan))
                            },
                        }
                    },
                    &None => {
                        // there is no text query.  note that this might still be a text index,
                        // but at this point we don't care.  we are considering whether we can
                        // use the scalar keys to the left of the text index.

                        match first_no_eqs {
                            None => {
                                if matching_eqs.len() > 0 {
                                    let bounds = QueryBounds::EQ(matching_eqs);
                                    let plan = QueryPlan {
                                        // TODO clone
                                        ndx: ndx.clone(),
                                        bounds: bounds,
                                    };
                                    Ok(Some(plan))
                                } else {
                                    // we can't use this index at all
                                    Ok(None)
                                }
                            },
                            Some(num_eq) => {
                                match matching_ineqs[num_eq] {
                                    None | Some(&(None,None)) => {
                                        if num_eq>0 {
                                            let bounds = QueryBounds::EQ(matching_eqs);
                                            let plan = QueryPlan {
                                                // TODO clone
                                                ndx: ndx.clone(),
                                                bounds: bounds,
                                            };
                                            Ok(Some(plan))
                                        } else {
                                            // we can't use this index at all
                                            Ok(None)
                                        }
                                    },
                                    Some(&(Some(min),None)) => {
                                        let (op, v) = min;
                                        // TODO clone
                                        matching_eqs.push(v.clone());
                                        match op {
                                            OpGt::GT => {
                                                let bounds = QueryBounds::GT(matching_eqs);
                                                let plan = QueryPlan {
                                                    // TODO clone
                                                    ndx: ndx.clone(),
                                                    bounds: bounds,
                                                };
                                                Ok(Some(plan))
                                            },
                                            OpGt::GTE => {
                                                let bounds = QueryBounds::GTE(matching_eqs);
                                                let plan = QueryPlan {
                                                    // TODO clone
                                                    ndx: ndx.clone(),
                                                    bounds: bounds,
                                                };
                                                Ok(Some(plan))
                                            },
                                        }
                                    },
                                    Some(&(None,Some(max))) => {
                                        let (op, v) = max;
                                        // TODO clone
                                        matching_eqs.push(v.clone());
                                        match op {
                                            OpLt::LT => {
                                                let bounds = QueryBounds::LT(matching_eqs);
                                                let plan = QueryPlan {
                                                    // TODO clone
                                                    ndx: ndx.clone(),
                                                    bounds: bounds,
                                                };
                                                Ok(Some(plan))
                                            },
                                            OpLt::LTE => {
                                                let bounds = QueryBounds::LTE(matching_eqs);
                                                let plan = QueryPlan {
                                                    // TODO clone
                                                    ndx: ndx.clone(),
                                                    bounds: bounds,
                                                };
                                                Ok(Some(plan))
                                            },
                                        }
                                    },
                                    Some(&(Some(min),Some(max))) => {
                                        // this can probably only happen when the comps came
                                        // from an elemMatch
                                        let (op_gt, vmin) = min;
                                        let (op_lt, vmax) = max;

                                        // TODO clone disaster
                                        let mut minvals = matching_eqs.clone();
                                        minvals.push(vmin.clone());
                                        let mut maxvals = matching_eqs.clone();
                                        maxvals.push(vmax.clone());

                                        match (op_gt, op_lt) {
                                            (OpGt::GT, OpLt::LT) => {
                                                let bounds = QueryBounds::GT_LT(minvals, maxvals);
                                                let plan = QueryPlan {
                                                    // TODO clone
                                                    ndx: ndx.clone(),
                                                    bounds: bounds,
                                                };
                                                Ok(Some(plan))
                                            },
                                            (OpGt::GT, OpLt::LTE) => {
                                                let bounds = QueryBounds::GT_LTE(minvals, maxvals);
                                                let plan = QueryPlan {
                                                    // TODO clone
                                                    ndx: ndx.clone(),
                                                    bounds: bounds,
                                                };
                                                Ok(Some(plan))
                                            },
                                            (OpGt::GTE, OpLt::LT) => {
                                                let bounds = QueryBounds::GTE_LT(minvals, maxvals);
                                                let plan = QueryPlan {
                                                    // TODO clone
                                                    ndx: ndx.clone(),
                                                    bounds: bounds,
                                                };
                                                Ok(Some(plan))
                                            },
                                            (OpGt::GTE, OpLt::LTE) => {
                                                let bounds = QueryBounds::GTE_LTE(minvals, maxvals);
                                                let plan = QueryPlan {
                                                    // TODO clone
                                                    ndx: ndx.clone(),
                                                    bounds: bounds,
                                                };
                                                Ok(Some(plan))
                                            },
                                        }
                                    },
                                }
                            },
                        }
                    },
                }
            }
        }
    }

    fn parse_text_query(s: &Vec<char>) -> Result<Vec<TextQueryTerm>> {
        fn is_delim(c: char) -> bool {
            match c {
            ' ' => true,
            ',' => true,
            ';' => true,
            '.' => true,
            _ => false,
            }
        }

        let mut i = 0;
        let len = s.len();
        let mut a = vec![];
        loop {
            //printfn "get_token: %s" (s.Substring(!i))
            while i<len && is_delim(s[i]) {
                i = i + 1;
            }
            //printfn "after skip_delim: %s" (s.Substring(!i))
            if i >= len {
                break;
            } else {
                let neg =
                    if '-' == s[i] {
                        i = i + 1;
                        true
                    } else {
                        false
                    };

                // TODO do we allow space between the - and the word or phrase?

                if i >= len {
                    return Err(Error::Misc(String::from("negate of nothing")));
                }

                if '"' == s[i] {
                    let tok_start = i + 1;
                    //printfn "in phrase"
                    i = i + 1;
                    while i < len && s[i] != '"' {
                        i = i + 1;
                    }
                    //printfn "after look for other quote: %s" (s.Substring(!i))
                    let tok_len = 
                        if i < len { 
                            i - tok_start
                        } else {
                            return Err(Error::Misc(String::from("unmatched phrase quote")));
                        };
                    //printfn "phrase tok_len: %d" tok_len
                    i = i + 1;
                    // TODO need to get the individual words out of the phrase here?
                    // TODO what if the phrase is an empty string?  error?
                    if tok_len > 0 {
                        let sub = &s[tok_start .. tok_start + tok_len];
                        let s = sub.iter().cloned().collect::<String>();
                        let term = TextQueryTerm::Phrase(neg, s);
                        a.push(term);
                    } else {
                        // TODO isn't this always an error?
                        break;
                    }
                } else {
                    let tok_start = i;
                    while i < len && !is_delim(s[i]) {
                        i = i + 1;
                    }
                    let tok_len = i - tok_start;
                    if tok_len > 0 {
                        let sub = &s[tok_start .. tok_start + tok_len];
                        let s = sub.iter().cloned().collect::<String>();
                        let term = TextQueryTerm::Word(neg, s);
                        a.push(term);
                    } else {
                        // TODO isn't this always an error?
                        break;
                    }
                }
            }
        }

        let terms = a.into_iter().collect::<HashSet<_>>().into_iter().collect::<Vec<_>>();
        Ok(terms)
    }

    fn find_text_query(m: &matcher::QueryDoc) -> Result<Option<&str>> {
        let &matcher::QueryDoc::QueryDoc(ref items) = m;
        let mut a = 
            items
            .iter()
            .filter_map(|it| if let &matcher::QueryItem::Text(ref s) = it { Some(s.as_str()) } else { None })
            .collect::<Vec<_>>();
        if a.len() > 1 {
            Err(Error::Misc(String::from("only one $text in a query")))
        } else {
            let s = misc::remove_first_if_exists(&mut a);
            Ok(s)
        }
    }

    fn find_fit_indexes<'a>(indexes: &'a Vec<IndexInfo>, m: &matcher::QueryDoc) -> Result<(Vec<QueryPlan>, Option<Vec<TextQueryTerm>>)> {
        let text_query = if let Some(s) = try!(Self::find_text_query(m)) {
            let v = s.chars().collect::<Vec<char>>();
            Some(try!(Self::parse_text_query(&v)))
        } else {
            None
        };
        let comps_eq = try!(Self::find_compares_eq(m));
        let comps_ineq = try!(Self::find_compares_ineq(m));
        let mut fits = Vec::new();
        for ndx in indexes {
            if let Some(x) = try!(Self::fit_index_to_query(ndx, &comps_eq, &comps_ineq, &text_query)) {
                fits.push(x);
            }
        }
        Ok((fits, text_query))
    }

    fn choose_from_possibles(mut possibles: Vec<QueryPlan>) -> Option<QueryPlan> {
        if possibles.len() == 0 {
            None
        } else {
            // prefer the _id_ index if we can use it
            // TODO otherwise prefer any unique index
            // TODO otherwise prefer any EQ index
            // TODO or any index which has both min_max bounds
            // otherwise any index at all.  just take the first one.
            let mut winner = None;
            for plan in possibles {
                if winner.is_none() || plan.ndx.name == "_id_" {
                    winner = Some(plan);
                }
            }
            winner
        }
    }

    fn choose_index<'a>(indexes: &'a Vec<IndexInfo>, m: &matcher::QueryDoc, hint: Option<&IndexInfo>) -> Result<Option<QueryPlan>> {
        let (mut fits, text_query) = try!(Self::find_fit_indexes(indexes, m));
        match text_query {
            Some(_) => {
                // TODO if there is a $text query, disallow hint
                if fits.len() == 0 {
                    Err(Error::Misc(String::from("$text without index")))
                } else {
                    assert!(fits.len() == 1);
                    Ok(Some(fits.remove(0)))
                }
            },
            None => {
                // TODO the jstests seem to indicate that hint will be forced
                // even if it does not fit the query.  how does this work?
                // what bounds are used?

                match hint {
                    Some(hint) => {
                        panic!("TODO hint");
                    },
                    None => Ok(Self::choose_from_possibles(fits))
                }
            },
        }
    }

    fn find_index_for_min_max<'a>(indexes: &'a Vec<IndexInfo>, keys: &Vec<String>) -> Result<Option<&'a IndexInfo>> {
        for ndx in indexes {
            let (normspec, _) = try!(get_normalized_spec(ndx));
            let a = normspec.iter().map(|&(ref k,_)| k).collect::<Vec<_>>();
            if a.len() != keys.len() {
                continue;
            }
            // TODO this should just be a == *keys, or something similar
            let mut same = true;
            for i in 0 .. a.len() {
                if a[i] != keys[i].as_str() {
                    same = false;
                    break;
                }
            }
            if same {
                return Ok(Some(ndx));
            }
        }
        return Ok(None);
    }

    pub fn find(&self,
                db: &str,
                coll: &str,
                query: bson::Document,
                orderby: Option<bson::Value>,
                projection: Option<bson::Document>,
                min: Option<bson::Value>,
                max: Option<bson::Value>,
                hint: Option<bson::Value>,
                explain: Option<bson::Value>
                ) 
        -> Result<Box<Iterator<Item=Result<Row>> + 'static>>
    {
        let reader = try!(self.conn.begin_read());
        // TODO make the following filter DRY
        let indexes = try!(reader.list_indexes()).into_iter().filter(
            |ndx| ndx.db == db && ndx.coll == coll
            ).collect::<Vec<_>>();
        // TODO maybe we should get normalized index specs for all the indexes now.
        let m = try!(matcher::parse_query(query));
        let (natural, hint) = 
            match hint {
                Some(ref v) => {
                    if v.is_string() && try!(v.as_str()) == "$natural" {
                        (true, None)
                    } else {
                        if let Some(ndx) = Self::try_find_index_by_name_or_spec(&indexes, v) {
                            (false, Some(ndx))
                        } else {
                            return Err(Error::Misc(String::from("bad hint")));
                        }
                    }
                },
                None => (false, None),
            };
        let plan =
            // unless we're going to add comparisons to the query,
            // the bounds for min/max need to be precise, since the matcher isn't
            // going to help if they're not.  min is inclusive.  max is
            // exclusive.
            match (min, max) {
                (None, None) => {
                    if natural {
                        None
                    } else {
                        try!(Self::choose_index(&indexes, &m, hint))
                    }
                },
                (min, max) => {
                    // TODO if natural, then fail?
                    let pair =
                        match (min, max) {
                            (None, None) => {
                                // we handled this case above
                                unreachable!();
                            },
                            (Some(min), Some(max)) => {
                                panic!("TODO query bounds min and max");
                            },
                            (Some(min), None) => {
                                let min = try!(Self::parse_index_min_max(min));
                                let (keys, minvals): (Vec<_>, Vec<_>) = min.into_iter().unzip();
                                match try!(Self::find_index_for_min_max(&indexes, &keys)) {
                                    Some(ndx) => {
                                        let bounds = QueryBounds::GTE(minvals);
                                        (ndx, bounds)
                                    },
                                    None => {
                                        return Err(Error::Misc(String::from("index not found. TODO should be None?")));
                                    },
                                }
                            },
                            (None, Some(max)) => {
                                panic!("TODO query bounds max");
                            },
                        };

                    // TODO tests indicate that if there is a $min and/or $max as well as a $hint,
                    // then we need to error if they don't match each other.

                    let plan = QueryPlan {
                        // TODO clone
                        ndx: pair.0.clone(),
                        bounds: pair.1,
                    };
                    Some(plan)
                }
            };

        let mut seq: Box<Iterator<Item=Result<Row>>> = try!(reader.into_collection_reader(db, coll, plan));
        seq = box seq
            .filter(
                move |r| {
                    if let &Ok(ref d) = r {
                        matcher::match_query(&m, &d.doc)
                    } else {
                        // TODO so when we have an error we just let it through?
                        true
                    }
                }
        );
        match orderby {
            Some(orderby) => {
                let mut a = try!(seq.collect::<Result<Vec<_>>>());
                a.sort_by(cmp_row);
                seq = box a.into_iter().map(|d| Ok(d))
            },
            None => {
            },
        }
        // TODO projection
        Ok(seq)
    }
}

