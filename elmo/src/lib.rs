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

#[derive(Debug)]
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

#[derive(Debug)]
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
#[derive(Debug)]
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

#[derive(Debug)]
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
        &bson::Value::BString(ref s) => 
            if s == "2d" { 
                IndexType::Geo2d 
            } else if s == "text" {
                panic!("decode_index_type: text")
            } else {
                panic!("decode_index_type: unknown type")
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
            //println!("scalar_keys: {:?}", scalar_keys);
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

// TODO this type was created so that all the projection operations
// could be done in the order they appeared, which we are not really
// doing.  So now the parser is constructing these values and then
// we unconstruct them later.  Messy.
#[derive(Debug)]
enum AggProj {
    Include,
    Expr(Expr),
}

impl AggProj {
    fn is_include(&self) -> bool {
        match self {
            &AggProj::Include => true,
            &AggProj::Expr(_) => false,
        }
    }
}

#[derive(Debug)]
enum GroupAccum {
    Sum(Expr),
    Avg(Expr),
    First(Expr),
    Last(Expr),
    Max(Expr),
    Min(Expr),
    Push(Expr),
    AddToSet(Expr),
}

#[derive(Debug)]
enum AggOp {
    Skip(i32),
    Limit(i32),
    Sort(bson::Value),
    Out(String),
    Unwind(String),
    Match(matcher::QueryDoc),
    Project(Vec<(String,AggProj)>),
    Group(bson::Value, Vec<(String, GroupAccum)>),
    GeoNear(bson::Value),
    Redact(Expr),
}

#[derive(Debug)]
enum Expr {
    Var(String),
    Literal(bson::Value),

    AllElementsTrue(Box<Expr>),
    AnyElementTrue(Box<Expr>),
    DayOfMonth(Box<Expr>),
    DayOfWeek(Box<Expr>),
    DayOfYear(Box<Expr>),
    Hour(Box<Expr>),
    Millisecond(Box<Expr>),
    Minute(Box<Expr>),
    Month(Box<Expr>),
    Not(Box<Expr>),
    Second(Box<Expr>),
    Size(Box<Expr>),
    ToLower(Box<Expr>),
    ToUpper(Box<Expr>),
    Week(Box<Expr>),
    Year(Box<Expr>),

    Cmp(Box<(Expr, Expr)>),
    Eq(Box<(Expr, Expr)>),
    Ne(Box<(Expr, Expr)>),
    Gt(Box<(Expr, Expr)>),
    Lt(Box<(Expr, Expr)>),
    Gte(Box<(Expr, Expr)>),
    Lte(Box<(Expr, Expr)>),
    Subtract(Box<(Expr, Expr)>),
    Divide(Box<(Expr, Expr)>),
    Mod(Box<(Expr, Expr)>),
    IfNull(Box<(Expr, Expr)>),
    SetDifference(Box<(Expr, Expr)>),
    SetIsSubset(Box<(Expr, Expr)>),
    StrCaseCmp(Box<(Expr, Expr)>),

    Substr(Box<(Expr, Expr, Expr)>),
    Cond(Box<(Expr, Expr, Expr)>),

    And(Vec<Expr>),
    Or(Vec<Expr>),
    Add(Vec<Expr>),
    Multiply(Vec<Expr>),
    Concat(Vec<Expr>),
    SetEquals(Vec<Expr>),
    SetIntersection(Vec<Expr>),
    SetUnion(Vec<Expr>),

    Let(Vec<(String, Expr)>, Box<Expr>),
    Map(Box<Expr>, String, Box<Expr>),
    DateToString(String, Box<Expr>),

    // TODO meta
}

impl Connection {
    pub fn new(conn: Box<StorageConnection>) -> Connection {
        Connection {
            conn: conn,
        }
    }

    fn fix_positional(s: &str, pos: Option<usize>) -> String {
        match pos {
            None => String::from(s),
            Some(i) => s.replace(".$", &format!(".{}", i)),
        }
    }

    // TODO maybe this could/should take ownership of ops?
    fn apply_update_ops(doc: &mut bson::Document, ops: &Vec<UpdateOp>, is_upsert: bool, pos: Option<usize>) -> Result<usize> {
        let mut count = 0;
        for op in ops {
            match op {
                &UpdateOp::Min(ref path, ref v) => {
                    let path = Self::fix_positional(path, pos);
                    match try!(doc.entry(&path)) {
                        bson::Entry::Found(e) => {
                            let c = matcher::cmp(v, e.get());
                            if c == Ordering::Less {
                                e.replace(v.clone());
                                count = count + 1;
                            }
                        },
                        bson::Entry::Absent(e) => {
                            // when the key isn't found, this works like $set
                            e.insert(v.clone());
                            count = count + 1;
                        },
                    }
                },
                &UpdateOp::Max(ref path, ref v) => {
                    let path = Self::fix_positional(path, pos);
                    match try!(doc.entry(&path)) {
                        bson::Entry::Found(e) => {
                            let c = matcher::cmp(v, e.get());
                            if c == Ordering::Greater {
                                e.replace(v.clone());
                                count = count + 1;
                            }
                        },
                        bson::Entry::Absent(e) => {
                            // when the key isn't found, this works like $set
                            e.insert(v.clone());
                            count = count + 1;
                        },
                    }
                },
                &UpdateOp::Inc(ref path, ref v) => {
                    let path = Self::fix_positional(path, pos);
                    if !v.is_numeric() {
                        return Err(Error::Misc(format!("argument to $inc must be numeric")));
                    }
                    match try!(doc.entry(&path)) {
                        bson::Entry::Found(mut e) => {
                            if try!(v.numeric_to_i64()) != 0 {
                                match e.get_mut() {
                                    &mut bson::Value::BInt32(ref mut n) => {
                                        *n = *n + try!(v.numeric_to_i32())
                                    },
                                    &mut bson::Value::BInt64(ref mut n) => {
                                        *n = *n + try!(v.numeric_to_i64())
                                    },
                                    &mut bson::Value::BDouble(ref mut n) => {
                                        *n = *n + try!(v.numeric_to_f64())
                                    },
                                    _ => return Err(Error::Misc(format!("can't $inc to this type"))),
                                }
                                count = count + 1;
                            }
                        },
                        bson::Entry::Absent(e) => {
                            // when the key isn't found, this works like $set
                            e.insert(v.clone());
                            count = count + 1;
                        },
                    }
                },
                &UpdateOp::Mul(ref path, ref v) => {
                    let path = Self::fix_positional(path, pos);
                    if !v.is_numeric() {
                        return Err(Error::Misc(format!("argument to $mul must be numeric")));
                    }
                    match try!(doc.entry(&path)) {
                        bson::Entry::Found(mut e) => {
                            match e.get_mut() {
                                &mut bson::Value::BInt32(ref mut n) => {
                                    *n = *n * try!(v.numeric_to_i32())
                                },
                                &mut bson::Value::BInt64(ref mut n) => {
                                    *n = *n * try!(v.numeric_to_i64())
                                },
                                &mut bson::Value::BDouble(ref mut n) => {
                                    *n = *n * try!(v.numeric_to_f64())
                                },
                                _ => return Err(Error::Misc(format!("can't $mul to this type"))),
                            }
                            count = count + 1;
                        },
                        bson::Entry::Absent(e) => {
                            // when the key isn't found, this works like $set
                            e.insert(v.clone());
                            count = count + 1;
                        },
                    }
                },
                &UpdateOp::Set(ref path, ref v) => {
                    let path = Self::fix_positional(path, pos);
                    try!(doc.set_path(&path, v.clone()));
                    // TODO this is an example of a place where we increment the counter
                    // but we don't actually know if the document changed, since we might
                    // have set the same value as was already there.
                    count = count + 1;
                },
                &UpdateOp::PullValue(ref path, ref v) => {
                    panic!("TODO UpdateOp::PullValue");
                },
                &UpdateOp::SetOnInsert(ref path, ref v) => {
                    panic!("TODO UpdateOp::SetOnInsert");
                },
                &UpdateOp::BitAnd(ref path, v) => {
                    let path = Self::fix_positional(path, pos);
                    match try!(doc.entry(&path)) {
                        bson::Entry::Found(mut e) => {
                            match e.get_mut() {
                                &mut bson::Value::BInt32(ref mut n) => {
                                    *n = *n & (v as i32)
                                },
                                &mut bson::Value::BInt64(ref mut n) => {
                                    *n = *n & v
                                },
                                _ => return Err(Error::Misc(format!("can't $bit.and to this type"))),
                            }
                            count = count + 1;
                        },
                        bson::Entry::Absent(e) => {
                            return Err(Error::Misc(format!("$bit.and path not found")));
                        },
                    }
                },
                &UpdateOp::BitOr(ref path, v) => {
                    let path = Self::fix_positional(path, pos);
                    match try!(doc.entry(&path)) {
                        bson::Entry::Found(mut e) => {
                            match e.get_mut() {
                                &mut bson::Value::BInt32(ref mut n) => {
                                    *n = *n | (v as i32)
                                },
                                &mut bson::Value::BInt64(ref mut n) => {
                                    *n = *n | v
                                },
                                _ => return Err(Error::Misc(format!("can't $bit.or to this type"))),
                            }
                            count = count + 1;
                        },
                        bson::Entry::Absent(e) => {
                            return Err(Error::Misc(format!("$bit.or path not found")));
                        },
                    }
                },
                &UpdateOp::BitXor(ref path, v) => {
                    let path = Self::fix_positional(path, pos);
                    match try!(doc.entry(&path)) {
                        bson::Entry::Found(mut e) => {
                            match e.get_mut() {
                                &mut bson::Value::BInt32(ref mut n) => {
                                    *n = *n ^ (v as i32)
                                },
                                &mut bson::Value::BInt64(ref mut n) => {
                                    *n = *n ^ v
                                },
                                _ => return Err(Error::Misc(format!("can't $bit.xor to this type"))),
                            }
                            count = count + 1;
                        },
                        bson::Entry::Absent(e) => {
                            return Err(Error::Misc(format!("$bit.xor path not found")));
                        },
                    }
                },
                &UpdateOp::Unset(ref path) => {
                    let path = Self::fix_positional(path, pos);
                    match try!(doc.entry(&path)) {
                        bson::Entry::Found(e) => {
                            e.remove();
                            count = count + 1;
                        },
                        bson::Entry::Absent(e) => {
                        },
                    }
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
        // TODO not sure if the housekeeping necessary to return an accurate
        // change count is worth the trouble.
        Ok(count)
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
                "$max" => {
                    for (path, v) in try!(v.into_document()).pairs {
                        result.push(UpdateOp::Max(path, v));
                    }
                },
                "$inc" => {
                    for (path, v) in try!(v.into_document()).pairs {
                        result.push(UpdateOp::Inc(path, v));
                    }
                },
                "$mul" => {
                    for (path, v) in try!(v.into_document()).pairs {
                        result.push(UpdateOp::Mul(path, v));
                    }
                },
                "$set" => {
                    for (path, v) in try!(v.into_document()).pairs {
                        result.push(UpdateOp::Set(path, v));
                    }
                },
                "$unset" => {
                    for (path, _) in try!(v.into_document()).pairs {
                        result.push(UpdateOp::Unset(path));
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
        //println!("indexes: {:?}", indexes);
        let plan = try!(Self::choose_index(&indexes, &m, None));
        //println!("plan: {:?}", plan);
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

    fn build_upsert_with_update_operators(m: &matcher::QueryDoc, ops: &Vec<UpdateOp>) -> Result<bson::Document> {
        let a = matcher::get_eqs(m);
        let mut doc = bson::Document::new_empty();
        for (path, v) in a {
            try!(doc.set_path(&path, v.clone()));
        }
        // TODO save the id so we can make sure it didn't change
        let count_changes = try!(Self::apply_update_ops(&mut doc, ops, true, None));
        // TODO make sure the id didn't change
        doc.ensure_id();
        Ok(doc)
    }

    fn build_simple_upsert(id_q: Option<bson::Value>, u: &mut bson::Document) -> Result<()> {
        // TODO I hate this code.  I want to match on u.get("_id"), but the
        // borrow survives the whole match, which means I can't modify it
        // in the None case.  This approach requires me to re-do the get()
        // just below, with an unwrap().  Ugly.  Use Entry?
        if u.get("_id").is_some() {
            match id_q {
                Some(id_q) => {
                    if id_q != *u.get("_id").unwrap() {
                        Err(Error::Misc(String::from("can't change _id")))
                    } else {
                        Ok(())
                    }
                },
                None => {
                    Ok(())
                },
            }
        } else {
            match id_q {
                Some(id_q) => {
                    u.set("_id", id_q);
                    Ok(())
                },
                None => {
                    u.set_objectid("_id", misc::new_bson_objectid_rand());
                    Ok(())
                },
            }
        }
    }

    fn validate_for_storage(d: &mut bson::Document) -> Result<()> {
        try!(d.validate_id());
        try!(d.validate_keys(0));
        // TODO validate depth
        // TODO return id?
        Ok(())
    }

    // TODO this func needs to return the 4-tuple
    // (count_matches, count_modified, Option<TODO>, Option<TODO>)
    pub fn update(&self, db: &str, coll: &str, updates: &mut Vec<bson::Document>) -> Result<Vec<Result<()>>> {
        //println!("in update: {:?}", updates);
        // TODO need separate conn?
        let mut results = Vec::new();
        {
            let writer = try!(self.conn.begin_write());
            {
                let mut collwriter = try!(writer.get_collection_writer(db, coll));
                // TODO why does this closure need to be mut?
                let mut one_update_or_upsert = |upd: &mut bson::Document| -> Result<()> {
                    //println!("in closure: {:?}", upd);
                    let q = try!(upd.must_remove_document("q"));
                    let mut u = try!(upd.must_remove_document("u"));
                    let multi = try!(upd.must_remove_bool("multi"));
                    let upsert = try!(upd.must_remove_bool("upsert"));
                    // rescue _id from q if possible
                    let q_id =
                        match q.get("_id") {
                            Some(id) => Some(id.clone()),
                            None => None,
                        };
                    let m = try!(matcher::parse_query(q));
                    let has_update_operators = u.pairs.iter().any(|&(ref k, _)| k.starts_with("$"));
                    if has_update_operators {
                        let ops = try!(Self::parse_update_doc(u));
                        //println!("ops: {:?}", ops);
                        let (count_matches, count_modified) =
                            if multi {
                                panic!("TODO update operators multi");
                            } else {
                                match try!(Self::get_one_match(db, coll, &*writer, &m)) {
                                    Some(row) => {
                                        //println!("row found for update: {:?}", row);
                                        let mut doc = try!(row.doc.into_document());
                                        let count_changes = try!(Self::apply_update_ops(&mut doc, &ops, false, None));
                                        // TODO make sure _id did not change
                                        // TODO only do the actual update if a change happened.  clone and compare?
                                        try!(Self::validate_for_storage(&mut doc));
                                        // TODO error in the following line?
                                        collwriter.update(&doc);
                                        // TODO return (1, 0) if the change didn't happen
                                        (1, 1)
                                    },
                                    None => {
                                        //println!("get_one_match found nothing");
                                        (0, 0)
                                    },
                                }
                            };
                        if count_matches == 0 {
                            if upsert {
                                let mut doc = try!(Self::build_upsert_with_update_operators(&m, &ops));
                                try!(Self::validate_for_storage(&mut doc));
                                // TODO handle error in following line
                                collwriter.insert(&doc);
                                // TODO return something
                                Ok(())
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
                                try!(Self::validate_for_storage(&mut u));
                                // TODO handle error in following line
                                collwriter.update(&u);
                                // TODO return something
                                Ok(())
                            },
                            None => {
                                if upsert {
                                    try!(Self::build_simple_upsert(q_id, &mut u));
                                    try!(Self::validate_for_storage(&mut u));
                                    // TODO handle error in following line
                                    collwriter.insert(&u);
                                    // TODO return something
                                    Ok(())
                                } else {
                                    // TODO (0,0,None,None)
                                    //panic!("TODO nothing updated");
                                    Ok(())
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
            d.ensure_id();
        }
        let mut results = Vec::new();
        {
            let writer = try!(self.conn.begin_write());
            {
                let mut collwriter = try!(writer.get_collection_writer(db, coll));
                for mut doc in docs {
                    try!(Self::validate_for_storage(&mut doc));
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
        let mut count_deleted = 0;
        for ndx in indexes {
            if try!(writer.drop_index(&ndx.db, &ndx.coll, &ndx.name)) {
                count_deleted = count_deleted + 1;
            }
        }
        try!(writer.commit());
        Ok((count_before, count_deleted))
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
                        match fits.iter().position(|plan| plan.ndx.spec == hint.spec) {
                            Some(i) => {
                                Ok(Some(fits.remove(i)))
                            },
                            None => Ok(Self::choose_from_possibles(fits))
                        }
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

    fn parse_expr(v: bson::Value) -> Result<Expr> {
        let get_one_arg = |mut v: bson::Value| -> Result<Expr> {
            match v {
                bson::Value::BArray(mut a) => {
                    if a.len() != 1 {
                        Err(Error::Misc(String::from("16020 wrong number of args")))
                    } else {
                        Self::parse_expr(a.items.remove(0))
                    }
                },
                _ => Self::parse_expr(v),
            }
        };

        let get_two_args = |mut v: bson::Value| -> Result<(Expr,Expr)> {
            match v {
                bson::Value::BArray(mut a) => {
                    if a.len() != 2 {
                        Err(Error::Misc(String::from("16020 wrong number of args")))
                    } else {
                        let v1 = a.items.remove(1);
                        let v0 = a.items.remove(0);
                        let e0 = try!(Self::parse_expr(v0));
                        let e1 = try!(Self::parse_expr(v1));
                        Ok((e0, e1))
                    }
                },
                _ => Err(Error::Misc(String::from("16020 wrong number of args")))
            }
        };

        let get_three_args = |mut v: bson::Value| -> Result<(Expr,Expr,Expr)> {
            match v {
                bson::Value::BArray(mut a) => {
                    if a.len() != 3 {
                        Err(Error::Misc(String::from("16020 wrong number of args")))
                    } else {
                        let v2 = a.items.remove(2);
                        let v1 = a.items.remove(1);
                        let v0 = a.items.remove(0);
                        let e0 = try!(Self::parse_expr(v0));
                        let e1 = try!(Self::parse_expr(v1));
                        let e2 = try!(Self::parse_expr(v2));
                        Ok((e0, e1, e2))
                    }
                },
                _ => Err(Error::Misc(String::from("16020 wrong number of args")))
            }
        };

        let parse_vec = |mut v: bson::Value| -> Result<Vec<Expr>> {
            let a = try!(v.into_array());
            a.items.into_iter().map(|v| Self::parse_expr(v)).collect::<Result<Vec<_>>>()
        };

        match v {
            bson::Value::BString(s) => {
                if s.starts_with("$$") {
                    Ok(Expr::Var(String::from(&s[2 ..])))
                } else if s.starts_with("$") {
                    Ok(Expr::Var(format!("CURRENT.{}", &s[1 ..])))
                } else {
                    Ok(Expr::Literal(bson::Value::BString(s)))
                }
            },
            bson::Value::BDocument(mut bd) => {
                if bd.pairs.len() == 1 {
                    let (k, v) = bd.pairs.remove(0);
                    if k.starts_with("$") {
                        match k.as_str() {
                            "$allElementsTrue" => {
                                Ok(Expr::AllElementsTrue(box try!(get_one_arg(v))))
                            },
                            "$anyElementTrue" => {
                                Ok(Expr::AnyElementTrue(box try!(get_one_arg(v))))
                            },
                            "$dayOfMonth" => {
                                Ok(Expr::DayOfMonth(box try!(get_one_arg(v))))
                            },
                            "$dayOfWeek" => {
                                Ok(Expr::DayOfWeek(box try!(get_one_arg(v))))
                            },
                            "$dayOfYear" => {
                                Ok(Expr::DayOfYear(box try!(get_one_arg(v))))
                            },
                            "$hour" => {
                                Ok(Expr::Hour(box try!(get_one_arg(v))))
                            },
                            "$millisecond" => {
                                Ok(Expr::Millisecond(box try!(get_one_arg(v))))
                            },
                            "$minute" => {
                                Ok(Expr::Minute(box try!(get_one_arg(v))))
                            },
                            "$month" => {
                                Ok(Expr::Month(box try!(get_one_arg(v))))
                            },
                            "$not" => {
                                Ok(Expr::Not(box try!(get_one_arg(v))))
                            },
                            "$second" => {
                                Ok(Expr::Second(box try!(get_one_arg(v))))
                            },
                            "$size" => {
                                Ok(Expr::Size(box try!(get_one_arg(v))))
                            },
                            "$toLower" => {
                                Ok(Expr::ToLower(box try!(get_one_arg(v))))
                            },
                            "$toUpper" => {
                                Ok(Expr::ToUpper(box try!(get_one_arg(v))))
                            },
                            "$week" => {
                                Ok(Expr::Week(box try!(get_one_arg(v))))
                            },
                            "$year" => {
                                Ok(Expr::Year(box try!(get_one_arg(v))))
                            },

                            "$cmp" => {
                                Ok(Expr::Cmp(box try!(get_two_args(v))))
                            },
                            "$eq" => {
                                Ok(Expr::Eq(box try!(get_two_args(v))))
                            },
                            "$ne" => {
                                Ok(Expr::Ne(box try!(get_two_args(v))))
                            },
                            "$gt" => {
                                Ok(Expr::Gt(box try!(get_two_args(v))))
                            },
                            "$lt" => {
                                Ok(Expr::Lt(box try!(get_two_args(v))))
                            },
                            "$gte" => {
                                Ok(Expr::Gte(box try!(get_two_args(v))))
                            },
                            "$lte" => {
                                Ok(Expr::Lte(box try!(get_two_args(v))))
                            },
                            "$subtract" => {
                                Ok(Expr::Subtract(box try!(get_two_args(v))))
                            },
                            "$divide" => {
                                Ok(Expr::Divide(box try!(get_two_args(v))))
                            },
                            "$mod" => {
                                Ok(Expr::Mod(box try!(get_two_args(v))))
                            },
                            "$ifNull" => {
                                Ok(Expr::IfNull(box try!(get_two_args(v))))
                            },
                            "$setDifference" => {
                                Ok(Expr::SetDifference(box try!(get_two_args(v))))
                            },
                            "$setIsSubset" => {
                                Ok(Expr::SetIsSubset(box try!(get_two_args(v))))
                            },
                            "$strcasecmp" => {
                                Ok(Expr::StrCaseCmp(box try!(get_two_args(v))))
                            },

                            "$substr" => {
                                Ok(Expr::Substr(box try!(get_three_args(v))))
                            },

                            "$cond" => {
                                if v.is_array() {
                                    Ok(Expr::Substr(box try!(get_three_args(v))))
                                } else if v.is_document() {
                                    Err(Error::Misc(format!("TODO $cond document: {:?}", v)))
                                } else {
                                    Err(Error::Misc(String::from("16020 wrong number of args")))
                                }
                            },

                            "$and" => {
                                Ok(Expr::And(try!(parse_vec(v))))
                            },
                            "$or" => {
                                Ok(Expr::Or(try!(parse_vec(v))))
                            },
                            "$add" => {
                                Ok(Expr::Add(try!(parse_vec(v))))
                            },
                            "$multiply" => {
                                Ok(Expr::Multiply(try!(parse_vec(v))))
                            },
                            "$concat" => {
                                Ok(Expr::Concat(try!(parse_vec(v))))
                            },
                            "$setEquals" => {
                                Ok(Expr::SetEquals(try!(parse_vec(v))))
                            },
                            "$setIntersection" => {
                                Ok(Expr::SetIntersection(try!(parse_vec(v))))
                            },
                            "$setUnion" => {
                                Ok(Expr::SetUnion(try!(parse_vec(v))))
                            },

                            // TODO let
                            // TODO map
                            // TODO dateToString

                            _ => {
                                Err(Error::Misc(format!("invalid expression operator: {}", k)))
                            },
                        }
                    } else {
                        Err(Error::Misc(String::from("TODO no match key")))
                    }
                } else {
                    // TODO any cases where this is not a literal need to have
                    // been handled before this point.
                    Ok(Expr::Literal(bson::Value::BDocument(bd)))
                }
            },
            _ => {
                // TODO any cases where this is not a literal need to have
                // been handled before this point.
                Ok(Expr::Literal(v))
            },
        }
    }

    fn eval(ctx: &bson::Document, e: &Expr) -> Result<bson::Value> {
        match e {
            &Expr::Literal(ref v) => Ok(v.clone()),
            _ => Err(Error::Misc(format!("TODO eval: {:?}", e)))
        }
    }

    fn parse_agg(a: bson::Array) -> Result<Vec<AggOp>> {
        fn flatten_projection(d: bson::Value) -> Result<Vec<(String, bson::Value)>> {
            fn flatten(a: &mut Vec<(String, bson::Value)>, path: String, d: bson::Value) -> Result<()> {
                match d {
                    bson::Value::BDocument(bd) => {
                        if  bd.pairs.iter().any(|&(ref k, _)| k.starts_with("$")) {
                            if path.as_str() == "" {
                                return Err(Error::Misc(String::from("16404 $project key begins with $")))
                            } else {
                                a.push((path, bson::Value::BDocument(bd)));
                            }
                        } else {
                            for (k,v) in bd.pairs {
                                let new_path =
                                    if path.as_str() == "" {
                                        String::from(k)
                                    } else {
                                        format!("{}.{}", path, k)
                                    };
                                try!(flatten(a, new_path, v));
                            }
                        }
                    },
                    _ => {
                        a.push((path, d));
                    },
                }
                Ok(())
            }
            let mut a = vec![];
            try!(flatten(&mut a, String::from(""), d));
            Ok(a)
        }

        a.items.into_iter().map(
            |d| {
                let mut d = try!(d.into_document());
                if d.pairs.len() != 1 {
                    Err(Error::Misc(String::from("agg pipeline stage spec must have one item in it")))
                } else {
                    let (k, v) = d.pairs.pop().expect("just checked this");
                    match k.as_str() {
                        "$limit" => {
                            Ok(AggOp::Limit(try!(v.numeric_to_i32())))
                        },
                        "$skip" => {
                            Ok(AggOp::Skip(try!(v.numeric_to_i32())))
                        },
                        "$sort" => {
                            Ok(AggOp::Sort(v))
                        },
                        "$out" => {
                            Ok(AggOp::Out(try!(v.into_string())))
                        },
                        "$unwind" => {
                            Ok(AggOp::Unwind(try!(v.into_string())))
                        },
                        "$match" => {
                            let v = try!(v.into_document());
                            let m = try!(matcher::parse_query(v));
                            // TODO disallow $where
                            // TODO disallow $near
                            Ok(AggOp::Match(m))
                        },
                        "$project" => {
                            // flatten so that:
                            // project b:{a:1} should be an inclusion of b.a, not {a:1} as a doc literal for b
                            let args = try!(flatten_projection(v));
                            // TODO is this $ check needed here again?  It's also done in flatten_projection().
                            if args.iter().any(|&(ref k, _)| k.starts_with("$")) {
                                return Err(Error::Misc(String::from("16404 $project key begins with $")))
                            }
                            let (mut id, not_id): (Vec<_>, Vec<_>) = args.into_iter().partition(|&(ref k, _)| k=="_id");
                            if id.len() > 1 {
                                return Err(Error::Misc(String::from("only one id allowed here")))
                            }
                            let id = id.pop();
                            let id_item =
                                match id {
                                    None => Some((String::from("_id"), AggProj::Include)),
                                    Some((_,id)) => {
                                        match id {
                                            bson::Value::BInt32(0) 
                                            | bson::Value::BInt64(0) 
                                            | bson::Value::BDouble(0.0)
                                            | bson::Value::BBoolean(false) => None,
                                            _ => Some((String::from("_id"), AggProj::Expr(try!(Self::parse_expr(id))))),
                                        }
                                    },
                                };
                            let expressions =
                                not_id.into_iter().map(
                                    |(k,v)| match v {
                                        bson::Value::BInt32(1) 
                                        | bson::Value::BInt64(1) 
                                        | bson::Value::BDouble(1.0)
                                        | bson::Value::BBoolean(true) => Ok((k, AggProj::Include)),
                                        _ => Ok((k, AggProj::Expr(try!(Self::parse_expr(v))))),
                                    }
                                    ).collect::<Result<Vec<_>>>();
                            let mut expressions = try!(expressions);
                            match id_item {
                                Some(id) => {
                                    expressions.insert(0, id);
                                },
                                None => {
                                },
                            }
                            Ok(AggOp::Project(expressions))
                        },
                        "$group" => {
                            Err(Error::Misc(format!("agg pipeline TODO: {}", k)))
                        },
                        "$redact" => {
                            Err(Error::Misc(format!("agg pipeline TODO: {}", k)))
                        },
                        "$geoNear" => {
                            Err(Error::Misc(format!("agg pipeline TODO: {}", k)))
                        },
                        _ => Err(Error::Misc(format!("16435 invalid agg pipeline stage name: {}", k)))
                    }
                }
            }).collect::<Result<Vec<AggOp>>>()
    }

    fn agg_project(seq: Box<Iterator<Item=Result<Row>>>, expressions: Vec<(String,AggProj)>) -> Box<Iterator<Item=Result<Row>>> {
        box seq.map(
            move |rr| {
                match rr {
                    Ok(mut row) => {
                        let (includes, exes): (Vec<_>, Vec<_>) = expressions.iter().partition(|&&(ref s, ref a)| a.is_include());
                        let exes = exes.into_iter().map(|&(ref path, ref a)| 
                                                        match a {
                                                            &AggProj::Expr(ref e) => {
                                                                (path, e)
                                                            },
                                                            &AggProj::Include => {
                                                                unreachable!();
                                                            },
                                                        }
                                                       ).collect::<Vec<_>>();
                        let mut d = row.doc;
                        // TODO process the includes against d
                        // TODO ctx, move d into it as CURRENT, and a clone as ROOT.
                        let mut ctx = bson::Document::new_empty();
                        for (ref path, ref e) in exes {
                            let v = try!(Self::eval(&ctx, e));
                            // TODO this should modify CURRENT, not d
                            match try!(d.entry(path)) {
                                bson::Entry::Found(e) => {
                                    return Err(Error::Misc(format!("16400 already: {}", path)))
                                },
                                bson::Entry::Absent(e) => {
                                    if !v.is_undefined() {
                                        e.insert(v);
                                    }
                                },
                            }
                        }
                        row.doc = d;
                        Ok(row)
                    },
                    Err(e) => Err(e),
                }
            }
            )
    }

    pub fn aggregate(&self,
                db: &str,
                coll: &str,
                pipeline: bson::Array
                ) 
        -> Result<(Option<String>, Box<Iterator<Item=Result<Row>> + 'static>)>
    {
        let ops = try!(Self::parse_agg(pipeline));
        //Err(Error::Misc(format!("agg pipeline TODO: {:?}", ops)))
        // TODO check for plan
        let plan = None;
        let reader = try!(self.conn.begin_read());
        let mut seq: Box<Iterator<Item=Result<Row>>> = try!(reader.into_collection_reader(db, coll, plan));
        for op in ops {
            match op {
                AggOp::Skip(n) => {
                    seq = box seq.skip(n as usize);
                },
                AggOp::Limit(n) => {
                    seq = box seq.take(n as usize);
                },
                AggOp::Match(m) => {
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
                },
                AggOp::Sort(k) => {
                    let mut a = try!(seq.collect::<Result<Vec<_>>>());
                    a.sort_by(cmp_row);
                    seq = box a.into_iter().map(|d| Ok(d));
                },
                AggOp::Project(expressions) => {
                    seq = box Self::agg_project(seq, expressions);
                },
                _ => {
                    //return Err(Error::Misc(format!("agg pipeline TODO: {:?}", ops)))
                },
            }
        }
        Ok((None, seq))
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

