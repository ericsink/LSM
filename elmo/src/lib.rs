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

extern crate misc;

use misc::endian;
use misc::bufndx;
use misc::varint;

extern crate bson;

#[derive(Debug)]
// TODO do we really want this public?
pub enum Error {
    // TODO remove Misc
    Misc(&'static str),

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
            Error::Misc(s) => write!(f, "Misc error: {}", s),
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
            Error::Misc(s) => s,
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
#[derive(Clone)]
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

pub enum TextQueryTerm {
    Word(bool, String),
    Phrase(bool, String),
}

pub enum QueryBounds {
    EQ(QueryKey),
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

// TODO the Item below should be a struct that contains both the bson::Value
// and also the score (and maybe pos).

// TODO should this be a seq of Document instead of bson::Value ?

pub trait StorageCollectionReader : Iterator<Item=Result<bson::Value>> {
    fn get_total_keys_examined(&self) -> u64;
    fn get_total_docs_examined(&self) -> u64;
    // TODO more explain stuff here?
}

pub trait StorageBase {
    // TODO maybe these two should return an iterator
    // TODO maybe these two should accept params to limit the rows returned
    fn list_collections(&self) -> Result<Vec<CollectionInfo>>;
    fn list_indexes(&self) -> Result<Vec<IndexInfo>>;

    fn get_collection_reader(&self, db: &str, coll: &str, plan: Option<QueryPlan>) -> Result<Box<StorageCollectionReader<Item=Result<bson::Value>> + 'static>>;
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
    fn into_collection_reader(self: Box<Self>, db: &str, coll: &str, plan: Option<QueryPlan>) -> Result<Box<StorageCollectionReader<Item=Result<bson::Value>> + 'static>>;
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
pub fn get_normalized_spec(info: &IndexInfo) -> Result<(Vec<(String,IndexType)>,Option<std::collections::HashMap<String,i32>>)> {
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
            let mut weights = std::collections::HashMap::new();
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

    pub fn insert(&self, db: &str, coll: &str, docs: &Vec<bson::Document>) -> Result<Vec<Result<()>>> {
        // TODO make sure every doc has an _id
        let mut results = Vec::new();
        {
            let writer = try!(self.conn.begin_write());
            {
                let mut collwriter = try!(writer.get_collection_writer(db, coll));
                for doc in docs {
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
        unimplemented!();
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
                                Err(Error::Misc("bad min max"))
                            },
                        }
                    } else {
                        Err(Error::Misc("bad min max"))
                    }
                },
                _ => {
                    Err(Error::Misc("bad min max"))
                },
            }
        ).collect::<Result<Vec<(_,_)>>>()
    }

    fn find_compares_eq(m: &matcher::QueryDoc) -> Result<std::collections::HashMap<&str, Vec<&bson::Value>>> {
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
        let mut mc = std::collections::HashMap::new();
        for (k,v) in comps {
            let a = match mc.entry(k) {
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert(vec![])
                },
                std::collections::hash_map::Entry::Occupied(e) => {
                    e.into_mut()
                },
            };
            a.push(v);
        }

        // query for x=3 && x=4 is legit in mongo.
        // it can match a doc where x is an array that contains both 3 and 4
        // {x:[1,2,3,4,5]}
        // in terms of choosing an index to use, we can pick either one.
        // the index will give us, for example, "all documents where x is 3",
        // which will include the one above, and the matcher will then also
        // make sure that the 4 is there as well.

        for (ref k, ref mut v) in mc.iter_mut() {
            if v.len() > 1 {
                let distinct = {
                    let uniq : std::collections::HashSet<_> = v.iter().collect();
                    uniq.len()
                };
                if distinct > 1 {
                    return Err(Error::Misc("conflict $eq"));
                } else {
                    v.truncate(1);
                }
            }
        }

        Ok(mc)
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
        -> Result<Box<StorageCollectionReader<Item=Result<bson::Value>> + 'static>>
    {
        let reader = try!(self.conn.begin_read());
        // TODO make the following filter DRY
        let indexes = try!(reader.list_indexes()).into_iter().filter(
            |ndx| ndx.db == db && ndx.coll == coll
            ).collect::<Vec<_>>();
        // TODO maybe we should get normalized index specs for all the indexes now.
        let m = matcher::parse_query(query);
        let (natural, hint) = 
            match hint {
                Some(ref v) => {
                    if v.is_string() && try!(v.as_str()) == "$natural" {
                        (true, None)
                    } else {
                        if let Some(ndx) = Self::try_find_index_by_name_or_spec(&indexes, v) {
                            (false, Some(ndx))
                        } else {
                            return Err(Error::Misc("bad hint"));
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
                        // TODO chooseIndex indexes m hint
                        unimplemented!();
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
                                unimplemented!();
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
                                        return Err(Error::Misc("index not found. TODO should be None?"));
                                    },
                                }
                            },
                            (None, Some(max)) => {
                                unimplemented!();
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

        let coll_reader = try!(reader.into_collection_reader(db, coll, plan));
        Ok(coll_reader)
    }
}

