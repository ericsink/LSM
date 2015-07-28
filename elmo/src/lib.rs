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

#[derive(Debug)]
// TODO do we really want this public?
pub enum Error {
    // TODO remove Misc
    Misc(&'static str),

    // TODO more detail within CorruptFile
    CorruptFile(&'static str),

    Bson(bson::BsonError),
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

impl From<bson::BsonError> for Error {
    fn from(err: bson::BsonError) -> Error {
        Error::Bson(err)
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
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

pub struct IndexInfo {
    pub db: String,
    pub coll: String,
    pub name: String,
    pub spec: BsonValue,
    pub options: BsonValue,
}

pub type QueryKey = Vec<BsonValue>;

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

pub trait StorageReader : Iterator<Item=Result<BsonValue>> {
    fn get_total_keys_examined(&self) -> u64;
    // TODO more stuff here
}

pub trait StorageCollectionWriter {
    fn insert(&mut self, v: BsonValue) -> Result<()>;
    fn update(&mut self, v: BsonValue) -> Result<()>;
    fn delete(&mut self, v: BsonValue) -> Result<bool>;
    // TODO getIndexes
}

// TODO should implement Drop = rollback

pub trait StorageWriter {
    fn create_collection(&self, db: &str, coll: &str, options: BsonValue) -> Result<bool>;
    fn rename_collection(&self, old_name: &str, new_name: &str, drop_target: bool) -> Result<bool>;
    fn clear_collection(&self, db: &str, coll: &str) -> Result<bool>;
    fn drop_collection(&self, db: &str, coll: &str) -> Result<bool>;

    fn create_indexes(&self, Vec<IndexInfo>) -> Result<Vec<bool>>;
    fn drop_index(&self, db: &str, coll: &str, name: &str) -> Result<bool>;

    fn drop_database(&self, db: &str) -> Result<bool>;

    fn prepare_collection_writer<'a>(&'a self, db: &str, coll: &str) -> Result<Box<StorageCollectionWriter + 'a>>;

    // TODO should consume self, but not Sized
    fn commit(&self) -> Result<()>;
    fn rollback(&self) -> Result<()>;
    // TODO getSelect, a reader that lives in the write tx
}

pub trait StorageConnection {
    fn list_collections(&self) -> Result<Vec<(String, String, BsonValue)>>;
    fn list_indexes(&self) -> Result<Vec<IndexInfo>>;

    fn begin_write<'a>(&'a self) -> Result<Box<StorageWriter + 'a>>;

    fn begin_read<'a>(&'a self, db: &str, coll: &str, plan: Option<QueryPlan>) -> Result<Box<StorageReader<Item=Result<BsonValue>> + 'a>>;
}


