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
use std::error::Error;

#[derive(Debug)]
enum ElmoError {
    // TODO remove Misc
    Misc(&'static str),

    // TODO more detail within CorruptFile
    CorruptFile(&'static str),

    Bson(bson::BsonError),
    Io(std::io::Error),
    Utf8(std::str::Utf8Error),

    CursorNotValid,
    InvalidPageNumber,
    InvalidPageType,
    RootPageNotInSegmentBlockList,
    Poisoned,
}

impl std::fmt::Display for ElmoError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            ElmoError::Bson(ref err) => write!(f, "bson error: {}", err),
            ElmoError::Io(ref err) => write!(f, "IO error: {}", err),
            ElmoError::Utf8(ref err) => write!(f, "Utf8 error: {}", err),
            ElmoError::Misc(s) => write!(f, "Misc error: {}", s),
            ElmoError::CorruptFile(s) => write!(f, "Corrupt file: {}", s),
            ElmoError::Poisoned => write!(f, "Poisoned"),
            ElmoError::CursorNotValid => write!(f, "Cursor not valid"),
            ElmoError::InvalidPageNumber => write!(f, "Invalid page number"),
            ElmoError::InvalidPageType => write!(f, "Invalid page type"),
            ElmoError::RootPageNotInSegmentBlockList => write!(f, "Root page not in segment block list"),
        }
    }
}

impl std::error::Error for ElmoError {
    fn description(&self) -> &str {
        match *self {
            ElmoError::Bson(ref err) => std::error::Error::description(err),
            ElmoError::Io(ref err) => std::error::Error::description(err),
            ElmoError::Utf8(ref err) => std::error::Error::description(err),
            ElmoError::Misc(s) => s,
            ElmoError::CorruptFile(s) => s,
            ElmoError::Poisoned => "poisoned",
            ElmoError::CursorNotValid => "cursor not valid",
            ElmoError::InvalidPageNumber => "invalid page number",
            ElmoError::InvalidPageType => "invalid page type",
            ElmoError::RootPageNotInSegmentBlockList => "Root page not in segment block list",
        }
    }

    // TODO cause
}

impl From<bson::BsonError> for ElmoError {
    fn from(err: bson::BsonError) -> ElmoError {
        ElmoError::Bson(err)
    }
}

impl From<io::Error> for ElmoError {
    fn from(err: io::Error) -> ElmoError {
        ElmoError::Io(err)
    }
}

impl From<std::str::Utf8Error> for ElmoError {
    fn from(err: std::str::Utf8Error) -> ElmoError {
        ElmoError::Utf8(err)
    }
}

impl<T> From<std::sync::PoisonError<T>> for ElmoError {
    fn from(_err: std::sync::PoisonError<T>) -> ElmoError {
        ElmoError::Poisoned
    }
}

pub type Result<T> = std::result::Result<T, ElmoError>;

trait ElmoWriter {
    // TODO database
    // TODO collection
    fn insert(&self, v: BsonValue) -> Result<()>;
    fn update(&self, v: BsonValue) -> Result<()>;
    fn delete(&self, v: BsonValue) -> Result<bool>;
    // TODO getSelect
    // TODO getIndexes
    fn commit(&self) -> Result<()>;
    fn rollback(&self) -> Result<()>;
}

trait ElmoStorage {
    fn createCollection(&self, db: &str, coll: &str, options: BsonValue) -> Result<bool>;
    fn beginWrite(&self, db: &str, coll: &str) -> Result<Box<ElmoWriter>>;
}

