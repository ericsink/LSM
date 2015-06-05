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

// TODO turn the following warnings back on later
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

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

const SIZE_32: usize = 4; // like std::mem::size_of::<u32>()
const SIZE_16: usize = 2; // like std::mem::size_of::<u16>()

pub type PageNum = u32;
// type PageSize = u32;

// TODO also perhaps the type representing size of a value, u32
// size of a value should NOT be usize, right?

// TODO there is code which assumes that PageNum is u32.
// but that's the nature of the file format.  the type alias
// isn't so much so that we can change it, but rather, to make
// reading the code easier.

pub enum Blob {
    Stream(Box<Read>),
    Array(Box<[u8]>),
    Tombstone,
}

#[derive(Debug)]
enum LsmError {
    // TODO remove Misc
    Misc(&'static str),

    // TODO more detail within CorruptFile
    CorruptFile(&'static str),

    Io(std::io::Error),

    CursorNotValid,
    InvalidPageNumber,
    InvalidPageType,
    RootPageNotInSegmentBlockList,
    Poisoned,
}

impl std::fmt::Display for LsmError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            LsmError::Io(ref err) => write!(f, "IO error: {}", err),
            LsmError::Misc(s) => write!(f, "Misc error: {}", s),
            LsmError::CorruptFile(s) => write!(f, "Corrupt file: {}", s),
            LsmError::Poisoned => write!(f, "Poisoned"),
            LsmError::CursorNotValid => write!(f, "Cursor not valid"),
            LsmError::InvalidPageNumber => write!(f, "Invalid page number"),
            LsmError::InvalidPageType => write!(f, "Invalid page type"),
            LsmError::RootPageNotInSegmentBlockList => write!(f, "Root page not in segment block list"),
        }
    }
}

impl std::error::Error for LsmError {
    fn description(&self) -> &str {
        match *self {
            LsmError::Io(ref err) => std::error::Error::description(err),
            LsmError::Misc(s) => s,
            LsmError::CorruptFile(s) => s,
            LsmError::Poisoned => "poisoned",
            LsmError::CursorNotValid => "cursor not valid",
            LsmError::InvalidPageNumber => "invalid page number",
            LsmError::InvalidPageType => "invalid page type",
            LsmError::RootPageNotInSegmentBlockList => "Root page not in segment block list",
        }
    }

    // TODO cause
}

impl From<io::Error> for LsmError {
    fn from(err: io::Error) -> LsmError {
        LsmError::Io(err)
    }
}

impl<T> From<std::sync::PoisonError<T>> for LsmError {
    fn from(_err: std::sync::PoisonError<T>) -> LsmError {
        LsmError::Poisoned
    }
}

pub type Result<T> = std::result::Result<T, LsmError>;

// kvp is the struct used to provide key-value pairs downward,
// for storage into the database.
pub struct kvp {
    Key : Box<[u8]>,
    Value : Blob,
}

struct PendingSegment {
    blockList: Vec<PageBlock>,
    segnum: SegmentNum,
}

// TODO this is experimental.  it might not be very useful unless
// it can be used everywhere a regular slice can be used.  but we
// obviously don't want to just pass around an Index<Output=u8>
// trait object if that forces us into dynamic dispatch everywhere.
#[cfg(remove_me)]
struct SplitSlice<'a> {
    front: &'a [u8],
    back: &'a [u8],
}

#[cfg(remove_me)]
impl<'a> SplitSlice<'a> {
    fn new(front: &'a [u8], back: &'a [u8]) -> SplitSlice<'a> {
        SplitSlice {front: front, back: back}
    }

    fn len(&self) -> usize {
        self.front.len() + self.back.len()
    }

    fn into_boxed_slice(self) -> Box<[u8]> {
        let mut k = Vec::with_capacity(self.front.len() + self.back.len());
        k.push_all(&self.front);
        k.push_all(&self.back);
        k.into_boxed_slice()
    }
}

#[cfg(remove_me)]
impl<'a> Index<usize> for SplitSlice<'a> {
    type Output = u8;

    fn index(&self, _index: usize) -> &u8 {
        if _index >= self.front.len() {
            &self.back[_index - self.front.len()]
        } else {
            &self.front[_index]
        }
    }
}

fn split3<T>(a: &mut [T], i: usize) -> (&mut [T], &mut [T], &mut [T]) {
    let (before, a2) = a.split_at_mut(i);
    let (islice, after) = a2.split_at_mut(1);
    (before, islice, after)
}

pub enum KeyRef<'a> {
    // for an overflowed key, we just punt and read it into memory
    Overflowed(Box<[u8]>),

    // the other two are references into the page
    Prefixed(&'a [u8],&'a [u8]),
    Array(&'a [u8]),
}

impl<'a> std::fmt::Debug for KeyRef<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::result::Result<(), std::fmt::Error> {
        match *self {
            KeyRef::Overflowed(ref a) => write!(f, "Overflowed, a={:?}", a),
            KeyRef::Prefixed(front,back) => write!(f, "Prefixed, front={:?}, back={:?}", front, back),
            KeyRef::Array(a) => write!(f, "Array, val={:?}", a),
        }
    }
}

impl<'a> KeyRef<'a> {
    pub fn len(&self) -> usize {
        match *self {
            KeyRef::Overflowed(ref a) => a.len(),
            KeyRef::Array(a) => a.len(),
            KeyRef::Prefixed(front,back) => front.len() + back.len(),
        }
    }

    pub fn from_boxed_slice(k: Box<[u8]>) -> KeyRef<'a> {
        KeyRef::Overflowed(k)
    }

    pub fn for_slice(k: &[u8]) -> KeyRef {
        KeyRef::Array(k)
    }

    pub fn into_boxed_slice(self) -> Box<[u8]> {
        match self {
            KeyRef::Overflowed(a) => {
                a
            },
            KeyRef::Array(a) => {
                let mut k = Vec::with_capacity(a.len());
                k.push_all(a);
                k.into_boxed_slice()
            },
            KeyRef::Prefixed(front,back) => {
                let mut k = Vec::with_capacity(front.len() + back.len());
                k.push_all(front);
                k.push_all(back);
                k.into_boxed_slice()
            },
        }
    }

    // TODO move this to the bcmp module?
    fn compare_px_py(px: &[u8], x: &[u8], py: &[u8], y: &[u8]) -> Ordering {
        let xlen = px.len() + x.len();
        let ylen = py.len() + y.len();
        let len = std::cmp::min(xlen, ylen);
        for i in 0 .. len {
            let xval = 
                if i<px.len() {
                    px[i]
                } else {
                    x[i - px.len()]
                };
            let yval = 
                if i<py.len() {
                    py[i]
                } else {
                    y[i - py.len()]
                };
            let c = xval.cmp(&yval);
            if c != Ordering::Equal {
                return c;
            }
        }
        return xlen.cmp(&ylen);
    }

    // TODO move this to the bcmp module?
    fn compare_px_y(px: &[u8], x: &[u8], y: &[u8]) -> Ordering {
        let xlen = px.len() + x.len();
        let ylen = y.len();
        let len = std::cmp::min(xlen, ylen);
        for i in 0 .. len {
            let xval = 
                if i<px.len() {
                    px[i]
                } else {
                    x[i - px.len()]
                };
            let yval = y[i];
            let c = xval.cmp(&yval);
            if c != Ordering::Equal {
                return c;
            }
        }
        return xlen.cmp(&ylen);
    }

    // TODO move this to the bcmp module?
    fn compare_x_py(x: &[u8], py: &[u8], y: &[u8]) -> Ordering {
        let xlen = x.len();
        let ylen = py.len() + y.len();
        let len = std::cmp::min(xlen, ylen);
        for i in 0 .. len {
            let xval = x[i];
            let yval = 
                if i<py.len() {
                    py[i]
                } else {
                    y[i - py.len()]
                };
            let c = xval.cmp(&yval);
            if c != Ordering::Equal {
                return c;
            }
        }
        return xlen.cmp(&ylen);
    }

    pub fn cmp(x: &KeyRef, y: &KeyRef) -> Ordering {
        match (x,y) {
            (&KeyRef::Overflowed(ref x_k), &KeyRef::Overflowed(ref y_k)) => {
                bcmp::Compare(&x_k, &y_k)
            },
            (&KeyRef::Overflowed(ref x_k), &KeyRef::Prefixed(ref y_p, ref y_k)) => {
                Self::compare_x_py(&x_k, y_p, y_k)
            },
            (&KeyRef::Overflowed(ref x_k), &KeyRef::Array(ref y_k)) => {
                bcmp::Compare(&x_k, &y_k)
            },
            (&KeyRef::Prefixed(ref x_p, ref x_k), &KeyRef::Overflowed(ref y_k)) => {
                Self::compare_px_y(x_p, x_k, &y_k)
            },
            (&KeyRef::Array(ref x_k), &KeyRef::Overflowed(ref y_k)) => {
                bcmp::Compare(&x_k, &y_k)
            },
            (&KeyRef::Prefixed(ref x_p, ref x_k), &KeyRef::Prefixed(ref y_p, ref y_k)) => {
                Self::compare_px_py(x_p, x_k, y_p, y_k)
            },
            (&KeyRef::Prefixed(ref x_p, ref x_k), &KeyRef::Array(ref y_k)) => {
                Self::compare_px_y(x_p, x_k, y_k)
            },
            (&KeyRef::Array(ref x_k), &KeyRef::Prefixed(ref y_p, ref y_k)) => {
                Self::compare_x_py(x_k, y_p, y_k)
            },
            (&KeyRef::Array(ref x_k), &KeyRef::Array(ref y_k)) => {
                bcmp::Compare(&x_k, &y_k)
            },
        }
    }
}


pub enum ValueRef<'a> {
    Array(&'a [u8]),
    Overflowed(usize, Box<Read>),
    Tombstone,
}

impl<'a> ValueRef<'a> {
    pub fn len(&self) -> Option<usize> {
        match *self {
            ValueRef::Array(a) => Some(a.len()),
            ValueRef::Overflowed(len, _) => Some(len),
            ValueRef::Tombstone => None,
        }
    }

    pub fn into_blob(self) -> Blob {
        match self {
            ValueRef::Array(a) => {
                let mut k = Vec::with_capacity(a.len());
                k.push_all(a);
                Blob::Array(k.into_boxed_slice())
            },
            ValueRef::Overflowed(len, r) => Blob::Stream(r),
            ValueRef::Tombstone => Blob::Tombstone,
        }
    }
}

impl<'a> std::fmt::Debug for ValueRef<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::result::Result<(), std::fmt::Error> {
        match *self {
            ValueRef::Array(a) => write!(f, "Array, len={:?}", a),
            ValueRef::Overflowed(klen,_) => write!(f, "Overflowed, len={}", klen),
            ValueRef::Tombstone => write!(f, "Tombstone"),
        }
    }
}

#[derive(Hash,PartialEq,Eq,Copy,Clone,Debug)]
struct PageBlock {
    firstPage: PageNum,
    lastPage: PageNum,
}

impl PageBlock {
    fn new(first: PageNum, last: PageNum) -> PageBlock {
        PageBlock { firstPage: first, lastPage: last }
    }

    fn count_pages(&self) -> PageNum {
        self.lastPage - self.firstPage + 1
    }

    fn contains_page(&self, pgnum: PageNum) -> bool {
        (pgnum >= self.firstPage) && (pgnum <= self.lastPage)
    }
}

fn block_list_contains_page(blocks: &Vec<PageBlock>, pgnum: PageNum) -> bool {
    for blk in blocks.iter() {
        if blk.contains_page(pgnum) {
            return true;
        }
    }
    return false;
}

pub type SegmentNum = u64;

trait IPages {
    fn PageSize(&self) -> usize;
    fn Begin(&self) -> Result<PendingSegment>;
    fn GetBlock(&self, token: &mut PendingSegment) -> Result<PageBlock>;
    fn End(&self, token: PendingSegment, page: PageNum) -> Result<SegmentNum>;
}

#[derive(PartialEq,Copy,Clone)]
pub enum SeekOp {
    SEEK_EQ = 0,
    SEEK_LE = 1,
    SEEK_GE = 2,
}

// this code was ported from F# which assumes that any Stream
// that supports Seek also can give you its Length.  That method
// isn't part of the Seek trait, but this implementation should
// suffice.
fn seek_len<R>(fs: &mut R) -> io::Result<u64> where R : Seek {
    // remember where we are
    let pos = try!(fs.seek(SeekFrom::Current(0)));

    // seek the end
    let len = try!(fs.seek(SeekFrom::End(0)));

    // restore to where we were
    let _ = try!(fs.seek(SeekFrom::Start(pos)));

    Ok(len)
}

struct CursorIterator<'a> {
    csr: MultiCursor<'a>
}

impl<'a> CursorIterator<'a> {
    fn new(it: MultiCursor) -> CursorIterator {
        CursorIterator { csr: it }
    }
}

impl<'a> Iterator for CursorIterator<'a> {
    type Item = Result<kvp>;
    fn next(&mut self) -> Option<Result<kvp>> {
        if self.csr.IsValid() {
            let k = {
                let k = self.csr.KeyRef();
                if k.is_err() {
                    return Some(Err(k.err().unwrap()));
                }
                let k = k.unwrap().into_boxed_slice();
                k
            };
            let v = {
                let v = self.csr.ValueRef();
                if v.is_err() {
                    return Some(Err(v.err().unwrap()));
                }
                let v = v.unwrap().into_blob();
                v
            };
            let r = self.csr.Next();
            if r.is_err() {
                return Some(Err(r.err().unwrap()));
            }
            Some(Ok(kvp{Key:k, Value:v}))
        } else {
            return None;
        }
    }
}

#[derive(Copy,Clone,Debug)]
pub enum SeekResult {
    Invalid,
    Unequal,
    Equal,
}

impl SeekResult {
    fn from_cursor<'a, T: ICursor<'a>>(csr: &T, k: &KeyRef) -> Result<SeekResult> {
        if csr.IsValid() {
            if Ordering::Equal == try!(csr.KeyCompare(k)) {
                Ok(SeekResult::Equal)
            } else {
                Ok(SeekResult::Unequal)
            }
        } else {
            Ok(SeekResult::Invalid)
        }
    }

    fn is_valid(self) -> bool {
        match self {
            SeekResult::Invalid => false,
            SeekResult::Unequal => true,
            SeekResult::Equal => true,
        }
    }

    fn is_valid_and_equal(self) -> bool {
        match self {
            SeekResult::Invalid => false,
            SeekResult::Unequal => false,
            SeekResult::Equal => true,
        }
    }
}

pub trait ICursor<'a> {
    fn SeekRef(&mut self, k: &KeyRef, sop: SeekOp) -> Result<SeekResult>;
    fn First(&mut self) -> Result<()>;
    fn Last(&mut self) -> Result<()>;
    fn Next(&mut self) -> Result<()>;
    fn Prev(&mut self) -> Result<()>;

    fn IsValid(&self) -> bool;

    fn KeyRef(&'a self) -> Result<KeyRef<'a>>;
    fn ValueRef(&'a self) -> Result<ValueRef<'a>>;

    // TODO maybe rm ValueLength.  but LivingCursor uses it as a fast
    // way to detect whether a value is a tombstone or not.
    fn ValueLength(&self) -> Result<Option<usize>>; // tombstone is None

    // TODO maybe rm KeyCompare
    fn KeyCompare(&self, k: &KeyRef) -> Result<Ordering>;
}

//#[derive(Copy,Clone)]
pub struct DbSettings {
    pub AutoMergeEnabled : bool,
    pub AutoMergeMinimumPages : PageNum,
    pub DefaultPageSize : usize,
    pub PagesPerBlock : PageNum,
}

pub const DEFAULT_SETTINGS : DbSettings = 
    DbSettings
    {
        AutoMergeEnabled : true,
        AutoMergeMinimumPages : 4,
        DefaultPageSize : 4096,
        PagesPerBlock : 256,
    };

#[derive(Clone)]
struct SegmentInfo {
    root : PageNum,
    age : u32,
    // TODO does this grow?  shouldn't it be a boxed array?
    // yes, but then derive clone complains.
    // ideally we could just stop cloning this struct.
    blocks : Vec<PageBlock> 
}

pub mod utils {
    use std::io;
    use std::io::Seek;
    use std::io::Read;
    use std::io::SeekFrom;
    use super::PageNum;
    use super::LsmError;
    use super::Result;

    pub fn SeekPage(strm: &mut Seek, pgsz: usize, pageNumber: PageNum) -> Result<u64> {
        if 0==pageNumber { 
            return Err(LsmError::InvalidPageNumber);
        }
        let pos = ((pageNumber as u64) - 1) * (pgsz as u64);
        let v = try!(strm.seek(SeekFrom::Start(pos)));
        Ok(v)
    }

    pub fn ReadFully(strm: &mut Read, buf: &mut [u8]) -> io::Result<usize> {
        let mut sofar = 0;
        let len = buf.len();
        loop {
            let cur = &mut buf[sofar..len];
            let n = try!(strm.read(cur));
            if n==0 {
                break;
            }
            sofar += n;
            if sofar==len {
                break;
            }
        }
        let res : io::Result<usize> = Ok(sofar);
        res
    }
}

mod bcmp {
    use std::cmp::Ordering;
    use std::cmp::min;

    // this fn is actually kinda handy to make sure that we are comparing
    // what we are supposed to be comparing.  if we just use cmp, we
    // can end up comparing two things that happen to match each other's
    // type but which do not match &[u8].  this function makes the type
    // checking more explicit.
    #[inline(always)]
    pub fn Compare(x: &[u8], y: &[u8]) -> Ordering {
        x.cmp(y)
    }

    pub fn PrefixMatch(x: &[u8], y: &[u8], max: usize) -> usize {
        let len = min(x.len(), y.len());
        let lim = min(len, max);
        let mut i = 0;
        while i<lim && x[i]==y[i] {
            i = i + 1;
        }
        i
    }

    #[cfg(remove_me)]
    fn StartsWith(x: &[u8], y: &[u8], max: usize) -> bool {
        if x.len() < y.len() {
            false
        } else {
            let len = y.len();
            let mut i = 0;
            while i<len && x[i]==y[i] {
                i = i + 1;
            }
            i==len
        }
    }
}

mod Varint {
    // TODO this doesn't need to be usize.  u8 is enough.
    pub fn SpaceNeededFor(v: u64) -> usize {
        if v<=240 { 1 }
        else if v<=2287 { 2 }
        else if v<=67823 { 3 }
        else if v<=16777215 { 4 }
        else if v<=4294967295 { 5 }
        else if v<=1099511627775 { 6 }
        else if v<=281474976710655 { 7 }
        else if v<=72057594037927935 { 8 }
        else { 9 }
    }

    // TODO stronger inline hint?
    pub fn read(buf: &[u8], cur: &mut usize) -> u64 {
        let c = *cur;
        let a0 = buf[c] as u64;
        if a0 <= 240u64 { 
            *cur = *cur + 1;
            a0
        } else if a0 <= 248u64 {
            let a1 = buf[c+1] as u64;
            let r = 240u64 + 256u64 * (a0 - 241u64) + a1;
            *cur = *cur + 2;
            r
        } else if a0 == 249u64 {
            let a1 = buf[c+1] as u64;
            let a2 = buf[c+2] as u64;
            let r = 2288u64 + 256u64 * a1 + a2;
            *cur = *cur + 3;
            r
        } else if a0 == 250u64 {
            let a1 = buf[c+1] as u64;
            let a2 = buf[c+2] as u64;
            let a3 = buf[c+3] as u64;
            let r = (a1<<16) | (a2<<8) | a3;
            *cur = *cur + 4;
            r
        } else if a0 == 251u64 {
            let a1 = buf[c+1] as u64;
            let a2 = buf[c+2] as u64;
            let a3 = buf[c+3] as u64;
            let a4 = buf[c+4] as u64;
            let r = (a1<<24) | (a2<<16) | (a3<<8) | a4;
            *cur = *cur + 5;
            r
        } else if a0 == 252u64 {
            let a1 = buf[c+1] as u64;
            let a2 = buf[c+2] as u64;
            let a3 = buf[c+3] as u64;
            let a4 = buf[c+4] as u64;
            let a5 = buf[c+5] as u64;
            let r = (a1<<32) | (a2<<24) | (a3<<16) | (a4<<8) | a5;
            *cur = *cur + 6;
            r
        } else if a0 == 253u64 {
            let a1 = buf[c+1] as u64;
            let a2 = buf[c+2] as u64;
            let a3 = buf[c+3] as u64;
            let a4 = buf[c+4] as u64;
            let a5 = buf[c+5] as u64;
            let a6 = buf[c+6] as u64;
            let r = (a1<<40) | (a2<<32) | (a3<<24) | (a4<<16) | (a5<<8) | a6;
            *cur = *cur + 7;
            r
        } else if a0 == 254u64 {
            let a1 = buf[c+1] as u64;
            let a2 = buf[c+2] as u64;
            let a3 = buf[c+3] as u64;
            let a4 = buf[c+4] as u64;
            let a5 = buf[c+5] as u64;
            let a6 = buf[c+6] as u64;
            let a7 = buf[c+7] as u64;
            let r = (a1<<48) | (a2<<40) | (a3<<32) | (a4<<24) | (a5<<16) | (a6<<8) | a7;
            *cur = *cur + 8;
            r
        } else {
            let a1 = buf[c+1] as u64;
            let a2 = buf[c+2] as u64;
            let a3 = buf[c+3] as u64;
            let a4 = buf[c+4] as u64;
            let a5 = buf[c+5] as u64;
            let a6 = buf[c+6] as u64;
            let a7 = buf[c+7] as u64;
            let a8 = buf[c+8] as u64;
            let r = (a1<<56) | (a2<<48) | (a3<<40) | (a4<<32) | (a5<<24) | (a6<<16) | (a7<<8) | a8;
            *cur = *cur + 9;
            r
        }
    }

    pub fn write(buf: &mut [u8], cur: &mut usize, v: u64) {
        let c = *cur;
        if v<=240u64 { 
            buf[c] = v as u8;
            *cur = *cur + 1;
        } else if v<=2287u64 { 
            buf[c] = ((v - 240u64) / 256u64 + 241u64) as u8;
            buf[c+1] = ((v - 240u64) % 256u64) as u8;
            *cur = *cur + 2;
        } else if v<=67823u64 { 
            buf[c] = 249u8;
            buf[c+1] = ((v - 2288u64) / 256u64) as u8;
            buf[c+2] = ((v - 2288u64) % 256u64) as u8;
            *cur = *cur + 3;
        } else if v<=16777215u64 { 
            buf[c] = 250u8;
            buf[c+1] = (v >> 16) as u8;
            buf[c+2] = (v >>  8) as u8;
            buf[c+3] = (v >>  0) as u8;
            *cur = *cur + 4;
        } else if v<=4294967295u64 { 
            buf[c] = 251u8;
            buf[c+1] = (v >> 24) as u8;
            buf[c+2] = (v >> 16) as u8;
            buf[c+3] = (v >>  8) as u8;
            buf[c+4] = (v >>  0) as u8;
            *cur = *cur + 5;
        } else if v<=1099511627775u64 { 
            buf[c] = 252u8;
            buf[c+1] = (v >> 32) as u8;
            buf[c+2] = (v >> 24) as u8;
            buf[c+3] = (v >> 16) as u8;
            buf[c+4] = (v >>  8) as u8;
            buf[c+5] = (v >>  0) as u8;
            *cur = *cur + 6;
        } else if v<=281474976710655u64 { 
            buf[c] = 253u8;
            buf[c+1] = (v >> 40) as u8;
            buf[c+2] = (v >> 32) as u8;
            buf[c+3] = (v >> 24) as u8;
            buf[c+4] = (v >> 16) as u8;
            buf[c+5] = (v >>  8) as u8;
            buf[c+6] = (v >>  0) as u8;
            *cur = *cur + 7;
        } else if v<=72057594037927935u64 { 
            buf[c] = 254u8;
            buf[c+1] = (v >> 48) as u8;
            buf[c+2] = (v >> 40) as u8;
            buf[c+3] = (v >> 32) as u8;
            buf[c+4] = (v >> 24) as u8;
            buf[c+5] = (v >> 16) as u8;
            buf[c+6] = (v >>  8) as u8;
            buf[c+7] = (v >>  0) as u8;
            *cur = *cur + 8;
        } else {
            buf[c] = 255u8;
            buf[c+1] = (v >> 56) as u8;
            buf[c+2] = (v >> 48) as u8;
            buf[c+3] = (v >> 40) as u8;
            buf[c+4] = (v >> 32) as u8;
            buf[c+5] = (v >> 24) as u8;
            buf[c+6] = (v >> 16) as u8;
            buf[c+7] = (v >>  8) as u8;
            buf[c+8] = (v >>  0) as u8;
            *cur = *cur + 9;
        }
    }
}

fn write_u32_le(v: &mut [u8], i: u32)
{
    v[0] = ((i>> 0) & 0xff_u32) as u8;
    v[1] = ((i>> 8) & 0xff_u32) as u8;
    v[2] = ((i>>16) & 0xff_u32) as u8;
    v[3] = ((i>>24) & 0xff_u32) as u8;
}

fn write_f64_le(v: &mut [u8], f: f64)
{
    // TODO
}

fn write_i32_le(v: &mut [u8], f: i32)
{
    // TODO
}

fn write_i64_le(v: &mut [u8], f: i64)
{
    // TODO
}

fn write_u32_be(v: &mut [u8], i: u32)
{
    // TODO can this assert be checked at compile time?
    // TODO do this kind of assert in other places too.
    assert!(v.len() == 4);
    v[0] = ((i>>24) & 0xff_u32) as u8;
    v[1] = ((i>>16) & 0xff_u32) as u8;
    v[2] = ((i>> 8) & 0xff_u32) as u8;
    v[3] = ((i>> 0) & 0xff_u32) as u8;
}

fn read_u32_be(v: &[u8]) -> u32
{
    let a0 = v[0] as u64;
    let a1 = v[1] as u64;
    let a2 = v[2] as u64;
    let a3 = v[3] as u64;
    let r = (a0 << 24) | (a1 << 16) | (a2 << 8) | (a3 << 0);
    // assert r fits
    r as u32
}

fn read_u16_be(v: &[u8]) -> u16
{
    let a0 = v[0] as u64;
    let a1 = v[1] as u64;
    let r = (a0 << 8) | (a1 << 0);
    // assert r fits
    r as u16
}

fn write_u16_be(v: &mut [u8], i: u16)
{
    v[0] = ((i>>8) & 0xff_u16) as u8;
    v[1] = ((i>>0) & 0xff_u16) as u8;
}

struct PageBuilder {
    cur : usize,
    buf : Box<[u8]>,
}

// TODO bundling cur with the buf almost seems sad, because there are
// cases where we want buf to be mutable but not cur.  :-)

impl PageBuilder {
    fn new(pgsz : usize) -> PageBuilder { 
        let ba = vec![0;pgsz as usize].into_boxed_slice();
        PageBuilder { cur: 0, buf:ba } 
    }

    fn Reset(&mut self) {
        self.cur = 0;
    }

    fn Write(&self, strm: &mut Write) -> io::Result<()> {
        strm.write_all(&*self.buf)
    }

    #[cfg(remove_me)]
    fn PageSize(&self) -> usize {
        self.buf.len()
    }

    fn Buffer(&self) -> &[u8] {
        &self.buf
    }
    
    #[cfg(remove_me)]
    fn Position(&self) -> usize {
        self.cur
    }

    fn Available(&self) -> usize {
        self.buf.len() - self.cur
    }

    fn SetPageFlag(&mut self, x: u8) {
        self.buf[1] = self.buf[1] | (x);
    }

    fn PutByte(&mut self, x: u8) {
        self.buf[self.cur] = x;
        self.cur = self.cur + 1;
    }

    fn PutStream2(&mut self, s: &mut Read, len: usize) -> io::Result<usize> {
        let n = try!(utils::ReadFully(s, &mut self.buf[self.cur .. self.cur + len]));
        self.cur = self.cur + n;
        let res : io::Result<usize> = Ok(n);
        res
    }

    #[cfg(remove_me)]
    fn PutStream(&mut self, s: &mut Read, len: usize) -> io::Result<usize> {
        let n = try!(self.PutStream2(s, len));
        // TODO if n != len fail, which may mean a different result type here
        let res : io::Result<usize> = Ok(len);
        res
    }

    fn PutArray(&mut self, ba: &[u8]) {
        self.buf[self.cur .. self.cur + ba.len()].clone_from_slice(ba);
        self.cur = self.cur + ba.len();
    }

    fn PutInt32(&mut self, ov: u32) {
        let at = self.cur;
        write_u32_be(&mut self.buf[at .. at + SIZE_32], ov);
        self.cur = self.cur + SIZE_32;
    }

    fn SetSecondToLastInt32(&mut self, page: u32) {
        let len = self.buf.len();
        let at = len - 2 * SIZE_32;
        if self.cur > at { panic!("SetSecondToLastInt32 is squashing data"); }
        write_u32_be(&mut self.buf[at .. at + SIZE_32], page);
    }

    fn SetLastInt32(&mut self, page: u32) {
        let len = self.buf.len();
        let at = len - 1 * SIZE_32;
        if self.cur > at { panic!("SetLastInt32 is squashing data"); }
        write_u32_be(&mut self.buf[at .. at + SIZE_32], page);
    }

    fn PutInt16(&mut self, ov: u16) {
        let at = self.cur;
        write_u16_be(&mut self.buf[at .. at + SIZE_16], ov);
        self.cur = self.cur + SIZE_16;
    }

    #[cfg(remove_me)]
    fn PutInt16At(&mut self, at: usize, ov: u16) {
        write_u16_be(&mut self.buf[at .. at + SIZE_16], ov);
    }

    fn PutVarint(&mut self, ov: u64) {
        Varint::write(&mut *self.buf, &mut self.cur, ov);
    }

}

// TODO this struct should just go away.  just use the buf.
struct PageBuffer {
    buf : Box<[u8]>,
}

impl PageBuffer {
    fn new(pgsz: usize) -> PageBuffer { 
        let ba = vec![0;pgsz as usize].into_boxed_slice();
        PageBuffer { buf:ba } 
    }

    fn PageSize(&self) -> usize {
        self.buf.len()
    }

    fn Read(&mut self, strm: &mut Read) -> io::Result<usize> {
        utils::ReadFully(strm, &mut self.buf)
    }

    fn ReadPart(&mut self, strm: &mut Read, off: usize, len: usize) -> io::Result<usize> {
        utils::ReadFully(strm, &mut self.buf[off .. len-off])
    }

    #[cfg(remove_me)]
    fn Compare(&self, cur: usize, len: usize, other: &[u8]) -> Ordering {
        let slice = &self.buf[cur .. cur + len];
        bcmp::Compare(slice, other)
    }

    fn PageType(&self) -> Result<PageType> {
        PageType::from_u8(self.buf[0])
    }

    fn GetByte(&self, cur: &mut usize) -> u8 {
        let r = self.buf[*cur];
        *cur = *cur + 1;
        r
    }

    fn GetInt32(&self, cur: &mut usize) -> u32 {
        let at = *cur;
        let r = read_u32_be(&self.buf[at .. at + SIZE_32]);
        *cur = *cur + SIZE_32;
        r
    }

    fn GetInt32At(&self, at: usize) -> u32 {
        read_u32_be(&self.buf[at .. at + SIZE_32])
    }

    fn CheckPageFlag(&self, f: u8) -> bool {
        0 != (self.buf[1] & f)
    }

    fn GetSecondToLastInt32(&self) -> u32 {
        let len = self.buf.len();
        let at = len - 2 * SIZE_32;
        self.GetInt32At(at)
    }

    fn GetLastInt32(&self) -> u32 {
        let len = self.buf.len();
        let at = len - 1 * SIZE_32;
        self.GetInt32At(at)
    }

    fn GetInt16(&self, cur: &mut usize) -> u16 {
        let at = *cur;
        let r = read_u16_be(&self.buf[at .. at + SIZE_16]);
        *cur = *cur + SIZE_16;
        r
    }

    fn get_slice(&self, start: usize, len: usize) -> &[u8] {
        &self.buf[start .. start + len]
    }

    fn GetIntoArray(&self, cur: &mut usize,  a : &mut [u8]) {
        let len = a.len();
        a.clone_from_slice(&self.buf[*cur .. *cur + len]);
        *cur = *cur + a.len();
    }

    // TODO this function shows up a lot in the profiler
    // TODO inline hint?  Varint::read() gets inlined here,
    // but this one does not seem to get inlined anywhere.
    fn GetVarint(&self, cur: &mut usize) -> u64 {
        Varint::read(&*self.buf, cur)
    }

}

#[derive(PartialEq,Copy,Clone)]
enum Direction {
    FORWARD = 0,
    BACKWARD = 1,
    WANDERING = 2,
}

struct MultiCursor<'a> { 
    subcursors: Box<[SegmentCursor<'a>]>, 
    sorted: Box<[(usize,Option<Ordering>)]>,
    cur: Option<usize>, 
    dir: Direction,
}

impl<'a> MultiCursor<'a> {
    fn sort(&mut self, want_max: bool) -> Result<()> {
        if self.subcursors.is_empty() {
            return Ok(())
        }

        // TODO this memory allocation is expensive.

        // get a KeyRef for all the cursors
        let mut ka = Vec::with_capacity(self.subcursors.len());
        for c in self.subcursors.iter() {
            if c.IsValid() {
                ka.push(Some(try!(c.KeyRef())));
            } else {
                ka.push(None);
            }
        }

        // TODO consider converting ka to a boxed slice here?

        // init the orderings to None.
        // the invalid cursors will stay that way.
        for i in 0 .. self.sorted.len() {
            self.sorted[i].1 = None;
        }

        for i in 1 .. self.sorted.len() {
            let mut j = i;
            while j > 0 {
                let nj = self.sorted[j].0;
                let nprev = self.sorted[j - 1].0;
                match (&ka[nj], &ka[nprev]) {
                    (&Some(ref kj), &Some(ref kprev)) => {
                        let c = {
                            if want_max {
                                KeyRef::cmp(kprev, kj)
                            } else {
                                KeyRef::cmp(kj, kprev)
                            }
                        };
                        match c {
                            Ordering::Greater => {
                                self.sorted[j].1 = Some(Ordering::Greater);
                                break;
                            },
                            Ordering::Equal => {
                                match nj.cmp(&nprev) {
                                    Ordering::Equal => {
                                        unreachable!();
                                    },
                                    Ordering::Greater => {
                                        self.sorted[j].1 = Some(Ordering::Equal);
                                        break;
                                    },
                                    Ordering::Less => {
                                        self.sorted[j - 1].1 = Some(Ordering::Equal);
                                        // keep going
                                    },
                                }
                            },
                            Ordering::Less => {
                                // keep going
                                self.sorted[j - 1].1 = Some(Ordering::Greater);
                            },
                        }
                    },
                    (&Some(_), &None) => {
                        // keep going
                    },
                    (&None, &Some(_)) => {
                        break;
                    },
                    (&None, &None) => {
                        match nj.cmp(&nprev) {
                            Ordering::Equal => {
                                unreachable!();
                            },
                            Ordering::Greater => {
                                break;
                            },
                            Ordering::Less => {
                                // keep going
                            },
                        }
                    }
                };
                self.sorted.swap(j, j - 1);
                j = j - 1;
            }
        }

        // fix the first one
        if self.sorted.len() > 0 {
            let n = self.sorted[0].0;
            if ka[n].is_some() {
                self.sorted[0].1 = Some(Ordering::Equal);
            }
        }

        /*
        println!("{:?} : {}", self.sorted, if want_max { "backward" } else {"forward"} );
        for i in 0 .. self.sorted.len() {
            let (n, ord) = self.sorted[i];
            println!("    {:?}", ka[n]);
        }
        */
        Ok(())
    }

    fn sorted_first(&self) -> Option<usize> {
        let n = self.sorted[0].0;
        if self.sorted[0].1.is_some() {
            Some(n)
        } else {
            None
        }
    }

    fn findMin(&mut self) -> Result<Option<usize>> {
        self.dir = Direction::FORWARD;
        if self.subcursors.is_empty() {
            Ok(None)
        } else {
            try!(self.sort(false));
            Ok(self.sorted_first())
        }
    }

    fn findMax(&mut self) -> Result<Option<usize>> {
        self.dir = Direction::BACKWARD; 
        if self.subcursors.is_empty() {
            Ok(None)
        } else {
            try!(self.sort(true));
            Ok(self.sorted_first())
        }
    }

    fn Create(subs: Vec<SegmentCursor>) -> MultiCursor {
        let s = subs.into_boxed_slice();
        let mut sorted = Vec::with_capacity(s.len());
        for i in 0 .. s.len() {
            sorted.push((i, None));
        }
        MultiCursor { 
            subcursors: s, 
            sorted: sorted.into_boxed_slice(), 
            cur: None, 
            dir: Direction::WANDERING,
        }
    }

}

impl<'a> ICursor<'a> for MultiCursor<'a> {
    fn IsValid(&self) -> bool {
        match self.cur {
            Some(i) => self.subcursors[i].IsValid(),
            None => false
        }
    }

    fn First(&mut self) -> Result<()> {
        for i in 0 .. self.subcursors.len() {
            try!(self.subcursors[i].First());
        }
        self.cur = try!(self.findMin());
        Ok(())
    }

    fn Last(&mut self) -> Result<()> {
        for i in 0 .. self.subcursors.len() {
            try!(self.subcursors[i].Last());
        }
        self.cur = try!(self.findMax());
        Ok(())
    }

    fn KeyRef(&'a self) -> Result<KeyRef<'a>> {
        match self.cur {
            None => Err(LsmError::CursorNotValid),
            Some(icur) => self.subcursors[icur].KeyRef(),
        }
    }

    fn ValueRef(&'a self) -> Result<ValueRef<'a>> {
        match self.cur {
            None => Err(LsmError::CursorNotValid),
            Some(icur) => self.subcursors[icur].ValueRef(),
        }
    }

    fn KeyCompare(&self, k: &KeyRef) -> Result<Ordering> {
        match self.cur {
            None => Err(LsmError::CursorNotValid),
            Some(icur) => self.subcursors[icur].KeyCompare(k),
        }
    }

    fn ValueLength(&self) -> Result<Option<usize>> {
        match self.cur {
            None => Err(LsmError::CursorNotValid),
            Some(icur) => self.subcursors[icur].ValueLength(),
        }
    }

    fn Next(&mut self) -> Result<()> {
        match self.cur {
            None => Err(LsmError::CursorNotValid),
            Some(icur) => {
                // we need to fix every cursor to point to its min
                // value > icur.

                // if perf didn't matter, this would be simple.
                // call Next on icur.  and call Seek(GE) (and maybe Next)
                // on every other cursor.

                // but there are several cases where we can do a lot
                // less work than a Seek.  And we have the information
                // to identify those cases.  So, this function is
                // pretty complicated, but it's fast.

                // --------

                // the current cursor (icur) is easy.  it just needs Next().
                // we'll do it last, so we can use it for comparisons.
                // for now we deal with all the others.

                // the current direction of the multicursor tells us
                // something about the state of all the others.

                if (self.dir == Direction::FORWARD) {
                    // this is the happy case.  each cursor is at most
                    // one step away.

                    // direction is FORWARD, so we know that every valid cursor
                    // is pointing at a key which is either == to icur, or
                    // it is already the min key > icur.

                    assert!(icur == self.sorted[0].0);
                    // immediately after that, there may (or may not be) some
                    // entries which were Ordering:Equal to cur.  call Next on
                    // each of these.

                    for i in 1 .. self.sorted.len() {
                        //println!("sorted[{}] : {:?}", i, self.sorted[i]);
                        let (n,c) = self.sorted[i];
                        match c {
                            None => {
                                break;
                            },
                            Some(c) => {
                                if c == Ordering::Equal {
                                    try!(self.subcursors[n].Next());
                                } else {
                                    break;
                                }
                            },
                        }
                    }

                } else {
                    // TODO consider simplifying all the stuff below.
                    // all this complexity may not be worth it.

                    fn half(dir: Direction, ki: &KeyRef, subs: &mut [SegmentCursor]) -> Result<()> {
                        match dir {
                            Direction::FORWARD => {
                                unreachable!();
                            },
                            Direction::BACKWARD => {
                                // this case isn't too bad.  each cursor is either
                                // one step away or two.
                                
                                // every other cursor is either == icur or it is the
                                // max value < icur.

                                for csr in subs {
                                    if csr.IsValid() {
                                        let cmp = {
                                            let k = try!(csr.KeyRef());
                                            let cmp = KeyRef::cmp(&k, ki);
                                            cmp
                                        };
                                        match cmp {
                                            Ordering::Less => {
                                                try!(csr.Next());
                                                // we moved one step.  let's see if we need to move one more.
                                                if csr.IsValid() {
                                                    let cmp = {
                                                        let k = try!(csr.KeyRef());
                                                        let cmp = KeyRef::cmp(&k, ki);
                                                        cmp
                                                    };
                                                    match cmp {
                                                        Ordering::Less => {
                                                            // should never happen.  we should not have
                                                            // been more than one step away from icur.
                                                            unreachable!();
                                                        },
                                                        Ordering::Greater => {
                                                            // done
                                                        },
                                                        Ordering::Equal => {
                                                            // and one more step
                                                            try!(csr.Next());
                                                        },
                                                    }
                                                }
                                            },
                                            Ordering::Greater => {
                                                // should never happen, because BACKWARD
                                                unreachable!();
                                            },
                                            Ordering::Equal => {
                                                // one step away
                                                try!(csr.Next());
                                            },
                                        }
                                    } else {
                                        let sr = try!(csr.SeekRef(&ki, SeekOp::SEEK_GE));
                                        if sr.is_valid_and_equal() {
                                            try!(csr.Next());
                                        }
                                    }
                                }

                                Ok(())
                            },
                            Direction::WANDERING => {
                                // we have no idea where all the other cursors are.
                                // so we have to do a seek on each one.

                                for j in 0 .. subs.len() {
                                    let csr = &mut subs[j];
                                    let sr = try!(csr.SeekRef(&ki, SeekOp::SEEK_GE));
                                    if sr.is_valid_and_equal() {
                                        try!(csr.Next());
                                    }
                                }
                                Ok(())
                            },
                        }
                    }

                    {
                        let (before, middle, after) = split3(&mut *self.subcursors, icur);
                        let icsr = &middle[0];
                        let ki = try!(icsr.KeyRef());
                        half(self.dir, &ki, before);
                        half(self.dir, &ki, after);
                    }
                }

                // now the current cursor
                try!(self.subcursors[icur].Next());

                // now re-sort
                self.cur = try!(self.findMin());
                Ok(())
            },
        }
    }

    // TODO fix Prev like Next
    fn Prev(&mut self) -> Result<()> {
        match self.cur {
            None => Err(LsmError::CursorNotValid),
            Some(icur) => {
                let k = {
                    let k = try!(self.subcursors[icur].KeyRef());
                    let k = k.into_boxed_slice();
                    let k = KeyRef::from_boxed_slice(k);
                    k
                };
                for j in 0 .. self.subcursors.len() {
                    let csr = &mut self.subcursors[j];
                    if (self.dir != Direction::BACKWARD) && (icur != j) { 
                        try!(csr.SeekRef(&k, SeekOp::SEEK_LE));
                    }
                    if csr.IsValid() && (Ordering::Equal == try!(csr.KeyCompare(&k))) { 
                        try!(csr.Prev());
                    }
                }
                self.cur = try!(self.findMax());
                Ok(())
            },
        }
    }

    fn SeekRef(&mut self, k: &KeyRef, sop:SeekOp) -> Result<SeekResult> {
        self.cur = None;
        self.dir = Direction::WANDERING;
        for j in 0 .. self.subcursors.len() {
            let sr = try!(self.subcursors[j].SeekRef(k, sop));
            if sr.is_valid_and_equal() { 
                self.cur = Some(j);
                return Ok(sr);
            }
        }
        match sop {
            SeekOp::SEEK_GE => {
                self.cur = try!(self.findMin());
                match self.cur {
                    Some(i) => {
                        SeekResult::from_cursor(&self.subcursors[i], k)
                    },
                    None => {
                        Ok(SeekResult::Invalid)
                    },
                }
            },
            SeekOp::SEEK_LE => {
                self.cur = try!(self.findMax());
                match self.cur {
                    Some(i) => {
                        SeekResult::from_cursor(&self.subcursors[i], k)
                    },
                    None => {
                        Ok(SeekResult::Invalid)
                    },
                }
            },
            SeekOp::SEEK_EQ => {
                Ok(SeekResult::Invalid)
            },
        }
    }

}

pub struct LivingCursor<'a> { 
    chain : MultiCursor<'a>
}

impl<'a> LivingCursor<'a> {
    fn skipTombstonesForward(&mut self) -> Result<()> {
        while self.chain.IsValid() && try!(self.chain.ValueLength()).is_none() {
            try!(self.chain.Next());
        }
        Ok(())
    }

    fn skipTombstonesBackward(&mut self) -> Result<()> {
        while self.chain.IsValid() && try!(self.chain.ValueLength()).is_none() {
            try!(self.chain.Prev());
        }
        Ok(())
    }

    fn Create(ch : MultiCursor) -> LivingCursor {
        LivingCursor { chain : ch }
    }
}

impl<'a> ICursor<'a> for LivingCursor<'a> {
    fn First(&mut self) -> Result<()> {
        try!(self.chain.First());
        try!(self.skipTombstonesForward());
        Ok(())
    }

    fn Last(&mut self) -> Result<()> {
        try!(self.chain.Last());
        try!(self.skipTombstonesBackward());
        Ok(())
    }

    fn KeyRef(&'a self) -> Result<KeyRef<'a>> {
        self.chain.KeyRef()
    }

    fn ValueRef(&'a self) -> Result<ValueRef<'a>> {
        self.chain.ValueRef()
    }

    fn ValueLength(&self) -> Result<Option<usize>> {
        self.chain.ValueLength()
    }

    fn IsValid(&self) -> bool {
        self.chain.IsValid() 
            && {
                let r = self.chain.ValueLength();
                if r.is_ok() {
                    r.unwrap().is_some()
                } else {
                    false
                }
            }
    }

    fn KeyCompare(&self, k: &KeyRef) -> Result<Ordering> {
        self.chain.KeyCompare(k)
    }

    fn Next(&mut self) -> Result<()> {
        try!(self.chain.Next());
        try!(self.skipTombstonesForward());
        Ok(())
    }

    fn Prev(&mut self) -> Result<()> {
        try!(self.chain.Prev());
        try!(self.skipTombstonesBackward());
        Ok(())
    }

    fn SeekRef(&mut self, k: &KeyRef, sop:SeekOp) -> Result<SeekResult> {
        let sr = try!(self.chain.SeekRef(k, sop));
        match sop {
            SeekOp::SEEK_GE => {
                if sr.is_valid() && self.chain.ValueLength().unwrap().is_none() {
                    try!(self.skipTombstonesForward());
                    SeekResult::from_cursor(&self.chain, k)
                } else {
                    Ok(sr)
                }
            },
            SeekOp::SEEK_LE => {
                if sr.is_valid() && self.chain.ValueLength().unwrap().is_none() {
                    try!(self.skipTombstonesBackward());
                    SeekResult::from_cursor(&self.chain, k)
                } else {
                    Ok(sr)
                }
            },
            SeekOp::SEEK_EQ => Ok(sr),
        }
    }

}

#[derive(Hash,PartialEq,Eq,Copy,Clone,Debug)]
#[repr(u8)]
enum PageType {
    LEAF_NODE,
    PARENT_NODE,
    OVERFLOW_NODE,
}

impl PageType {

    #[inline(always)]
    fn to_u8(self) -> u8 {
        match self {
            PageType::LEAF_NODE => 1,
            PageType::PARENT_NODE => 2,
            PageType::OVERFLOW_NODE => 3,
        }
    }

    #[inline(always)]
    fn from_u8(v: u8) -> Result<PageType> {
        match v {
            1 => Ok(PageType::LEAF_NODE),
            2 => Ok(PageType::PARENT_NODE),
            3 => Ok(PageType::OVERFLOW_NODE),
            _ => Err(LsmError::InvalidPageType),
        }
    }
}

mod ValueFlag {
    pub const FLAG_OVERFLOW: u8 = 1;
    pub const FLAG_TOMBSTONE: u8 = 2;
}

mod PageFlag {
    pub const FLAG_ROOT_NODE: u8 = 1;
    pub const FLAG_BOUNDARY_NODE: u8 = 2;
    pub const FLAG_ENDS_ON_BOUNDARY: u8 = 3;
}

#[derive(Debug)]
// this struct is used to remember pages we have written.
// for each page, we need to remember a key, and it needs
// to be in a box because the original copy is gone and
// the page has been written out to disk.
struct pgitem {
    page : PageNum,
    key : Box<[u8]>,
}

struct ParentState {
    sofar : usize,
    nextGeneration : Vec<pgitem>,
    blk : PageBlock,
}

// this enum keeps track of what happened to a key as we
// processed it.  either we determined that it will fit
// inline or we wrote it as an overflow.
enum KeyLocation {
    Inline,
    Overflowed(PageNum),
}

// this enum keeps track of what happened to a value as we
// processed it.  it might have already been overflowed.  if
// it's going to fit in the page, we still have the data
// buffer.
enum ValueLocation {
    Tombstone,
    // when this is a Buffer, this gets ownership of kvp.Value
    Buffer(Box<[u8]>),
    Overflowed(usize,PageNum),
}

struct LeafPair {
    // key gets ownership of kvp.Key
    key : Box<[u8]>,
    kLoc : KeyLocation,
    vLoc : ValueLocation,
}

struct LeafState {
    sofarLeaf : usize,
    keys_in_this_leaf : Vec<LeafPair>,
    prevLeaf : PageNum,
    prefixLen : usize,
    firstLeaf : PageNum,
    leaves : Vec<pgitem>,
    blk : PageBlock,
}

fn CreateFromSortedSequenceOfKeyValuePairs<I,SeekWrite>(fs: &mut SeekWrite, 
                                                            pageManager: &IPages, 
                                                            source: I,
                                                           ) -> Result<(SegmentNum,PageNum)> where I:Iterator<Item=Result<kvp>>, SeekWrite : Seek+Write {

    fn writeOverflow<SeekWrite>(startingBlock: PageBlock, 
                                ba: &mut Read, 
                                pageManager: &IPages, 
                                fs: &mut SeekWrite
                               ) -> Result<(usize,PageBlock)> where SeekWrite : Seek+Write {

        fn buildFirstPage(ba: &mut Read, pbFirstOverflow : &mut PageBuilder, pgsz: usize) -> Result<(usize,bool)> {
            pbFirstOverflow.Reset();
            pbFirstOverflow.PutByte(PageType::OVERFLOW_NODE.to_u8());
            pbFirstOverflow.PutByte(0u8); // starts 0, may be changed later
            let room = pgsz - (2 + SIZE_32);
            // something will be put in lastInt32 later
            let put = try!(pbFirstOverflow.PutStream2(ba, room));
            Ok((put, put<room))
        };

        fn buildRegularPage(ba: &mut Read, pbOverflow : &mut PageBuilder, pgsz: usize) -> Result<(usize,bool)> {
            pbOverflow.Reset();
            let room = pgsz;
            let put = try!(pbOverflow.PutStream2(ba, room));
            Ok((put, put<room))
        };

        fn buildBoundaryPage(ba: &mut Read, pbOverflow : &mut PageBuilder, pgsz: usize) -> Result<(usize,bool)> {
            pbOverflow.Reset();
            let room = pgsz - SIZE_32;
            // something will be put in lastInt32 before the page is written
            let put = try!(pbOverflow.PutStream2(ba, room));
            Ok((put, put<room))
        }

        fn writeRegularPages<SeekWrite>(max: PageNum, 
                                        sofar: usize, 
                                        pb: &mut PageBuilder, 
                                        fs: &mut SeekWrite, 
                                        ba: &mut Read, 
                                        pgsz: usize
                                       ) -> Result<(PageNum,usize,bool)> where SeekWrite : Seek+Write {
            let mut i = 0;
            let mut sofar = sofar;
            loop {
                if i < max {
                    let (put, finished) = try!(buildRegularPage(ba, pb, pgsz));
                    if put==0 {
                        return Ok((i, sofar, true));
                    } else {
                        sofar = sofar + put;
                        try!(pb.Write(fs));
                        if finished {
                            return Ok((i+1, sofar, true));
                        } else {
                            i = i + 1;
                        }
                    }
                } else {
                    return Ok((i, sofar, false));
                }
            }
        }

        // TODO misnamed
        fn writeOneBlock<SeekWrite>(param_sofar: usize, 
                                    param_firstBlk: PageBlock,
                                    fs: &mut SeekWrite, 
                                    ba: &mut Read, 
                                    pgsz: usize,
                                    pbOverflow: &mut PageBuilder,
                                    pbFirstOverflow: &mut PageBuilder,
                                    pageManager: &IPages,
                                    token: &mut PendingSegment
                                   ) -> Result<(usize,PageBlock)> where SeekWrite : Seek+Write {
            // each trip through this loop will write out one
            // block, starting with the overflow first page,
            // followed by zero-or-more "regular" overflow pages,
            // which have no header.  we'll stop at the block boundary,
            // either because we land there or because the whole overflow
            // won't fit and we have to continue into the next block.
            // the boundary page will be like a regular overflow page,
            // headerless, but it is four bytes smaller.
            let mut loop_sofar = param_sofar;
            let mut loop_firstBlk = param_firstBlk;
            loop {
                let sofar = loop_sofar;
                let firstBlk = loop_firstBlk;
                let (putFirst,finished) = try!(buildFirstPage (ba, pbFirstOverflow, pgsz));
                if putFirst==0 { 
                    return Ok((sofar, firstBlk));
                } else {
                    // note that we haven't written the first page yet.  we may have to fix
                    // a couple of things before it gets written out.
                    let sofar = sofar + putFirst;
                    if firstBlk.firstPage == firstBlk.lastPage {
                        // the first page landed on a boundary.
                        // we can just set the flag and write it now.
                        pbFirstOverflow.SetPageFlag(PageFlag::FLAG_BOUNDARY_NODE);
                        let blk = try!(pageManager.GetBlock(&mut *token));
                        pbFirstOverflow.SetLastInt32(blk.firstPage);
                        try!(pbFirstOverflow.Write(fs));
                        try!(utils::SeekPage(fs, pgsz, blk.firstPage));
                        if !finished {
                            loop_sofar = sofar;
                            loop_firstBlk = blk;
                        } else {
                            return Ok((sofar, blk));
                        }
                    } else {
                        let firstRegularPageNumber = firstBlk.firstPage + 1;
                        if finished {
                            // the first page is also the last one
                            pbFirstOverflow.SetLastInt32(0); 
                            // offset to last used page in this block, which is this one
                            try!(pbFirstOverflow.Write(fs));
                            return Ok((sofar, PageBlock::new(firstRegularPageNumber,firstBlk.lastPage)));
                        } else {
                            // we need to write more pages,
                            // until the end of the block,
                            // or the end of the stream, 
                            // whichever comes first

                            try!(utils::SeekPage(fs, pgsz, firstRegularPageNumber));

                            // availableBeforeBoundary is the number of pages until the boundary,
                            // NOT counting the boundary page, and the first page in the block
                            // has already been accounted for, so we're just talking about data pages.
                            let availableBeforeBoundary = 
                                if firstBlk.lastPage > 0 
                                    { (firstBlk.lastPage - firstRegularPageNumber) }
                                else 
                                    { PageNum::max_value() }
                                ;

                            let (numRegularPages, sofar, finished) = 
                                try!(writeRegularPages(availableBeforeBoundary, sofar, pbOverflow, fs, ba, pgsz));

                            if finished {
                                // go back and fix the first page
                                pbFirstOverflow.SetLastInt32(numRegularPages);
                                try!(utils::SeekPage(fs, pgsz, firstBlk.firstPage));
                                try!(pbFirstOverflow.Write(fs));
                                // now reset to the next page in the block
                                let blk = PageBlock::new(firstRegularPageNumber + numRegularPages, firstBlk.lastPage);
                                try!(utils::SeekPage(fs, pgsz, blk.firstPage));
                                return Ok((sofar,blk));
                            } else {
                                // we need to write out a regular page except with a
                                // boundary pointer in it.  and we need to set
                                // FLAG_ENDS_ON_BOUNDARY on the first
                                // overflow page in this block.

                                let (putBoundary,finished) = try!(buildBoundaryPage (ba, pbOverflow, pgsz));
                                if putBoundary==0 {
                                    // go back and fix the first page
                                    pbFirstOverflow.SetLastInt32(numRegularPages);
                                    try!(utils::SeekPage(fs, pgsz, firstBlk.firstPage));
                                    try!(pbFirstOverflow.Write(fs));

                                    // now reset to the next page in the block
                                    let blk = PageBlock::new(firstRegularPageNumber + numRegularPages, firstBlk.lastPage);
                                    try!(utils::SeekPage(fs, pgsz, firstBlk.lastPage));
                                    return Ok((sofar,blk));
                                } else {
                                    // write the boundary page
                                    let sofar = sofar + putBoundary;
                                    let blk = try!(pageManager.GetBlock(&mut *token));
                                    pbOverflow.SetLastInt32(blk.firstPage);
                                    try!(pbOverflow.Write(fs));

                                    // go back and fix the first page
                                    pbFirstOverflow.SetPageFlag(PageFlag::FLAG_ENDS_ON_BOUNDARY);
                                    pbFirstOverflow.SetLastInt32(numRegularPages + 1);
                                    try!(utils::SeekPage(fs, pgsz, firstBlk.firstPage));
                                    try!(pbFirstOverflow.Write(fs));

                                    // now reset to the first page in the next block
                                    try!(utils::SeekPage(fs, pgsz, blk.firstPage));
                                    if finished {
                                        loop_sofar = sofar;
                                        loop_firstBlk = blk;
                                    } else {
                                        return Ok((sofar,blk));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let pgsz = pageManager.PageSize();
        let mut token = try!(pageManager.Begin());
        let mut pbFirstOverflow = PageBuilder::new(pgsz);
        let mut pbOverflow = PageBuilder::new(pgsz);

        writeOneBlock(0, startingBlock, fs, ba, pgsz, &mut pbOverflow, &mut pbFirstOverflow, pageManager, &mut token)
    }

    fn writeLeaves<I,SeekWrite>(leavesBlk:PageBlock,
                                pageManager: &IPages,
                                source: I,
                                vbuf: &mut [u8],
                                fs: &mut SeekWrite, 
                                pb: &mut PageBuilder,
                                token: &mut PendingSegment,
                                ) -> Result<(PageBlock,Vec<pgitem>,PageNum)> where I: Iterator<Item=Result<kvp>> , SeekWrite : Seek+Write {
        // 2 for the page type and flags
        // 4 for the prev page
        // 2 for the stored count
        // 4 for lastInt32 (which isn't in pb.Available)
        const LEAF_PAGE_OVERHEAD: usize = 2 + 4 + 2 + 4;

        fn buildLeaf(st: &mut LeafState, pb: &mut PageBuilder) -> Box<[u8]> {
            pb.Reset();
            pb.PutByte(PageType::LEAF_NODE.to_u8());
            pb.PutByte(0u8); // flags
            pb.PutInt32 (st.prevLeaf); // prev page num.
            // TODO prefixLen is one byte.  should it be two?
            pb.PutByte(st.prefixLen as u8);
            if st.prefixLen > 0 {
                pb.PutArray(&st.keys_in_this_leaf[0].key[0 .. st.prefixLen]);
            }
            let count_keys_in_this_leaf = st.keys_in_this_leaf.len();
            // TODO should we support more than 64k keys in a leaf?
            // either way, overflow-check this cast.
            pb.PutInt16 (count_keys_in_this_leaf as u16);

            fn f(pb: &mut PageBuilder, prefixLen: usize, lp: &LeafPair) {
                match lp.kLoc {
                    KeyLocation::Inline => {
                        pb.PutByte(0u8); // flags
                        pb.PutVarint(lp.key.len() as u64);
                        pb.PutArray(&lp.key[prefixLen .. lp.key.len()]);
                    },
                    KeyLocation::Overflowed(kpage) => {
                        pb.PutByte(ValueFlag::FLAG_OVERFLOW);
                        pb.PutVarint(lp.key.len() as u64);
                        pb.PutInt32(kpage);
                    },
                }
                match lp.vLoc {
                    ValueLocation::Tombstone => {
                        pb.PutByte(ValueFlag::FLAG_TOMBSTONE);
                    },
                    ValueLocation::Buffer (ref vbuf) => {
                        pb.PutByte(0u8);
                        pb.PutVarint(vbuf.len() as u64);
                        pb.PutArray(&vbuf);
                    },
                    ValueLocation::Overflowed (vlen,vpage) => {
                        pb.PutByte(ValueFlag::FLAG_OVERFLOW);
                        pb.PutVarint(vlen as u64);
                        pb.PutInt32(vpage);
                    },
                }
            }

            // deal with all the keys except the last one
            for lp in st.keys_in_this_leaf.drain(0 .. count_keys_in_this_leaf-1) {
                f(pb, st.prefixLen, &lp);
            }
            assert!(st.keys_in_this_leaf.len() == 1);

            let lp = st.keys_in_this_leaf.remove(0); 
            assert!(st.keys_in_this_leaf.is_empty());

            f(pb, st.prefixLen, &lp);
            lp.key
        }

        fn writeLeaf<SeekWrite>(st: &mut LeafState, 
                                isRootPage: bool, 
                                pb: &mut PageBuilder, 
                                fs: &mut SeekWrite, 
                                pgsz: usize,
                                pageManager: &IPages,
                                token: &mut PendingSegment,
                               ) -> Result<()> where SeekWrite : Seek+Write { 
            let last_key = buildLeaf(st, pb);
            assert!(st.keys_in_this_leaf.is_empty());
            let thisPageNumber = st.blk.firstPage;
            let firstLeaf = if st.leaves.is_empty() { thisPageNumber } else { st.firstLeaf };
            let nextBlk = 
                if isRootPage {
                    PageBlock::new(thisPageNumber + 1, st.blk.lastPage)
                } else if thisPageNumber == st.blk.lastPage {
                    pb.SetPageFlag(PageFlag::FLAG_BOUNDARY_NODE);
                    let newBlk = try!(pageManager.GetBlock(&mut *token));
                    pb.SetLastInt32(newBlk.firstPage);
                    newBlk
                } else {
                    PageBlock::new(thisPageNumber + 1, st.blk.lastPage)
                };
            try!(pb.Write(fs));
            if nextBlk.firstPage != (thisPageNumber+1) {
                try!(utils::SeekPage(fs, pgsz, nextBlk.firstPage));
            }
            let pg = pgitem {page:thisPageNumber, key:last_key};
            st.leaves.push(pg);
            st.sofarLeaf = 0;
            st.prevLeaf = thisPageNumber;
            st.prefixLen = 0;
            st.firstLeaf = firstLeaf;
            st.blk = nextBlk;
            Ok(())
        }

        // TODO can the overflow page number become a varint?
        const NEEDED_FOR_OVERFLOW_PAGE_NUMBER: usize = 4;

        // the max limit of an inline key is when that key is the only
        // one in the leaf, and its value is overflowed.

        let pgsz = pageManager.PageSize();
        let maxKeyInline = 
            pgsz 
            - LEAF_PAGE_OVERHEAD 
            - 1 // prefixLen
            - 1 // key flags
            - Varint::SpaceNeededFor(pgsz as u64) // approx worst case inline key len
            - 1 // value flags
            - 9 // worst case varint value len
            - NEEDED_FOR_OVERFLOW_PAGE_NUMBER; // overflowed value page

        fn kLocNeed(k: &[u8], kloc: &KeyLocation, prefixLen: usize) -> usize {
            let klen = k.len();
            match *kloc {
                KeyLocation::Inline => {
                    1 + Varint::SpaceNeededFor(klen as u64) + klen - prefixLen
                },
                KeyLocation::Overflowed(_) => {
                    1 + Varint::SpaceNeededFor(klen as u64) + NEEDED_FOR_OVERFLOW_PAGE_NUMBER
                },
            }
        }

        fn vLocNeed (vloc: &ValueLocation) -> usize {
            match *vloc {
                ValueLocation::Tombstone => {
                    1
                },
                ValueLocation::Buffer(ref vbuf) => {
                    let vlen = vbuf.len();
                    1 + Varint::SpaceNeededFor(vlen as u64) + vlen
                },
                ValueLocation::Overflowed(vlen,_) => {
                    1 + Varint::SpaceNeededFor(vlen as u64) + NEEDED_FOR_OVERFLOW_PAGE_NUMBER
                },
            }
        }

        fn leafPairSize(prefixLen: usize, lp: &LeafPair) -> usize {
            kLocNeed(&lp.key, &lp.kLoc, prefixLen)
            +
            vLocNeed(&lp.vLoc)
        }

        fn defaultPrefixLen(k: &[u8]) -> usize {
            // TODO max prefix.  relative to page size?  currently must fit in one byte.
            if k.len() > 255 { 255 } else { k.len() }
        }

        // this is the body of writeLeaves
        let mut st = LeafState {
            sofarLeaf: 0,
            firstLeaf: 0,
            prevLeaf: 0,
            keys_in_this_leaf:Vec::new(),
            prefixLen: 0,
            leaves:Vec::new(),
            blk:leavesBlk,
            };

        for result_pair in source {
            let mut pair = try!(result_pair);
            let k = pair.Key;

            // TODO is it possible for this to conclude that the key must be overflowed
            // when it would actually fit because of prefixing?

            let (blkAfterKey,kloc) = 
                if k.len() <= maxKeyInline {
                    (st.blk, KeyLocation::Inline)
                } else {
                    let vPage = st.blk.firstPage;
                    let (_,newBlk) = try!(writeOverflow(st.blk, &mut &*k, pageManager, fs));
                    (newBlk, KeyLocation::Overflowed(vPage))
                };

            // the max limit of an inline value is when the key is inline
            // on a new page.

            // TODO this is a usize, so it might cause integer underflow.
            let availableOnNewPageAfterKey = 
                pgsz 
                - LEAF_PAGE_OVERHEAD 
                - 1 // prefixLen
                - 1 // key flags
                - Varint::SpaceNeededFor(k.len() as u64)
                - k.len() 
                - 1 // value flags
                ;

            // availableOnNewPageAfterKey needs to accomodate the value and its length as a varint.
            // it might already be <=0 because of the key length

            let maxValueInline = 
                if availableOnNewPageAfterKey > 0 {
                    let neededForVarintLen = Varint::SpaceNeededFor(availableOnNewPageAfterKey as u64);
                    let avail2 = availableOnNewPageAfterKey - neededForVarintLen;
                    if avail2 > 0 { avail2 } else { 0 }
                } else {
                    0
                };

            let (blkAfterValue, vloc) = 
                match pair.Value {
                    Blob::Tombstone => {
                        (blkAfterKey, ValueLocation::Tombstone)
                    },
                    _ => match kloc {
                         KeyLocation::Inline => {
                            if maxValueInline == 0 {
                                match pair.Value {
                                    Blob::Tombstone => {
                                        (blkAfterKey, ValueLocation::Tombstone)
                                    },
                                    Blob::Stream(ref mut strm) => {
                                        let valuePage = blkAfterKey.firstPage;
                                        let (len,newBlk) = try!(writeOverflow(blkAfterKey, &mut *strm, pageManager, fs));
                                        (newBlk, ValueLocation::Overflowed(len,valuePage))
                                    },
                                    Blob::Array(a) => {
                                        if a.is_empty() {
                                            // TODO maybe we need ValueLocation::Empty
                                            (blkAfterKey, ValueLocation::Buffer(a))
                                        } else {
                                            let valuePage = blkAfterKey.firstPage;
                                            let strm = a; // TODO need a Read for this
                                            let (len,newBlk) = try!(writeOverflow(blkAfterKey, &mut &*strm, pageManager, fs));
                                            (newBlk, ValueLocation::Overflowed(len,valuePage))
                                        }
                                    },
                                }
                            } else {
                                match pair.Value {
                                    Blob::Tombstone => {
                                        (blkAfterKey, ValueLocation::Tombstone)
                                    },
                                    Blob::Stream(ref mut strm) => {
                                        // not sure reusing vbuf is worth it.  maybe we should just
                                        // alloc here.  ownership will get passed into the
                                        // ValueLocation when it fits.
                                        let vread = try!(utils::ReadFully(&mut *strm, &mut vbuf[0 .. maxValueInline+1]));
                                        let vbuf = &vbuf[0 .. vread];
                                        if vread < maxValueInline {
                                            // TODO this alloc+copy is unfortunate
                                            let mut va = Vec::with_capacity(vbuf.len());
                                            for i in 0 .. vbuf.len() {
                                                va.push(vbuf[i]);
                                            }
                                            (blkAfterKey, ValueLocation::Buffer(va.into_boxed_slice()))
                                        } else {
                                            let valuePage = blkAfterKey.firstPage;
                                            let (len,newBlk) = try!(writeOverflow(blkAfterKey, &mut (vbuf.chain(strm)), pageManager, fs));
                                            (newBlk, ValueLocation::Overflowed (len,valuePage))
                                        }
                                    },
                                    Blob::Array(a) => {
                                        if a.len() < maxValueInline {
                                            (blkAfterKey, ValueLocation::Buffer(a))
                                        } else {
                                            let valuePage = blkAfterKey.firstPage;
                                            let (len,newBlk) = try!(writeOverflow(blkAfterKey, &mut &*a, pageManager, fs));
                                            (newBlk, ValueLocation::Overflowed(len,valuePage))
                                        }
                                    },
                                }
                            }
                         },

                         KeyLocation::Overflowed(_) => {
                            match pair.Value {
                                Blob::Tombstone => {
                                    (blkAfterKey, ValueLocation::Tombstone)
                                },
                                Blob::Stream(ref mut strm) => {
                                    let valuePage = blkAfterKey.firstPage;
                                    let (len,newBlk) = try!(writeOverflow(blkAfterKey, &mut *strm, pageManager, fs));
                                    (newBlk, ValueLocation::Overflowed(len,valuePage))
                                },
                                Blob::Array(a) => {
                                    if a.is_empty() {
                                        // TODO maybe we need ValueLocation::Empty
                                        (blkAfterKey, ValueLocation::Buffer(a))
                                    } else {
                                        let valuePage = blkAfterKey.firstPage;
                                        let (len,newBlk) = try!(writeOverflow(blkAfterKey, &mut &*a, pageManager, fs));
                                        (newBlk, ValueLocation::Overflowed(len,valuePage))
                                    }
                                }
                            }
                         }
                    }
            };

            // whether/not the key/value are to be overflowed is now already decided.
            // now all we have to do is decide if this key/value are going into this leaf
            // or not.  note that it is possible to overflow these and then have them not
            // fit into the current leaf and end up landing in the next leaf.

            st.blk = blkAfterValue;

            // TODO ignore prefixLen for overflowed keys?
            let newPrefixLen = 
                if st.keys_in_this_leaf.is_empty() {
                    defaultPrefixLen(&k)
                } else {
                    bcmp::PrefixMatch(&*st.keys_in_this_leaf[0].key, &k, st.prefixLen)
                };
            let sofar = 
                if newPrefixLen < st.prefixLen {
                    // the prefixLen would change with the addition of this key,
                    // so we need to recalc sofar
                    let sum = st.keys_in_this_leaf.iter().map(|lp| leafPairSize(newPrefixLen, lp)).sum();;
                    sum
                } else {
                    st.sofarLeaf
                };
            let fit = {
                let needed = kLocNeed(&k, &kloc, newPrefixLen) + vLocNeed(&vloc);
                let used = sofar + LEAF_PAGE_OVERHEAD + 1 + newPrefixLen;
                if pgsz > used {
                    let available = pgsz - used;
                    (available >= needed)
                } else {
                    false
                }
            };
            let writeThisPage = (! st.keys_in_this_leaf.is_empty()) && (! fit);

            if writeThisPage {
                try!(writeLeaf(&mut st, false, pb, fs, pgsz, pageManager, &mut *token));
            }

            // TODO ignore prefixLen for overflowed keys?
            let newPrefixLen = 
                if st.keys_in_this_leaf.is_empty() {
                    defaultPrefixLen(&k)
                } else {
                    bcmp::PrefixMatch(&*st.keys_in_this_leaf[0].key, &k, st.prefixLen)
                };
            let sofar = 
                if newPrefixLen < st.prefixLen {
                    // the prefixLen will change with the addition of this key,
                    // so we need to recalc sofar
                    let sum = st.keys_in_this_leaf.iter().map(|lp| leafPairSize(newPrefixLen, lp)).sum();;
                    sum
                } else {
                    st.sofarLeaf
                };
            // note that the LeafPair struct gets ownership of the key provided
            // from above.
            let lp = LeafPair {
                        key:k,
                        kLoc:kloc,
                        vLoc:vloc,
                        };

            st.sofarLeaf=sofar + leafPairSize(newPrefixLen, &lp);
            st.keys_in_this_leaf.push(lp);
            st.prefixLen=newPrefixLen;
        }

        if !st.keys_in_this_leaf.is_empty() {
            let isRootNode = st.leaves.is_empty();
            try!(writeLeaf(&mut st, isRootNode, pb, fs, pgsz, pageManager, &mut *token));
        }
        Ok((st.blk,st.leaves,st.firstLeaf))
    }

    fn writeParentNodes<SeekWrite>(startingBlk: PageBlock, 
                                   children: &mut Vec<pgitem>,
                                   pgsz: usize,
                                   fs: &mut SeekWrite,
                                   pageManager: &IPages,
                                   token: &mut PendingSegment,
                                   lastLeaf: PageNum,
                                   firstLeaf: PageNum,
                                   pb: &mut PageBuilder,
                                  ) -> Result<(PageBlock, Vec<pgitem>)> where SeekWrite : Seek+Write {
        // 2 for the page type and flags
        // 2 for the stored count
        // 5 for the extra ptr we will add at the end, a varint, 5 is worst case (page num < 4294967295L)
        // 4 for lastInt32
        const PARENT_PAGE_OVERHEAD: usize = 2 + 2 + 5 + 4;

        fn calcAvailable(currentSize: usize, couldBeRoot: bool, pgsz: usize) -> usize {
            let basicSize = pgsz - currentSize;
            let allowanceForRootNode = if couldBeRoot { SIZE_32 } else { 0 }; // first/last Leaf, lastInt32 already
            // TODO can this cause integer overflow?
            basicSize - allowanceForRootNode
        }

        fn buildParentPage(items: &mut Vec<pgitem>,
                           lastPtr: PageNum, 
                           overflows: &HashMap<usize,PageNum>,
                           pb : &mut PageBuilder,
                          ) {
            pb.Reset();
            pb.PutByte(PageType::PARENT_NODE.to_u8());
            pb.PutByte(0u8);
            pb.PutInt16(items.len() as u16);
            // store all the ptrs, n+1 of them
            for x in items.iter() {
                pb.PutVarint(x.page as u64);
            }
            pb.PutVarint(lastPtr as u64);
            // store all the keys, n of them
            for (i,x) in items.drain(..).enumerate() {
                match overflows.get(&i) {
                    Some(pg) => {
                        pb.PutByte(ValueFlag::FLAG_OVERFLOW);
                        pb.PutVarint(x.key.len() as u64);
                        pb.PutInt32(*pg as PageNum);
                    },
                    None => {
                        pb.PutByte(0u8);
                        pb.PutVarint(x.key.len() as u64);
                        pb.PutArray(&x.key);
                    },
                }
            }
        }

        fn writeParentPage<SeekWrite>(st: &mut ParentState, 
                                      items: &mut Vec<pgitem>,
                                      overflows: &HashMap<usize,PageNum>,
                                      pgnum: PageNum,
                                      key: Box<[u8]>,
                                      isRootNode: bool, 
                                      pb: &mut PageBuilder, 
                                      lastLeaf: PageNum,
                                      fs: &mut SeekWrite,
                                      pageManager: &IPages,
                                      pgsz: usize,
                                      token: &mut PendingSegment,
                                      firstLeaf: PageNum,
                                     ) -> Result<()> where SeekWrite : Seek+Write {
            // assert st.sofar > 0
            let thisPageNumber = st.blk.firstPage;
            buildParentPage(items, pgnum, &overflows, pb);
            let nextBlk =
                if isRootNode {
                    pb.SetPageFlag(PageFlag::FLAG_ROOT_NODE);
                    pb.SetSecondToLastInt32(firstLeaf);
                    pb.SetLastInt32(lastLeaf);
                    PageBlock::new(thisPageNumber+1,st.blk.lastPage)
                } else {
                    if st.blk.firstPage == st.blk.lastPage {
                        pb.SetPageFlag(PageFlag::FLAG_BOUNDARY_NODE);
                        let newBlk = try!(pageManager.GetBlock(&mut *token));
                        pb.SetLastInt32(newBlk.firstPage);
                        newBlk
                    } else {
                        PageBlock::new(thisPageNumber+1,st.blk.lastPage)
                    }
                };
            try!(pb.Write(fs));
            if nextBlk.firstPage != (thisPageNumber+1) {
                try!(utils::SeekPage(fs, pgsz, nextBlk.firstPage));
            }
            st.sofar = 0;
            st.blk = nextBlk;
            let pg = pgitem {page:thisPageNumber, key:key};
            st.nextGeneration.push(pg);
            Ok(())
        }

        // this is the body of writeParentNodes
        let mut st = ParentState {nextGeneration:Vec::new(),sofar: 0,blk:startingBlk,};
        let mut items = Vec::new();
        let mut overflows = HashMap::new();
        let count_children = children.len();
        // deal with all the children except the last one
        for pair in children.drain(0 .. count_children-1) {
            let pgnum = pair.page;

            let neededEitherWay = 1 + Varint::SpaceNeededFor(pair.key.len() as u64) + Varint::SpaceNeededFor(pgnum as u64);
            let neededForInline = neededEitherWay + pair.key.len();
            let neededForOverflow = neededEitherWay + SIZE_32;
            let couldBeRoot = st.nextGeneration.is_empty();

            let available = calcAvailable(st.sofar, couldBeRoot, pgsz);
            let fitsInline = available >= neededForInline;
            let wouldFitInlineOnNextPage = (pgsz - PARENT_PAGE_OVERHEAD) >= neededForInline;
            let fitsOverflow = available >= neededForOverflow;
            let writeThisPage = (! fitsInline) && (wouldFitInlineOnNextPage || (! fitsOverflow));

            if writeThisPage {
                // assert sofar > 0
                // we need to make a copy of this key because writeParentPage needs to own one,
                // but we still need to put this pair in the items (below).
                let mut copy_key = vec![0; pair.key.len()].into_boxed_slice(); 
                copy_key.clone_from_slice(&pair.key);
                try!(writeParentPage(&mut st, &mut items, &overflows, pair.page, copy_key, false, pb, lastLeaf, fs, pageManager, pgsz, &mut *token, firstLeaf));
                assert!(items.is_empty());
            }

            if st.sofar == 0 {
                st.sofar = PARENT_PAGE_OVERHEAD;
                assert!(items.is_empty());
            }

            if calcAvailable(st.sofar, st.nextGeneration.is_empty(), pgsz) >= neededForInline {
                st.sofar = st.sofar + neededForInline;
            } else {
                let keyOverflowFirstPage = st.blk.firstPage;
                let (_,newBlk) = try!(writeOverflow(st.blk, &mut &*pair.key, pageManager, fs));
                st.sofar = st.sofar + neededForOverflow;
                st.blk = newBlk;
                // items.len() is the index that this pair is about to get, just below
                overflows.insert(items.len(),keyOverflowFirstPage);
            }
            items.push(pair);
        }
        assert!(children.len() == 1);
        let isRootNode = st.nextGeneration.is_empty();
        let pgitem {page: pgnum, key: key} = children.remove(0);
        assert!(children.is_empty());

        try!(writeParentPage(&mut st, &mut items, &overflows, pgnum, key, isRootNode, pb, lastLeaf, fs, pageManager, pgsz, &mut *token, firstLeaf));
        Ok((st.blk,st.nextGeneration))
    }

    // this is the body of Create
    let pgsz = pageManager.PageSize();
    let mut pb = PageBuilder::new(pgsz);
    let mut token = try!(pageManager.Begin());
    let startingBlk = try!(pageManager.GetBlock(&mut token));
    try!(utils::SeekPage(fs, pgsz, startingBlk.firstPage));

    // TODO this is a buffer just for the purpose of being reused
    // in cases where the blob is provided as a stream, and we need
    // read a bit of it to figure out if it might fit inline rather
    // than overflow.
    let mut vbuf = vec![0;pgsz].into_boxed_slice(); 
    let (blkAfterLeaves, leaves, firstLeaf) = try!(writeLeaves(startingBlk, pageManager, source, &mut vbuf, fs, &mut pb, &mut token));

    // all the leaves are written.
    // now write the parent pages.
    // maybe more than one level of them.
    // keep writing until we have written a level which has only one node,
    // which is the root node.

    let lastLeaf = leaves[leaves.len()-1].page;

    let rootPage = {
        let mut blk = blkAfterLeaves;
        let mut children = leaves;
        loop {
            let (newBlk, newChildren) = try!(writeParentNodes(blk, &mut children, pgsz, fs, pageManager, &mut token, lastLeaf, firstLeaf, &mut pb));
            assert!(children.is_empty());
            blk = newBlk;
            children = newChildren;
            if children.len()==1 {
                break;
            }
        }
        children[0].page
    };

    let g = try!(pageManager.End(token, rootPage));
    Ok((g,rootPage))
}

struct myOverflowReadStream {
    fs: File,
    len: usize, // same type as ValueLength(), max len of a single value
    firstPage: PageNum, // TODO will be needed later for Seek trait
    buf: Box<[u8]>,
    currentPage: PageNum,
    sofarOverall: usize,
    sofarThisPage: usize,
    firstPageInBlock: PageNum,
    offsetToLastPageInThisBlock: PageNum,
    countRegularDataPagesInBlock: PageNum,
    boundaryPageNumber: PageNum,
    bytesOnThisPage: usize,
    offsetOnThisPage: usize,
}
    
impl myOverflowReadStream {
    fn new(path: &str, pgsz: usize, firstPage: PageNum, len: usize) -> Result<myOverflowReadStream> {
        // TODO I wonder if maybe we should defer the opening of the file until
        // somebody actually tries to read from it?  so that constructing a
        // ValueRef object (which contains one of these) would be a lighter-weight
        // operation.
        let f = try!(OpenOptions::new()
                .read(true)
                .open(path));
        let mut res = 
            myOverflowReadStream {
                fs: f,
                len: len,
                firstPage: firstPage,
                buf: vec![0;pgsz].into_boxed_slice(),
                currentPage: firstPage,
                sofarOverall: 0,
                sofarThisPage: 0,
                firstPageInBlock: 0,
                offsetToLastPageInThisBlock: 0, // add to firstPageInBlock to get the last one
                countRegularDataPagesInBlock: 0,
                boundaryPageNumber: 0,
                bytesOnThisPage: 0,
                offsetOnThisPage: 0,
            };
        try!(res.ReadFirstPage());
        Ok(res)
    }

    fn len(&self) -> usize {
        self.len
    }

    // TODO consider supporting Seek trait

    fn ReadPage(&mut self) -> Result<()> {
        try!(utils::SeekPage(&mut self.fs, self.buf.len(), self.currentPage));
        try!(utils::ReadFully(&mut self.fs, &mut *self.buf));
        // assert PageType is OVERFLOW
        self.sofarThisPage = 0;
        if self.currentPage == self.firstPageInBlock {
            self.bytesOnThisPage = self.buf.len() - (2 + SIZE_32);
            self.offsetOnThisPage = 2;
        } else if self.currentPage == self.boundaryPageNumber {
            self.bytesOnThisPage = self.buf.len() - SIZE_32;
            self.offsetOnThisPage = 0;
        } else {
            // assert currentPage > firstPageInBlock
            // assert currentPage < boundaryPageNumber OR boundaryPageNumber = 0
            self.bytesOnThisPage = self.buf.len();
            self.offsetOnThisPage = 0;
        }
        Ok(())
    }

    fn GetLastInt32(&self) -> u32 {
        let at = self.buf.len() - SIZE_32;
        read_u32_be(&self.buf[at .. at+4])
    }

    fn PageType(&self) -> Result<PageType> {
        PageType::from_u8(self.buf[0])
    }

    fn CheckPageFlag(&self, f: u8) -> bool {
        0 != (self.buf[1] & f)
    }

    fn ReadFirstPage(&mut self) -> Result<()> {
        self.firstPageInBlock = self.currentPage;
        try!(self.ReadPage());
        if try!(self.PageType()) != (PageType::OVERFLOW_NODE) {
            return Err(LsmError::CorruptFile("first overflow page has invalid page type"));
        }
        if self.CheckPageFlag(PageFlag::FLAG_BOUNDARY_NODE) {
            // first page landed on a boundary node
            // lastInt32 is the next page number, which we'll fetch later
            self.boundaryPageNumber = self.currentPage;
            self.offsetToLastPageInThisBlock = 0;
            self.countRegularDataPagesInBlock = 0;
        } else {
            self.offsetToLastPageInThisBlock = self.GetLastInt32();
            if self.CheckPageFlag(PageFlag::FLAG_ENDS_ON_BOUNDARY) {
                self.boundaryPageNumber = self.currentPage + self.offsetToLastPageInThisBlock;
                self.countRegularDataPagesInBlock = self.offsetToLastPageInThisBlock - 1;
            } else {
                self.boundaryPageNumber = 0;
                self.countRegularDataPagesInBlock = self.offsetToLastPageInThisBlock;
            }
        }
        Ok(())
    }

    fn Read(&mut self, ba: &mut [u8], offset: usize, wanted: usize) -> Result<usize> {
        if self.sofarOverall >= self.len {
            Ok(0)
        } else {
            let mut direct = false;
            if self.sofarThisPage >= self.bytesOnThisPage {
                if self.currentPage == self.boundaryPageNumber {
                    self.currentPage = self.GetLastInt32();
                    try!(self.ReadFirstPage());
                } else {
                    // we need a new page.  and if it's a full data page,
                    // and if wanted is big enough to take all of it, then
                    // we want to read (at least) it directly into the
                    // buffer provided by the caller.  we already know
                    // this candidate page cannot be the first page in a
                    // block.
                    let maybeDataPage = self.currentPage + 1;
                    let isDataPage = 
                        if self.boundaryPageNumber > 0 {
                            ((self.len - self.sofarOverall) >= self.buf.len()) && (self.countRegularDataPagesInBlock > 0) && (maybeDataPage > self.firstPageInBlock) && (maybeDataPage < self.boundaryPageNumber)
                        } else {
                            ((self.len - self.sofarOverall) >= self.buf.len()) && (self.countRegularDataPagesInBlock > 0) && (maybeDataPage > self.firstPageInBlock) && (maybeDataPage <= (self.firstPageInBlock + self.countRegularDataPagesInBlock))
                        };

                    if isDataPage && (wanted >= self.buf.len()) {
                        // assert (currentPage + 1) > firstPageInBlock
                        //
                        // don't increment currentPage here because below, we will
                        // calculate how many pages we actually want to do.
                        direct = true;
                        self.bytesOnThisPage = self.buf.len();
                        self.sofarThisPage = 0;
                        self.offsetOnThisPage = 0;
                    } else {
                        self.currentPage = self.currentPage + 1;
                        try!(self.ReadPage());
                    }
                }
            }

            if direct {
                // currentPage has not been incremented yet
                //
                // skip the buffer.  note, therefore, that the contents of the
                // buffer are "invalid" in that they do not correspond to currentPage
                //
                let numPagesWanted = (wanted / self.buf.len()) as PageNum;
                // assert countRegularDataPagesInBlock > 0
                let lastDataPageInThisBlock = self.firstPageInBlock + self.countRegularDataPagesInBlock;
                let theDataPage = self.currentPage + 1;
                let numPagesAvailable = 
                    if self.boundaryPageNumber>0 { 
                        self.boundaryPageNumber - theDataPage 
                    } else {
                        lastDataPageInThisBlock - theDataPage + 1
                    };
                let numPagesToFetch = std::cmp::min(numPagesWanted, numPagesAvailable) as PageNum;
                let bytesToFetch = {
                    let bytesToFetch = (numPagesToFetch as usize) * self.buf.len();
                    let available = self.len - self.sofarOverall;
                    if bytesToFetch > available {
                        available
                    } else {
                        bytesToFetch
                    }
                };
                // assert bytesToFetch <= wanted

                try!(utils::SeekPage(&mut self.fs, self.buf.len(), theDataPage));
                try!(utils::ReadFully(&mut self.fs, &mut ba[offset .. offset + bytesToFetch]));
                self.sofarOverall = self.sofarOverall + bytesToFetch;
                self.currentPage = self.currentPage + numPagesToFetch;
                self.sofarThisPage = self.buf.len();
                Ok(bytesToFetch)
            } else {
                let available = std::cmp::min(self.bytesOnThisPage - self.sofarThisPage, self.len - self.sofarOverall);
                let num = std::cmp::min(available, wanted);
                for i in 0 .. num {
                    ba[offset+i] = self.buf[self.offsetOnThisPage + self.sofarThisPage + i];
                }
                self.sofarOverall = self.sofarOverall + num;
                self.sofarThisPage = self.sofarThisPage + num;
                Ok(num)
            }
        }
    }
}

impl Read for myOverflowReadStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = buf.len();
        match self.Read(buf, 0, len) {
            Ok(v) => Ok(v),
            Err(e) => {
                // this interface requires io::Result, so we shoehorn the others into it
                match e {
                    LsmError::Io(e) => Err(e),
                    _ => Err(std::io::Error::new(std::io::ErrorKind::Other, e.description())),
                }
            },
        }
    }
}

#[cfg(remove_me)]
fn readOverflow(path: &str, pgsz: usize, firstPage: PageNum, buf: &mut [u8]) -> Result<usize> {
    let mut ostrm = try!(myOverflowReadStream::new(path, pgsz, firstPage, buf.len()));
    let res = try!(utils::ReadFully(&mut ostrm, buf));
    Ok(res)
}

struct SegmentCursor<'a> {
    path: String,

    // TODO in the f# version, these three were a closure.
    // it would be nice to make it work that way again.
    // so that this code would not have specific knowledge
    // of the InnerPart type.
    inner: &'a InnerPart,
    segnum: SegmentNum,
    csrnum: u64,

    blocks: Vec<PageBlock>, // TODO will be needed later for stray checking
    fs: File,
    len: u64,
    rootPage: PageNum,
    pr: PageBuffer,
    currentPage: PageNum,
    leafKeys: Vec<usize>,
    previousLeaf: PageNum,
    currentKey: Option<usize>,
    prefix: Option<Box<[u8]>>,
    firstLeaf: PageNum,
    lastLeaf: PageNum,
}

impl<'a> SegmentCursor<'a> {
    fn new(path: &str, 
           pgsz: usize, 
           rootPage: PageNum, 
           blocks: Vec<PageBlock>,
           inner: &'a InnerPart, 
           segnum: SegmentNum, 
           csrnum: u64
          ) -> Result<SegmentCursor<'a>> {

        // TODO consider not passsing in the path, and instead,
        // making the cursor call back to inner.OpenForReading...
        let mut f = try!(OpenOptions::new()
                .read(true)
                .open(path));

        // TODO the len is used for checking to make sure we don't stray
        // to far.  This should probably be done with the blocks provided
        // by the caller, not by looking at the full length of the file,
        // which this cursor shouldn't care about.
        let len = try!(seek_len(&mut f));

        let mut res = SegmentCursor {
            path: String::from_str(path),
            fs: f,
            blocks: blocks,
            inner: inner,
            segnum: segnum,
            csrnum: csrnum,
            len: len,
            rootPage: rootPage,
            pr: PageBuffer::new(pgsz),
            currentPage: 0,
            leafKeys: Vec::new(),
            previousLeaf: 0,
            currentKey: None,
            prefix: None,
            firstLeaf: 0, // temporary
            lastLeaf: 0, // temporary
        };
        if ! try!(res.setCurrentPage(rootPage)) {
            // TODO fix this error.  or assert, because we previously verified
            // that the root page was in the block list we were given.
            return Err(LsmError::Misc("failed to read root page"));
        }
        let pt = try!(res.pr.PageType());
        if pt == PageType::LEAF_NODE {
            res.firstLeaf = rootPage;
            res.lastLeaf = rootPage;
        } else if pt == PageType::PARENT_NODE {
            if ! res.pr.CheckPageFlag(PageFlag::FLAG_ROOT_NODE) { 
                return Err(LsmError::CorruptFile("root page lacks flag"));
            }
            res.firstLeaf = res.pr.GetSecondToLastInt32() as PageNum;
            res.lastLeaf = res.pr.GetLastInt32() as PageNum;
        } else {
            return Err(LsmError::CorruptFile("root page has invalid page type"));
        }
          
        Ok(res)
    }

    fn resetLeaf(&mut self) {
        self.leafKeys.clear();
        self.previousLeaf = 0;
        self.currentKey = None;
        self.prefix = None;
    }

    fn setCurrentPage(&mut self, pgnum: PageNum) -> Result<bool> {
        // TODO use self.blocks to make sure we are not straying out of bounds.

        // TODO so I think this function actually should be Result<()>.
        // it used to return Ok(false) in situations that I think should
        // actually have been errors.  not 100% sure yet.  still trying
        // to verify all the cases.

        // TODO if currentPage = pgnum already...
        self.currentPage = pgnum;
        self.resetLeaf();
        if 0 == self.currentPage { 
            Err(LsmError::InvalidPageNumber)
            //Ok(false)
        } else {
            // refuse to go to a page beyond the end of the stream
            // TODO is this the right place for this check?    
            let pos = (self.currentPage - 1) as u64 * self.pr.PageSize() as u64;
            if pos + self.pr.PageSize() as u64 <= self.len {
                try!(utils::SeekPage(&mut self.fs, self.pr.PageSize(), self.currentPage));
                try!(self.pr.Read(&mut self.fs));
                Ok(true)
            } else {
                Err(LsmError::InvalidPageNumber)
                //Ok(false)
            }
        }
    }

    fn nextInLeaf(&mut self) -> bool {
        match self.currentKey {
            Some(cur) => {
                if (cur+1) < self.leafKeys.len() {
                    self.currentKey = Some(cur + 1);
                    true
                } else {
                    false
                }
            },
            None => {
                false
            },
        }
    }

    fn prevInLeaf(&mut self) -> bool {
        match self.currentKey {
            Some(cur) => {
                if cur > 0 {
                    self.currentKey = Some(cur - 1);
                    true
                } else {
                    false
                }
            },
            None => {
                false
            },
        }
    }

    fn skipKey(&self, cur: &mut usize) {
        let kflag = self.pr.GetByte(cur);
        let klen = self.pr.GetVarint(cur) as usize;
        if 0 == (kflag & ValueFlag::FLAG_OVERFLOW) {
            let prefixLen = match self.prefix {
                Some(ref a) => a.len(),
                None => 0
            };
            *cur = *cur + (klen - prefixLen);
        } else {
            *cur = *cur + SIZE_32;
        }
    }

    fn skipValue(&self, cur: &mut usize) {
        let vflag = self.pr.GetByte(cur);
        if 0 != (vflag & ValueFlag::FLAG_TOMBSTONE) { 
            ()
        } else {
            let vlen = self.pr.GetVarint(cur) as usize;
            if 0 != (vflag & ValueFlag::FLAG_OVERFLOW) {
                *cur = *cur + SIZE_32;
            }
            else {
                *cur = *cur + vlen;
            }
        }
    }

    fn readLeaf(&mut self) -> Result<()> {
        self.resetLeaf();
        let mut cur = 0;
        let pt = try!(PageType::from_u8(self.pr.GetByte(&mut cur)));
        if pt != PageType::LEAF_NODE {
            return Err(LsmError::CorruptFile("leaf has invalid page type"));
        }
        self.pr.GetByte(&mut cur);
        self.previousLeaf = self.pr.GetInt32(&mut cur) as PageNum;
        let prefixLen = self.pr.GetByte(&mut cur) as usize;
        if prefixLen > 0 {
            // TODO should we just remember prefix as a reference instead of box/copy?
            let mut a = vec![0;prefixLen].into_boxed_slice();
            self.pr.GetIntoArray(&mut cur, &mut a);
            self.prefix = Some(a);
        } else {
            self.prefix = None;
        }
        let countLeafKeys = self.pr.GetInt16(&mut cur) as usize;
        // assert countLeafKeys>0
        // TODO might need to extend capacity here, not just truncate
        self.leafKeys.truncate(countLeafKeys);
        while self.leafKeys.len() < countLeafKeys {
            self.leafKeys.push(0);
        }
        for i in 0 .. countLeafKeys {
            self.leafKeys[i] = cur;
            self.skipKey(&mut cur);
            self.skipValue(&mut cur);
        }
        Ok(())
    }

    fn keyInLeaf2(&'a self, n: usize) -> Result<KeyRef<'a>> { 
        let mut cur = self.leafKeys[n as usize];
        let kflag = self.pr.GetByte(&mut cur);
        let klen = self.pr.GetVarint(&mut cur) as usize;
        if 0 == (kflag & ValueFlag::FLAG_OVERFLOW) {
            match self.prefix {
                Some(ref a) => {
                    Ok(KeyRef::Prefixed(&a, self.pr.get_slice(cur, klen - a.len())))
                },
                None => {
                    Ok(KeyRef::Array(self.pr.get_slice(cur, klen)))
                },
            }
        } else {
            let pgnum = self.pr.GetInt32(&mut cur) as PageNum;
            let mut ostrm = try!(myOverflowReadStream::new(&self.path, self.pr.PageSize(), pgnum, klen));
            let mut x_k = Vec::with_capacity(klen);
            try!(ostrm.read_to_end(&mut x_k));
            let x_k = x_k.into_boxed_slice();
            Ok(KeyRef::Overflowed(x_k))
        }
    }

    #[cfg(remove_me)]
    fn keyInLeaf(&self, n: usize) -> Result<Box<[u8]>> { 
        let mut cur = self.leafKeys[n as usize];
        let kflag = self.pr.GetByte(&mut cur);
        let klen = self.pr.GetVarint(&mut cur) as usize;
        let mut res = vec![0;klen].into_boxed_slice();
        if 0 == (kflag & ValueFlag::FLAG_OVERFLOW) {
            match self.prefix {
                Some(ref a) => {
                    let prefixLen = a.len();
                    for i in 0 .. prefixLen {
                        res[i] = a[i];
                    }
                    self.pr.GetIntoArray(&mut cur, &mut res[prefixLen .. klen]);
                    Ok(res)
                },
                None => {
                    self.pr.GetIntoArray(&mut cur, &mut res);
                    Ok(res)
                },
            }
        } else {
            let pgnum = self.pr.GetInt32(&mut cur) as PageNum;
            try!(readOverflow(&self.path, self.pr.PageSize(), pgnum, &mut res));
            Ok(res)
        }
    }

    #[cfg(remove_me)]
    fn compareKeyInLeaf(&self, n: usize, other: &[u8]) -> Result<Ordering> {
        let mut cur = self.leafKeys[n as usize];
        let kflag = self.pr.GetByte(&mut cur);
        let klen = self.pr.GetVarint(&mut cur) as usize;
        if 0 == (kflag & ValueFlag::FLAG_OVERFLOW) {
            let res = 
                match self.prefix {
                    Some(ref a) => {
                        self.pr.CompareWithPrefix(cur, a, klen, other)
                    },
                    None => {
                        self.pr.Compare(cur, klen, other)
                    },
                };
            Ok(res)
        } else {
            // TODO this could be more efficient. we could compare the key
            // in place in the overflow without fetching the entire thing.

            // TODO overflowed keys are not prefixed.  should they be?
            let pgnum = self.pr.GetInt32(&mut cur) as PageNum;
            let mut k = vec![0;klen].into_boxed_slice();
            try!(readOverflow(&self.path, self.pr.PageSize(), pgnum, &mut k));
            let res = bcmp::Compare(&*k, other);
            Ok(res)
        }
    }

    #[cfg(remove_me)]
    fn compare_two(x: &SegmentCursor, y: &SegmentCursor) -> Result<Ordering> {
        fn get_info(c: &SegmentCursor) -> Result<(usize, bool, usize, usize)> {
            match c.currentKey {
                None => Err(LsmError::CursorNotValid),
                Some(n) => {
                    let mut cur = c.leafKeys[n as usize];
                    let kflag = c.pr.GetByte(&mut cur);
                    let klen = c.pr.GetVarint(&mut cur) as usize;
                    let overflowed = 0 != (kflag & ValueFlag::FLAG_OVERFLOW);
                    Ok((n, overflowed, cur, klen))
                },
            }
        }

        let (x_n, x_over, x_cur, x_klen) = try!(get_info(x));
        let (y_n, y_over, y_cur, y_klen) = try!(get_info(y));

        if x_over || y_over {
            // if either of these keys is overflowed, don't bother
            // trying to do anything clever.  just read both keys
            // into memory and compare them.
            let x_k = try!(x.keyInLeaf(x_n));
            let y_k = try!(y.keyInLeaf(y_n));
            Ok(bcmp::Compare(&x_k, &y_k))
        } else {
            match (&x.prefix, &y.prefix) {
                (&Some(ref x_p), &Some(ref y_p)) => {
                    let x_k = x.pr.get_slice(x_cur, x_klen - x_p.len());
                    let y_k = y.pr.get_slice(y_cur, y_klen - y_p.len());
                    Ok(KeyRef::compare_px_py(x_p, x_k, y_p, y_k))
                },
                (&Some(ref x_p), &None) => {
                    let x_k = x.pr.get_slice(x_cur, x_klen - x_p.len());
                    let y_k = y.pr.get_slice(y_cur, y_klen);
                    Ok(KeyRef::compare_px_y(x_p, x_k, y_k))
                },
                (&None, &Some(ref y_p)) => {
                    let x_k = x.pr.get_slice(x_cur, x_klen);
                    let y_k = y.pr.get_slice(y_cur, y_klen - y_p.len());
                    Ok(KeyRef::compare_x_py(x_k, y_p, y_k))
                },
                (&None, &None) => {
                    let x_k = x.pr.get_slice(x_cur, x_klen);
                    let y_k = y.pr.get_slice(y_cur, y_klen);
                    Ok(bcmp::Compare(&x_k, &y_k))
                },
            }
        }
    }

    fn searchLeaf(&mut self, k: &KeyRef, min:usize, max:usize, sop:SeekOp, le: Option<usize>, ge: Option<usize>) -> Result<(Option<usize>,bool)> {
        if max < min {
            match sop {
                SeekOp::SEEK_EQ => Ok((None, false)),
                SeekOp::SEEK_LE => Ok((le, false)),
                SeekOp::SEEK_GE => Ok((ge, false)),
            }
        } else {
            let mid = (max + min) / 2;
            // assert mid >= 0
            let cmp = {
                let q = try!(self.keyInLeaf2(mid));
                KeyRef::cmp(&q, k)
            };
            match cmp {
                Ordering::Equal => Ok((Some(mid), true)),
                Ordering::Less => self.searchLeaf(k, (mid+1), max, sop, Some(mid), ge),
                Ordering::Greater => 
                    // we could just recurse with mid-1, but that would overflow if
                    // mod is 0, so we catch that case here.
                    if mid==0 { 
                        match sop {
                            SeekOp::SEEK_EQ => Ok((None, false)),
                            SeekOp::SEEK_LE => Ok((le, false)),
                            SeekOp::SEEK_GE => Ok((Some(mid), false)),
                        }
                    } else { 
                        self.searchLeaf(k, min, (mid-1), sop, le, Some(mid))
                    },
            }
        }
    }

    fn readParentPage(&mut self) -> Result<(Vec<PageNum>, Vec<KeyRef>)> {
        let mut cur = 0;
        let pt = try!(PageType::from_u8(self.pr.GetByte(&mut cur)));
        if  pt != PageType::PARENT_NODE {
            return Err(LsmError::CorruptFile("parent page has invalid page type"));
        }
        cur = cur + 1; // page flags
        let count = self.pr.GetInt16(&mut cur);
        let mut ptrs = Vec::with_capacity((count + 1) as usize);
        let mut keys = Vec::with_capacity(count as usize);
        for _ in 0 .. count+1 {
            ptrs.push(self.pr.GetVarint(&mut cur) as PageNum);
        }
        for _ in 0 .. count {
            let kflag = self.pr.GetByte(&mut cur);
            let klen = self.pr.GetVarint(&mut cur) as usize;
            if 0 == (kflag & ValueFlag::FLAG_OVERFLOW) {
                keys.push(KeyRef::Array(self.pr.get_slice(cur, klen)));
                cur = cur + klen;
            } else {
                let firstPage = self.pr.GetInt32(&mut cur) as PageNum;
                let pgsz = self.pr.PageSize();
                let mut ostrm = try!(myOverflowReadStream::new(&self.path, pgsz, firstPage, klen));
                let mut x_k = Vec::with_capacity(klen);
                try!(ostrm.read_to_end(&mut x_k));
                let x_k = x_k.into_boxed_slice();
                keys.push(KeyRef::Overflowed(x_k));
            }
        }
        Ok((ptrs,keys))
    }

    // this is used when moving forward through the leaf pages.
    // we need to skip any overflows.  when moving backward,
    // this is not necessary, because each leaf has a pointer to
    // the leaf before it.
    // TODO it's unfortunate that Next is the slower operation
    // when it is far more common than Prev.  OTOH, the pages
    // are written as we stream through a set of kvp objects,
    // and we don't want to rewind, and we want to write each
    // page only once, and we want to keep the minimum amount
    // of stuff in memory as we go.  and this code only causes
    // a perf difference if there are overflow pages between
    // the leaves.
    fn searchForwardForLeaf(&mut self) -> Result<bool> {
        let pt = try!(self.pr.PageType());
        if pt == PageType::LEAF_NODE { 
            Ok(true)
        } else if pt == PageType::PARENT_NODE { 
            // if we bump into a parent node, that means there are
            // no more leaves.
            Ok(false)
        } else {
            let lastInt32 = self.pr.GetLastInt32() as PageNum;
            //
            // an overflow page has a value in its LastInt32 which
            // is one of two things.
            //
            // if it's a boundary node, it's the page number of the
            // next page in the segment.
            //
            // otherwise, it's the number of pages to skip ahead.
            // this skip might take us to whatever follows this
            // overflow (which could be a leaf or a parent or
            // another overflow), or it might just take us to a
            // boundary page (in the case where the overflow didn't
            // fit).  it doesn't matter.  we just skip ahead.
            //
            if self.pr.CheckPageFlag(PageFlag::FLAG_BOUNDARY_NODE) {
                if try!(self.setCurrentPage(lastInt32)) {
                    self.searchForwardForLeaf()
                } else {
                    Ok(false)
                }
            } else {
                let lastPage = self.currentPage + lastInt32;
                let endsOnBoundary = self.pr.CheckPageFlag(PageFlag::FLAG_ENDS_ON_BOUNDARY);
                if endsOnBoundary {
                    if try!(self.setCurrentPage(lastPage)) {
                        let next = self.pr.GetLastInt32() as PageNum;
                        if try!(self.setCurrentPage(next)) {
                            self.searchForwardForLeaf()
                        } else {
                            Ok(false)
                        }
                    } else {
                        Ok(false)
                    }
                } else {
                    if try!(self.setCurrentPage(lastPage + 1)) {
                        self.searchForwardForLeaf()
                    } else {
                        Ok(false)
                    }
                }
            }
        }
    }

    fn leafIsValid(&self) -> bool {
        // TODO the bounds check of self.currentKey against self.leafKeys.len() could be an assert
        let ok = (!self.leafKeys.is_empty()) && (self.currentKey.is_some()) && (self.currentKey.expect("just did is_some") as usize) < self.leafKeys.len();
        ok
    }

    fn search(&mut self, pg: PageNum, k: &KeyRef, sop:SeekOp) -> Result<SeekResult> {
        if try!(self.setCurrentPage(pg)) {
            let pt = try!(self.pr.PageType());
            if PageType::LEAF_NODE == pt {
                try!(self.readLeaf());
                let tmp_countLeafKeys = self.leafKeys.len();
                let (newCur, equal) = try!(self.searchLeaf(k, 0, (tmp_countLeafKeys - 1), sop, None, None));
                self.currentKey = newCur;
                if SeekOp::SEEK_EQ != sop {
                    if ! self.leafIsValid() {
                        // if LE or GE failed on a given page, we might need
                        // to look at the next/prev leaf.
                        if SeekOp::SEEK_GE == sop {
                            let nextPage =
                                if self.pr.CheckPageFlag(PageFlag::FLAG_BOUNDARY_NODE) { self.pr.GetLastInt32() as PageNum }
                                else if self.currentPage == self.rootPage { 0 }
                                else { self.currentPage + 1 };
                            if try!(self.setCurrentPage(nextPage)) && try!(self.searchForwardForLeaf()) {
                                try!(self.readLeaf());
                                self.currentKey = Some(0);
                            }
                        } else {
                            let tmp_previousLeaf = self.previousLeaf;
                            if 0 == self.previousLeaf {
                                self.resetLeaf();
                            } else if try!(self.setCurrentPage(tmp_previousLeaf)) {
                                try!(self.readLeaf());
                                self.currentKey = Some(self.leafKeys.len() - 1);
                            }
                        }
                    }
                }
                if self.currentKey.is_none() {
                    Ok(SeekResult::Invalid)
                } else if equal {
                    Ok(SeekResult::Equal)
                } else {
                    Ok(SeekResult::Unequal)
                }
            } else if PageType::PARENT_NODE == pt {
                let next = {
                    let (ptrs, keys) = try!(self.readParentPage());
                    match Self::searchInParentPage(k, &ptrs, &keys, 0) {
                        Some(found) => found,
                        None => ptrs[ptrs.len() - 1],
                    }
                };
                self.search(next, k, sop)
            } else {
                unreachable!();
            }
        } else {
            Ok(SeekResult::Invalid)
        }
    }

    fn searchInParentPage(k: &KeyRef, ptrs: &Vec<PageNum>, keys: &Vec<KeyRef>, i: usize) -> Option<PageNum> {
        // TODO linear search?  really?
        // TODO also, this doesn't need to be recursive
        if i < keys.len() {
            let cmp = KeyRef::cmp(k, &keys[i]);
            if cmp==Ordering::Greater {
                Self::searchInParentPage(k, ptrs, keys, i+1)
            } else {
                Some(ptrs[i])
            }
        } else {
            None
        }
    }

}

impl<'a> Drop for SegmentCursor<'a> {
    fn drop(&mut self) {
        self.inner.cursor_dropped(self.segnum, self.csrnum);
    }
}

impl<'a> ICursor<'a> for SegmentCursor<'a> {
    fn IsValid(&self) -> bool {
        self.leafIsValid()
    }

    fn SeekRef(&mut self, k: &KeyRef, sop:SeekOp) -> Result<SeekResult> {
        let rootPage = self.rootPage;
        self.search(rootPage, k, sop)
    }

    fn KeyRef(&'a self) -> Result<KeyRef<'a>> {
        match self.currentKey {
            None => Err(LsmError::CursorNotValid),
            Some(currentKey) => self.keyInLeaf2(currentKey),
        }
    }

    fn ValueRef(&'a self) -> Result<ValueRef<'a>> {
        match self.currentKey {
            None => Err(LsmError::CursorNotValid),
            Some(currentKey) => {
                let mut pos = self.leafKeys[currentKey as usize];

                self.skipKey(&mut pos);

                let vflag = self.pr.GetByte(&mut pos);
                if 0 != (vflag & ValueFlag::FLAG_TOMBSTONE) {
                    Ok(ValueRef::Tombstone)
                } else {
                    let vlen = self.pr.GetVarint(&mut pos) as usize;
                    if 0 != (vflag & ValueFlag::FLAG_OVERFLOW) {
                        let pgnum = self.pr.GetInt32(&mut pos) as PageNum;
                        let strm = try!(myOverflowReadStream::new(&self.path, self.pr.PageSize(), pgnum, vlen));
                        Ok(ValueRef::Overflowed(vlen, box strm))
                    } else {
                        Ok(ValueRef::Array(self.pr.get_slice(pos, vlen)))
                    }
                }
            }
        }
    }

    fn ValueLength(&self) -> Result<Option<usize>> {
        match self.currentKey {
            None => Err(LsmError::CursorNotValid),
            Some(currentKey) => {
                let mut cur = self.leafKeys[currentKey as usize];

                self.skipKey(&mut cur);

                let vflag = self.pr.GetByte(&mut cur);
                if 0 != (vflag & ValueFlag::FLAG_TOMBSTONE) { 
                    Ok(None)
                } else {
                    let vlen = self.pr.GetVarint(&mut cur) as usize;
                    Ok(Some(vlen))
                }
            }
        }
    }

    fn KeyCompare(&self, k_other: &KeyRef) -> Result<Ordering> {
        let k_me = try!(self.KeyRef());
        let c = KeyRef::cmp(&k_me, &k_other);
        Ok(c)
    }

    fn First(&mut self) -> Result<()> {
        let firstLeaf = self.firstLeaf;
        if try!(self.setCurrentPage(firstLeaf)) {
            try!(self.readLeaf());
            self.currentKey = Some(0);
        }
        Ok(())
    }

    fn Last(&mut self) -> Result<()> {
        let lastLeaf = self.lastLeaf;
        if try!(self.setCurrentPage(lastLeaf)) {
            try!(self.readLeaf());
            self.currentKey = Some(self.leafKeys.len() - 1);
        }
        Ok(())
    }

    fn Next(&mut self) -> Result<()> {
        if ! self.nextInLeaf() {
            let nextPage =
                if self.pr.CheckPageFlag(PageFlag::FLAG_BOUNDARY_NODE) { self.pr.GetLastInt32() as PageNum }
                else if try!(self.pr.PageType()) == PageType::LEAF_NODE {
                    if self.currentPage == self.rootPage { 0 }
                    else { self.currentPage + 1 }
                } else { 0 }
            ;
            if try!(self.setCurrentPage(nextPage)) && try!(self.searchForwardForLeaf()) {
                try!(self.readLeaf());
                self.currentKey = Some(0);
            }
        }
        Ok(())
    }

    fn Prev(&mut self) -> Result<()> {
        if ! self.prevInLeaf() {
            let previousLeaf = self.previousLeaf;
            if 0 == previousLeaf {
                self.resetLeaf();
            } else if try!(self.setCurrentPage(previousLeaf)) {
                try!(self.readLeaf());
                self.currentKey = Some(self.leafKeys.len() - 1);
            }
        }
        Ok(())
    }

}

#[derive(Clone)]
struct HeaderData {
    // TODO currentState is an ordered copy of segments.Keys.  eliminate duplication?
    // or add assertions and tests to make sure they never get out of sync?  we wish
    // we had a form of HashMap that kept track of ordering.
    currentState: Vec<SegmentNum>,
    segments: HashMap<SegmentNum,SegmentInfo>,
    headerOverflow: Option<PageBlock>,
    changeCounter: u64,
    mergeCounter: u64,
}

const HEADER_SIZE_IN_BYTES: usize = 4096;

impl PendingSegment {
    fn new(num: SegmentNum) -> PendingSegment {
        // TODO maybe set capacity of the blocklist vec to something low
        PendingSegment {blockList: Vec::new(), segnum: num}
    }

    fn AddBlock(&mut self, b: PageBlock) {
        //println!("seg {:?} got block {:?}", self.segnum, b);
        let len = self.blockList.len();
        if (! (self.blockList.is_empty())) && (b.firstPage == self.blockList[len-1].lastPage+1) {
            // note that by consolidating blocks here, the segment info list will
            // not have information about the fact that the two blocks were
            // originally separate.  that's okay, since all we care about here is
            // keeping track of which pages are used.  but the btree code itself
            // is still treating the last page of the first block as a boundary
            // page, even though its pointer to the next block goes to the very
            // next page, because its page manager happened to give it a block
            // which immediately follows the one it had.
            self.blockList[len-1].lastPage = b.lastPage;
        } else {
            self.blockList.push(b);
        }
    }

    fn End(mut self, lastPage: PageNum) -> (SegmentNum, Vec<PageBlock>, Option<PageBlock>) {
        let len = self.blockList.len();
        let leftovers = {
            let givenLastPage = self.blockList[len-1].lastPage;
            if lastPage < givenLastPage {
                self.blockList[len-1].lastPage = lastPage;
                Some (PageBlock::new(lastPage+1, givenLastPage))
            } else {
                None
            }
        };
        // consume self return blockList
        (self.segnum, self.blockList, leftovers)
    }
}

fn readHeader<R>(fs: &mut R) -> Result<(HeaderData,usize,PageNum,SegmentNum)> where R : Read+Seek {
    fn read<R>(fs: &mut R) -> Result<PageBuffer> where R : Read {
        let mut pr = PageBuffer::new(HEADER_SIZE_IN_BYTES);
        let got = try!(pr.Read(fs));
        if got < HEADER_SIZE_IN_BYTES {
            Err(LsmError::CorruptFile("invalid header"))
        } else {
            Ok(pr)
        }
    }

    fn parse<R>(pr: &PageBuffer, cur: &mut usize, fs: &mut R) -> Result<(HeaderData, usize)> where R : Read+Seek {
        fn readSegmentList(pr: &PageBuffer, cur: &mut usize) -> Result<(Vec<SegmentNum>,HashMap<SegmentNum,SegmentInfo>)> {
            fn readBlockList(prBlocks: &PageBuffer, cur: &mut usize) -> Vec<PageBlock> {
                let count = prBlocks.GetVarint(cur) as usize;
                let mut a = Vec::with_capacity(count);
                for _ in 0 .. count {
                    let firstPage = prBlocks.GetVarint(cur) as PageNum;
                    let countPages = prBlocks.GetVarint(cur) as PageNum;
                    // blocks are stored as firstPage/count rather than as
                    // firstPage/lastPage, because the count will always be
                    // smaller as a varint
                    a.push(PageBlock::new(firstPage,firstPage + countPages - 1));
                }
                a
            }

            let count = pr.GetVarint(cur) as usize;
            let mut a = Vec::with_capacity(count);
            let mut m = HashMap::with_capacity(count);
            for _ in 0 .. count {
                let g = pr.GetVarint(cur) as SegmentNum;
                a.push(g);
                let root = pr.GetVarint(cur) as PageNum;
                let age = pr.GetVarint(cur) as u32;
                let blocks = readBlockList(pr, cur);
                if !block_list_contains_page(&blocks, root) {
                    return Err(LsmError::RootPageNotInSegmentBlockList);
                }
                let info = SegmentInfo {root:root,age:age,blocks:blocks};
                m.insert(g,info);
            }
            Ok((a,m))
        }

        // --------

        let pgsz = pr.GetInt32(cur) as usize;
        let changeCounter = pr.GetVarint(cur);
        let mergeCounter = pr.GetVarint(cur);
        let lenSegmentList = pr.GetVarint(cur) as usize;

        let overflowed = pr.GetByte(cur) != 0u8;
        let (state, segments, blk) = 
            if overflowed {
                let lenChunk1 = pr.GetInt32(cur) as usize;
                let lenChunk2 = lenSegmentList - lenChunk1;
                let firstPageChunk2 = pr.GetInt32(cur) as PageNum;
                let extraPages = lenChunk2 / pgsz + if (lenChunk2 % pgsz) != 0 { 1 } else { 0 };
                let extraPages = extraPages as PageNum;
                let lastPageChunk2 = firstPageChunk2 + extraPages - 1;
                let mut pr2 = PageBuffer::new(lenSegmentList);
                // TODO chain?
                // copy from chunk1 into pr2
                try!(pr2.ReadPart(fs, 0, lenChunk1));
                // now get chunk2 and copy it in as well
                try!(utils::SeekPage(fs, pgsz, firstPageChunk2));
                try!(pr2.ReadPart(fs, lenChunk1, lenChunk2));
                let mut cur2 = 0;
                let (state, segments) = try!(readSegmentList(&pr2, &mut cur2));
                (state, segments, Some (PageBlock::new(firstPageChunk2, lastPageChunk2)))
            } else {
                let (state,segments) = try!(readSegmentList(pr, cur));
                (state, segments, None)
            };


        let hd = 
            HeaderData
            {
                currentState: state,
                segments: segments,
                headerOverflow: blk,
                changeCounter: changeCounter,
                mergeCounter: mergeCounter,
            };

        Ok((hd, pgsz))
    }

    fn calcNextPage(pgsz: usize, len: usize) -> PageNum {
        let numPagesSoFar = (if pgsz > len { 1 } else { len / pgsz }) as PageNum;
        numPagesSoFar + 1
    }

    // --------

    let len = try!(seek_len(fs));
    if len > 0 {
        try!(fs.seek(SeekFrom::Start(0 as u64)));
        let pr = try!(read(fs));
        let mut cur = 0;
        let (h, pgsz) = try!(parse(&pr, &mut cur, fs));
        let nextAvailablePage = calcNextPage(pgsz, len as usize);
        let nextAvailableSegmentNum = match h.currentState.iter().max() {
            Some(n) => n+1,
            None => 1,
        };
        Ok((h, pgsz, nextAvailablePage, nextAvailableSegmentNum))
    } else {
        let defaultPageSize = DEFAULT_SETTINGS.DefaultPageSize;
        let h = 
            HeaderData
            {
                segments: HashMap::new(),
                currentState: Vec::new(),
                headerOverflow: None,
                changeCounter: 0,
                mergeCounter: 0,
            };
        let nextAvailablePage = calcNextPage(defaultPageSize, HEADER_SIZE_IN_BYTES);
        let nextAvailableSegmentNum = 1;
        Ok((h, defaultPageSize, nextAvailablePage, nextAvailableSegmentNum))
    }

}

fn consolidateBlockList(blocks: &mut Vec<PageBlock>) {
    blocks.sort_by(|a,b| a.firstPage.cmp(&b.firstPage));
    loop {
        if blocks.len()==1 {
            break;
        }
        let mut did = false;
        for i in 1 .. blocks.len() {
            if blocks[i-1].lastPage+1 == blocks[i].firstPage {
                blocks[i-1].lastPage = blocks[i].lastPage;
                blocks.remove(i);
                did = true;
                break;
            }
        }
        if !did {
            break;
        }
    }
}

fn invertBlockList(blocks: &Vec<PageBlock>) -> Vec<PageBlock> {
    let len = blocks.len();
    let mut result = Vec::with_capacity(len);
    for i in 0 .. len {
        result.push(blocks[i]);
    }
    result.sort_by(|a,b| a.firstPage.cmp(&b.firstPage));
    for i in 0 .. len-1 {
        result[i].firstPage = result[i].lastPage+1;
        result[i].lastPage = result[i+1].firstPage-1;
    }
    result.remove(len-1);
    result
}

fn listAllBlocks(h: &HeaderData, segmentsInWaiting: &HashMap<SegmentNum,SegmentInfo>, pgsz: usize) -> Vec<PageBlock> {
    let headerBlock = PageBlock::new(1, (HEADER_SIZE_IN_BYTES / pgsz) as PageNum);
    let mut blocks = Vec::new();

    fn grab(blocks: &mut Vec<PageBlock>, from: &HashMap<SegmentNum,SegmentInfo>) {
        for info in from.values() {
            for b in info.blocks.iter() {
                blocks.push(*b);
            }
        }
    }

    grab(&mut blocks, &h.segments);
    grab(&mut blocks, segmentsInWaiting);
    blocks.push(headerBlock);
    match h.headerOverflow {
        Some(blk) => blocks.push(blk),
        None => ()
    }
    blocks
}

use std::sync::Mutex;

struct NextSeg {
    nextSeg: SegmentNum,
}

struct Space {
    nextPage: PageNum,
    freeBlocks: Vec<PageBlock>,
}

struct SafeSegmentsInWaiting {
    segmentsInWaiting: HashMap<SegmentNum,SegmentInfo>,
}

struct SafeMergeStuff {
    merging: HashSet<SegmentNum>,
    pendingMerges: HashMap<SegmentNum,Vec<SegmentNum>>,
}

struct SafeHeader {
    // TODO one level too much nesting
    header: HeaderData,
}

struct SafeCursors {
    nextCursorNum: u64,
    cursors: HashMap<u64,SegmentNum>,
    zombies: HashMap<SegmentNum,SegmentInfo>,
}

struct InnerPart {
    path: String,
    pgsz: usize,
    settings: DbSettings,

    nextSeg: Mutex<NextSeg>,
    space: Mutex<Space>,
    // TODO should the header mutex be an RWLock?
    header: Mutex<SafeHeader>,
    segmentsInWaiting: Mutex<SafeSegmentsInWaiting>,
    mergeStuff: Mutex<SafeMergeStuff>,
    cursors: Mutex<SafeCursors>,
}

pub struct WriteLock<'a> {
    inner: Option<&'a InnerPart>
}

impl<'a> WriteLock<'a> {
    pub fn commitSegments(&self, newSegs: Vec<SegmentNum>) -> Result<()> {
        self.inner.unwrap().commitSegments(newSegs)
    }

    pub fn commitMerge(&self, newSegNum:SegmentNum) -> Result<()> {
        self.inner.unwrap().commitMerge(newSegNum)
    }
}

// TODO rename this
pub struct db<'a> {

    inner: InnerPart,
    write_lock: Mutex<WriteLock<'a>>,
}

impl<'a> db<'a> {
    pub fn new(path: String, settings : DbSettings) -> Result<db<'a>> {

        let mut f = try!(OpenOptions::new()
                .read(true)
                .create(true)
                .open(&path));

        let (header,pgsz,firstAvailablePage,nextAvailableSegmentNum) = try!(readHeader(&mut f));

        let segmentsInWaiting = HashMap::new();
        let mut blocks = listAllBlocks(&header, &segmentsInWaiting, pgsz);
        consolidateBlockList(&mut blocks);
        let mut freeBlocks = invertBlockList(&blocks);
        freeBlocks.sort_by(|a,b| b.count_pages().cmp(&a.count_pages()));

        let nextSeg = NextSeg {
            nextSeg: nextAvailableSegmentNum,
        };

        let space = Space {
            nextPage: firstAvailablePage, 
            freeBlocks: freeBlocks,
        };

        let segmentsInWaiting = SafeSegmentsInWaiting {
            segmentsInWaiting: segmentsInWaiting,
        };

        let mergeStuff = SafeMergeStuff {
            merging: HashSet::new(),
            pendingMerges: HashMap::new(),
        };

        let header = SafeHeader {
            header: header, 
        };

        let cursors = SafeCursors {
            nextCursorNum: 1,
            cursors: HashMap::new(),
            zombies: HashMap::new(),
        };

        let inner = InnerPart {
            path: path,
            pgsz: pgsz,
            settings: settings, 
            header: Mutex::new(header),
            nextSeg: Mutex::new(nextSeg),
            space: Mutex::new(space),
            segmentsInWaiting: Mutex::new(segmentsInWaiting),
            mergeStuff: Mutex::new(mergeStuff),
            cursors: Mutex::new(cursors),
        };

        // WriteLock contains a reference to another part of
        // the struct it is in.  So we wrap it in an option,
        // and set it to null for now.  We set it later when
        // somebody actually asks for the lock.

        let lck = WriteLock { inner: None };
        let res = db {
            inner: inner,
            write_lock: Mutex::new(lck),
        };
        Ok(res)
    }

    // TODO func to ask for the write lock without blocking?

    pub fn GetWriteLock(&'a self) -> Result<std::sync::MutexGuard<WriteLock<'a>>> {
        let mut lck = try!(self.write_lock.lock());
        // set the inner reference
        lck.inner = Some(&self.inner);
        Ok(lck)
    }

    // the following methods are passthrus, exposing inner
    // stuff publicly.

    pub fn OpenCursor(&self) -> Result<LivingCursor> {
        self.inner.OpenCursor()
    }

    pub fn WriteSegmentFromSortedSequence<I>(&self, source: I) -> Result<SegmentNum> where I:Iterator<Item=Result<kvp>> {
        self.inner.WriteSegmentFromSortedSequence(source)
    }

    pub fn WriteSegment(&self, pairs: HashMap<Box<[u8]>,Box<[u8]>>) -> Result<SegmentNum> {
        self.inner.WriteSegment(pairs)
    }

    pub fn WriteSegment2(&self, pairs: HashMap<Box<[u8]>,Blob>) -> Result<SegmentNum> {
        self.inner.WriteSegment2(pairs)
    }

    pub fn merge(&self, level: u32, min: usize, max: Option<usize>) -> Result<Option<SegmentNum>> {
        self.inner.merge(level, min, max)
    }
}

// TODO this could be generic
fn slice_within(sub: &[SegmentNum], within: &[SegmentNum]) -> Result<usize> {
    match within.iter().position(|&g| g == sub[0]) {
        Some(ndx_first) => {
            let count = sub.len();
            if sub == &within[ndx_first .. ndx_first + count] {
                Ok(ndx_first)
            } else {
                Err(LsmError::Misc("not contiguous"))
            }
        },
        None => {
            Err(LsmError::Misc("not contiguous"))
        },
    }
}

impl InnerPart {

    fn cursor_dropped(&self, segnum: SegmentNum, csrnum: u64) {
        //println!("cursor_dropped");
        let mut cursors = self.cursors.lock().unwrap(); // gotta succeed
        let seg = cursors.cursors.remove(&csrnum).expect("gotta be there");
        assert_eq!(seg, segnum);
        match cursors.zombies.remove(&segnum) {
            Some(info) => {
                // TODO maybe allow this lock to fail with try_lock.  the
                // worst that can happen is that these blocks don't get
                // reclaimed until some other day.
                let mut space = self.space.lock().unwrap(); // gotta succeed
                self.addFreeBlocks(&mut space, info.blocks);
            },
            None => {
            },
        }
    }

    fn getBlock(&self, space: &mut Space, specificSizeInPages: PageNum) -> PageBlock {
        if specificSizeInPages > 0 {
            if space.freeBlocks.is_empty() || specificSizeInPages > space.freeBlocks[0].count_pages() {
                let newBlk = PageBlock::new(space.nextPage, space.nextPage+specificSizeInPages-1);
                space.nextPage = space.nextPage + specificSizeInPages;
                newBlk
            } else {
                let headBlk = space.freeBlocks[0];
                if headBlk.count_pages() > specificSizeInPages {
                    // trim the block to size
                    let blk2 = PageBlock::new(headBlk.firstPage,
                                              headBlk.firstPage+specificSizeInPages-1); 
                    space.freeBlocks[0].firstPage = space.freeBlocks[0].firstPage + specificSizeInPages;
                    // TODO problem: the list is probably no longer sorted.  is this okay?
                    // is a re-sort of the list really worth it?
                    blk2
                } else {
                    space.freeBlocks.remove(0);
                    headBlk
                }
            }
        } else {
            if space.freeBlocks.is_empty() {
                let size = self.settings.PagesPerBlock;
                let newBlk = PageBlock::new(space.nextPage, space.nextPage+size-1) ;
                space.nextPage = space.nextPage + size;
                newBlk
            } else {
                let headBlk = space.freeBlocks[0];
                space.freeBlocks.remove(0);
                headBlk
            }
        }
    }

    fn OpenForWriting(&self) -> io::Result<File> {
        OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.path)
    }

    fn OpenForReading(&self) -> io::Result<File> {
        OpenOptions::new()
                .read(true)
                .open(&self.path)
    }

    // this code should not be called in a release build.  it helps
    // finds problems by zeroing out pages in blocks that
    // have been freed.
    fn stomp(&self, blocks:Vec<PageBlock>) -> Result<()> {
        let bad = vec![0;self.pgsz as usize].into_boxed_slice();
        let mut fs = try!(OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.path));
        for b in blocks {
            for x in b.firstPage .. b.lastPage+1 {
                try!(utils::SeekPage(&mut fs, self.pgsz, x));
                try!(fs.write(&bad));
            }
        }
        Ok(())
    }

    fn addFreeBlocks(&self, space: &mut Space, blocks:Vec<PageBlock>) {

        // all additions to the freeBlocks list should happen here
        // by calling this function.
        //
        // the list is kept consolidated and sorted by size descending.
        // unfortunately this requires two sorts, and they happen here
        // inside a critical section.  but the benefit is considered
        // worth the trouble.
        
        // TODO it is important that freeBlocks contains no overlaps.
        // add debug-only checks to verify?

        // TODO is there such a thing as a block that is so small we
        // don't want to bother with it?  what about a single-page block?
        // should this be a configurable setting?

        // TODO if the last block of the file is free, consider just
        // moving nextPage back.

        for b in blocks {
            space.freeBlocks.push(b);
        }
        consolidateBlockList(&mut space.freeBlocks);
        space.freeBlocks.sort_by(|a,b| b.count_pages().cmp(&a.count_pages()));
    }

    // a stored segmentinfo for a segment is a single blob of bytes.
    // root page
    // age
    // number of pairs
    // each pair is startBlock,countBlocks
    // all in varints

    fn writeHeader(&self, 
                   st: &mut SafeHeader, 
                   space: &mut Space,
                   fs: &mut File, 
                   mut hdr: HeaderData
                  ) -> Result<Option<PageBlock>> {
        fn spaceNeededForSegmentInfo(info: &SegmentInfo) -> usize {
            let mut a = 0;
            for t in info.blocks.iter() {
                a = a + Varint::SpaceNeededFor(t.firstPage as u64);
                a = a + Varint::SpaceNeededFor(t.count_pages() as u64);
            }
            a = a + Varint::SpaceNeededFor(info.root as u64);
            a = a + Varint::SpaceNeededFor(info.age as u64);
            a = a + Varint::SpaceNeededFor(info.blocks.len() as u64);
            a
        }

        fn spaceForHeader(h: &HeaderData) -> usize {
            let mut a = Varint::SpaceNeededFor(h.currentState.len() as u64);
            // TODO use currentState with a lookup into h.segments instead?
            // should be the same, right?
            for (g,info) in h.segments.iter() {
                a = a + spaceNeededForSegmentInfo(&info) + Varint::SpaceNeededFor(*g);
            }
            a
        }

        fn buildSegmentList(h: &HeaderData) -> PageBuilder {
            let space = spaceForHeader(h);
            let mut pb = PageBuilder::new(space);
            // TODO format version number
            pb.PutVarint(h.currentState.len() as u64);
            for g in h.currentState.iter() {
                pb.PutVarint(*g);
                match h.segments.get(&g) {
                    Some(info) => {
                        pb.PutVarint(info.root as u64);
                        pb.PutVarint(info.age as u64);
                        pb.PutVarint(info.blocks.len() as u64);
                        // we store PageBlock as first/count instead of first/last, since the
                        // count will always compress better as a varint.
                        for t in info.blocks.iter() {
                            pb.PutVarint(t.firstPage as u64);
                            pb.PutVarint(t.count_pages() as u64);
                        }
                    },
                    None => panic!("segment num in currentState but not in segments")
                }
            }
            assert!(0 == pb.Available());
            pb
        }

        let mut pb = PageBuilder::new(HEADER_SIZE_IN_BYTES);
        pb.PutInt32(self.pgsz as u32);

        pb.PutVarint(hdr.changeCounter);
        pb.PutVarint(hdr.mergeCounter);

        let pbSegList = buildSegmentList(&hdr);
        let buf = pbSegList.Buffer();
        pb.PutVarint(buf.len() as u64);

        let headerOverflow =
            if pb.Available() >= (buf.len() + 1) {
                pb.PutByte(0u8);
                pb.PutArray(buf);
                None
            } else {
                pb.PutByte(1u8);
                let fits = pb.Available() - 4 - 4;
                let extra = buf.len() - fits;
                let extraPages = extra / self.pgsz + if (extra % self.pgsz) != 0 { 1 } else { 0 };
                //printfn "extra pages: %d" extraPages
                let blk = self.getBlock(space, extraPages as PageNum);
                try!(utils::SeekPage(fs, self.pgsz, blk.firstPage));
                try!(fs.write(&buf[fits .. buf.len()]));
                pb.PutInt32(fits as u32);
                pb.PutInt32(blk.firstPage);
                pb.PutArray(&buf[0 .. fits]);
                Some(blk)
            };

        try!(fs.seek(SeekFrom::Start(0)));
        try!(pb.Write(fs));
        try!(fs.flush());
        let oldHeaderOverflow = hdr.headerOverflow;
        hdr.headerOverflow = headerOverflow;
        st.header = hdr;
        Ok((oldHeaderOverflow))
    }

    // TODO this function looks for the segment in the header.segments,
    // which means it cannot be used to open a cursor on a pendingSegment,
    // which we think we might need in the future.
    fn getCursor(&self, 
                 st: &SafeHeader,
                 g: SegmentNum
                ) -> Result<SegmentCursor> {
        match st.header.segments.get(&g) {
            None => Err(LsmError::Misc("getCursor: segment not found")),
            Some(seg) => {
                let rootPage = seg.root;
                let mut cursors = try!(self.cursors.lock());
                let csrnum = cursors.nextCursorNum;
                let csr = try!(SegmentCursor::new(&self.path, self.pgsz, rootPage, seg.blocks.clone(), &self, g, csrnum));

                cursors.nextCursorNum = cursors.nextCursorNum + 1;
                let was = cursors.cursors.insert(csrnum, g);
                assert!(was.is_none());
                Ok(csr)
            }
        }
    }

    // TODO we also need a way to open a cursor on segments in waiting
    fn OpenCursor(&self) -> Result<LivingCursor> {
        // TODO this cursor needs to expose the changeCounter and segment list
        // on which it is based. for optimistic writes. caller can grab a cursor,
        // do their writes, then grab the writelock, and grab another cursor, then
        // compare the two cursors to see if anything important changed.  if not,
        // commit their writes.  if so, nevermind the written segments and start over.

        let st = try!(self.header.lock());
        let mut clist = Vec::with_capacity(st.header.currentState.len());
        for g in st.header.currentState.iter() {
            clist.push(try!(self.getCursor(&*st, *g)));
        }
        let mc = MultiCursor::Create(clist);
        let lc = LivingCursor::Create(mc);
        Ok(lc)
    }

    fn commitSegments(&self, 
                      newSegs: Vec<SegmentNum>
                     ) -> Result<()> {
        assert_eq!(newSegs.len(), newSegs.iter().map(|g| *g).collect::<HashSet<SegmentNum>>().len());

        let mut st = try!(self.header.lock());
        let mut waiting = try!(self.segmentsInWaiting.lock());
        let mut space = try!(self.space.lock());

        assert!({
            let mut ok = true;
            for newSegNum in newSegs.iter() {
                ok = st.header.currentState.iter().position(|&g| g == *newSegNum).is_none();
                if !ok {
                    break;
                }
            }
            ok
        });

        // self.segmentsInWaiting must contain one seg for each segment num in newSegs.
        // we want those entries to move out and move into the header, currentState
        // and segments.  This means taking ownership of those SegmentInfos.  But
        // the others we want to leave.

        let mut newHeader = st.header.clone();
        let mut newSegmentsInWaiting = waiting.segmentsInWaiting.clone();
        for g in newSegs.iter() {
            match newSegmentsInWaiting.remove(&g) {
                Some(info) => {
                    newHeader.segments.insert(*g,info);
                },
                None => {
                    return Err(LsmError::Misc("commitSegments: segment not found in segmentsInWaiting"));
                },
            }
        }

        // TODO surely there's a better way to insert one vec into another?
        // like insert_all, similar to push_all?
        for i in 0 .. newSegs.len() {
            let g = newSegs[i];
            newHeader.currentState.insert(i, g);
        }

        newHeader.changeCounter = newHeader.changeCounter + 1;

        let mut fs = try!(self.OpenForWriting());
        let oldHeaderOverflow = try!(self.writeHeader(&mut st, &mut space, &mut fs, newHeader));
        waiting.segmentsInWaiting = newSegmentsInWaiting;

        //printfn "after commit, currentState: %A" header.currentState
        //printfn "after commit, segments: %A" header.segments
        // all the segments we just committed can now be removed from
        // the segments in waiting list
        match oldHeaderOverflow {
            Some(blk) => self.addFreeBlocks(&mut space, vec![ blk ]),
            None => ()
        }
        // note that we intentionally do not release the writeLock here.
        // you can change the segment list more than once while holding
        // the writeLock.  the writeLock gets released when you Dispose() it.
        Ok(())
    }

    // TODO bad fn name
    fn WriteSegmentFromSortedSequence<I>(&self, source: I) -> Result<SegmentNum> where I:Iterator<Item=Result<kvp>> {
        let mut fs = try!(self.OpenForWriting());
        let (g,_) = try!(CreateFromSortedSequenceOfKeyValuePairs(&mut fs, self, source));
        Ok(g)
    }

    // TODO bad fn name
    fn WriteSegment(&self, pairs: HashMap<Box<[u8]>,Box<[u8]>>) -> Result<SegmentNum> {
        let mut a : Vec<(Box<[u8]>,Box<[u8]>)> = pairs.into_iter().collect();

        a.sort_by(|a,b| {
            let (ref ka,_) = *a;
            let (ref kb,_) = *b;
            bcmp::Compare(&ka,&kb)
        });
        let source = a.into_iter().map(|t| {
            let (k,v) = t;
            Ok(kvp {Key:k, Value:Blob::Array(v)})
        });
        let mut fs = try!(self.OpenForWriting());
        let (g,_) = try!(CreateFromSortedSequenceOfKeyValuePairs(&mut fs, self, source));
        Ok(g)
    }

    // TODO bad fn name
    fn WriteSegment2(&self, pairs: HashMap<Box<[u8]>,Blob>) -> Result<SegmentNum> {
        let mut a : Vec<(Box<[u8]>,Blob)> = pairs.into_iter().collect();

        a.sort_by(|a,b| {
            let (ref ka,_) = *a;
            let (ref kb,_) = *b;
            bcmp::Compare(&ka,&kb)
        });
        let source = a.into_iter().map(|t| {
            let (k,v) = t;
            Ok(kvp {Key:k, Value:v})
        });
        let mut fs = try!(self.OpenForWriting());
        let (g,_) = try!(CreateFromSortedSequenceOfKeyValuePairs(&mut fs, self, source));
        Ok(g)
    }

    fn merge(&self, level: u32, min: usize, max: Option<usize>) -> Result<Option<SegmentNum>> {
        let mrg = {
            let st = try!(self.header.lock());

            if st.header.currentState.len() == 0 {
                return Ok(None)
            }

            //println!("age for merge: {}", level);
            //println!("currentState: {:?}", st.header.currentState);

            let age_group = st.header.currentState.iter().filter(|g| {
                let info = st.header.segments.get(&g).unwrap();
                info.age == level
            }).map(|g| *g).collect::<Vec<SegmentNum>>();

            //println!("age_group: {:?}", age_group);

            if age_group.len() == 0 {
                return Ok(None)
            }

            // make sure this is contiguous
            assert!(slice_within(age_group.as_slice(), st.header.currentState.as_slice()).is_ok());

            let mut segs = Vec::new();

            let mut mergeStuff = try!(self.mergeStuff.lock());

            // we can merge any contiguous set of not-already-being-merged 
            // segments at the end of the group.  if we merge something
            // that is not at the end of the group, we could end up with
            // age groups not being contiguous.

            for g in age_group.iter().rev() {
                if mergeStuff.merging.contains(g) {
                    break;
                } else {
                    segs.push(*g);
                }
            }

            if segs.len() >= min {
                match max {
                    Some(max) => {
                        segs.truncate(max);
                    },
                    None => (),
                }
                segs.reverse();
                let mut clist = Vec::with_capacity(segs.len());
                for g in segs.iter() {
                    clist.push(try!(self.getCursor(&st, *g)));
                }
                for g in segs.iter() {
                    mergeStuff.merging.insert(*g);
                }
                Some((segs,clist))
            } else {
                None
            }
        };
        match mrg {
            Some((segs,clist)) => {
                let mut mc = MultiCursor::Create(clist);
                let mut fs = try!(self.OpenForWriting());
                try!(mc.First());
                let (g,_) = try!(CreateFromSortedSequenceOfKeyValuePairs(&mut fs, self, CursorIterator::new(mc)));
                //printfn "merged %A to get %A" segs g
                let mut mergeStuff = try!(self.mergeStuff.lock());
                mergeStuff.pendingMerges.insert(g, segs);
                Ok(Some(g))
            },
            None => {
                Ok(None)
            },
        }
    }

    // TODO maybe commitSegments and commitMerge should be the same function.
    // just check to see if the segment being committed is a merge.  if so,
    // do the extra paperwork.
    fn commitMerge(&self, newSegNum:SegmentNum) -> Result<()> {

        let mut st = try!(self.header.lock());
        let mut waiting = try!(self.segmentsInWaiting.lock());
        let mut space = try!(self.space.lock());
        let mut mergeStuff = try!(self.mergeStuff.lock());

        assert!(st.header.currentState.iter().position(|&g| g == newSegNum).is_none());

        // we need the list of segments which were merged.  we make a copy of
        // so that we're not keeping a reference that inhibits our ability to
        // get other references a little later in the function.

        let old = {
            let maybe = mergeStuff.pendingMerges.get(&newSegNum);
            if maybe.is_none() {
                return Err(LsmError::Misc("commitMerge: segment not found in pendingMerges"));
            } else {
                maybe.expect("just checked is_none").clone()
            }
        };

        let oldAsSet : HashSet<SegmentNum> = old.iter().map(|g| *g).collect();
        assert!(oldAsSet.len() == old.len());

        // now we need to verify that the segments being replaced are in currentState
        // and contiguous.

        let ndxFirstOld = try!(slice_within(old.as_slice(), st.header.currentState.as_slice()));

        // now we construct a newHeader

        let mut newHeader = st.header.clone();

        // first, fix the currentState

        for _ in &old {
            newHeader.currentState.remove(ndxFirstOld);
        }
        newHeader.currentState.insert(ndxFirstOld, newSegNum);

        // remove the old segmentinfos, keeping them for later

        let mut segmentsBeingReplaced = HashMap::with_capacity(oldAsSet.len());
        for g in &oldAsSet {
            let info = newHeader.segments.remove(g).expect("old seg not found in header.segments");
            segmentsBeingReplaced.insert(g, info);
        }

        // now get the segment info for the new segment

        let mut newSegmentInfo = {
            let maybe = waiting.segmentsInWaiting.get(&newSegNum);
            if maybe.is_none() {
                return Err(LsmError::Misc("commitMerge: segment not found in segmentsInWaiting"));
            } else {
                maybe.expect("seg not found").clone()
            }
        };

        // and fix its age to be one higher than the maximum age of the
        // segments it replaced.

        let age_of_new_segment = {
            let ages: Vec<u32> = segmentsBeingReplaced.values().map(|info| info.age).collect();
            1 + ages.iter().max().expect("this cannot be empty")
        };
        newSegmentInfo.age = age_of_new_segment;

        newHeader.segments.insert(newSegNum, newSegmentInfo);

        newHeader.mergeCounter = newHeader.mergeCounter + 1;

        let mut fs = try!(self.OpenForWriting());
        let oldHeaderOverflow = try!(self.writeHeader(&mut st, &mut space, &mut fs, newHeader));

        // the write of the new header has succeeded.

        waiting.segmentsInWaiting.remove(&newSegNum);
        mergeStuff.pendingMerges.remove(&newSegNum);
        for g in old {
            mergeStuff.merging.remove(&g);
        }

        let mut segmentsToBeFreed = segmentsBeingReplaced;
        {
            let mut cursors = try!(self.cursors.lock());
            let segmentsWithACursor : HashSet<SegmentNum> = cursors.cursors.iter().map(|t| {let (_,segnum) = t; *segnum}).collect();
            for g in segmentsWithACursor {
                // don't free anything that has a cursor
                match segmentsToBeFreed.remove(&g) {
                    Some(z) => {
                        cursors.zombies.insert(g, z);
                    },
                    None => {
                    },
                }
            }
        }
        let mut blocksToBeFreed = Vec::new();
        for info in segmentsToBeFreed.values() {
            blocksToBeFreed.push_all(&info.blocks);
        }
        match oldHeaderOverflow {
            Some(blk) => blocksToBeFreed.push(blk),
            None => (),
        }
        self.addFreeBlocks(&mut space, blocksToBeFreed);

        // note that we intentionally do not release the writeLock here.
        // you can change the segment list more than once while holding
        // the writeLock.  the writeLock gets released when you Dispose() it.
        Ok(())
    }

}

impl IPages for InnerPart {
    fn PageSize(&self) -> usize {
        self.pgsz
    }

    fn Begin(&self) -> Result<PendingSegment> {
        let mut lck = try!(self.nextSeg.lock());
        let p = PendingSegment::new(lck.nextSeg);
        lck.nextSeg = lck.nextSeg + 1;
        Ok(p)
    }

    fn GetBlock(&self, ps: &mut PendingSegment) -> Result<PageBlock> {
        let mut space = try!(self.space.lock());
        // specificSize=0 means we don't care how big of a block we get
        let blk = self.getBlock(&mut space, 0);
        ps.AddBlock(blk);
        Ok(blk)
    }

    fn End(&self, ps:PendingSegment, lastPage: PageNum) -> Result<SegmentNum> {
        let (g, blocks, leftovers) = ps.End(lastPage);
        let info = SegmentInfo {age: 0,blocks:blocks,root:lastPage};
        let mut waiting = try!(self.segmentsInWaiting.lock());
        let mut space = try!(self.space.lock());
        waiting.segmentsInWaiting.insert(g,info);
        //printfn "wrote %A: %A" g blocks
        match leftovers {
            Some(b) => self.addFreeBlocks(&mut space, vec![b]),
            None => ()
        }
        Ok(g)
    }

}

// ----------------------------------------------------------------

/*

type Database(_io:IDatabaseFile, _settings:DbSettings) =

    let doAutoMerge() = 
        if settings.AutoMergeEnabled then
            for level in 0 .. 3 do // TODO max merge level immediate
                match getPossibleMerge level settings.AutoMergeMinimumPages false with
                | Some f -> 
                    let g = f()
                    commitMerge g
                | None -> 
                    () // printfn "cannot merge level %d" level
            for level in 4 .. 7 do // TODO max merge level
                match getPossibleMerge level settings.AutoMergeMinimumPages false with
                | Some f -> 
                    f |> wrapMergeForLater |> startBackgroundMergeJob
                | None -> 
                    () // printfn "cannot merge level %d" level

        member this.ForgetWaitingSegments(guids:seq<Guid>) =
            // TODO need a test case for this
            let guidsAsSet = Seq.fold (fun acc g -> Set.add g acc) Set.empty guids
            let mySegmentsInWaiting = Map.filter (fun g _ -> Set.contains g guidsAsSet) segmentsInWaiting
            lock critSectionSegmentsInWaiting (fun () ->
                let remainingSegmentsInWaiting = Map.filter (fun g _ -> Set.contains g guidsAsSet |> not) segmentsInWaiting
                segmentsInWaiting <- remainingSegmentsInWaiting
            )
            lock critSectionCursors (fun () -> 
                let segmentsToBeFreed = Map.filter (fun g _ -> not (Map.containsKey g cursors)) mySegmentsInWaiting
                let blocksToBeFreed = Seq.fold (fun acc info -> info.blocks @ acc) List.empty (Map.values segmentsToBeFreed)
                addFreeBlocks blocksToBeFreed
            )

        member this.OpenSegmentCursor(g:Guid) =
            let csr = lock critSectionCursors (fun () ->
                let h = header
                getCursor h.segments g (Some checkForGoneSegment)
            )
            csr

        member this.GetFreeBlocks() = freeBlocks

        member this.PageSize() = pgsz

        member this.ListSegments() =
            (header.currentState, header.segments)

        member this.RequestWriteLock(timeout:int) =
            // TODO need a test case for this
            getWriteLock false timeout (Some doAutoMerge)

        member this.RequestWriteLock() =
            getWriteLock false (-1) (Some doAutoMerge)

    type PairBuffer(_db:IDatabase, _limit:int) =
        let db = _db
        let limit = _limit
        let d = System.Collections.Generic.Dictionary<byte[],Blob>()
        let mutable segs = []
        let emptyByteArray:byte[] = Array.empty
        let emptyBlobValue = Blob.Array emptyByteArray

        member this.Flush() =
            if d.Count > 0 then
                let g = db.WriteSegment(d)
                segs <- g :: segs
                d.Clear()

        member this.AddPair(k:byte[], v:Blob) =
            // TODO dictionary deals with byte[] keys by reference.
            d.[k] <- v
            if d.Count >= limit then
                this.Flush()

        member this.AddEmptyKey(k:byte[]) =
            this.AddPair(k, emptyBlobValue)

        member this.Commit(tx:IWriteLock) =
            tx.CommitSegments segs
            segs <- []
*/

#[cfg(test)]
mod tests {
    use std;
    use super::Result;

    #[test]
    fn it_works() {
    }

    #[test]
    #[ignore]
    fn quick() {
        fn tempfile(base: &str) -> String {
            fn tid() -> String {
                // TODO use the rand crate
                fn bytes() -> std::io::Result<[u8;16]> {
                    let mut f = try!(std::fs::OpenOptions::new()
                            .read(true)
                            .open("/dev/urandom"));
                    let mut ba = [0;16];
                    try!(super::utils::ReadFully(&mut f, &mut ba));
                    Ok(ba)
                }

                fn to_hex_string(ba: &[u8]) -> String {
                    let strs: Vec<String> = ba.iter()
                        .map(|b| format!("{:02X}", b))
                        .collect();
                    strs.connect("")
                }

                let ba = bytes().unwrap();
                to_hex_string(&ba)
            }

            std::fs::create_dir("tmp");
            let file = "tmp/".to_string() + base + "_" + &tid();
            file
        }

        fn f() -> Result<()> {
            //println!("running");
            let db = try!(super::db::new(tempfile("quick"), super::DEFAULT_SETTINGS));

            const NUM : usize = 100000;

            let mut a = Vec::new();
            for i in 0 .. 10 {
                let g = try!(db.WriteSegmentFromSortedSequence(super::GenerateNumbers {cur: i * NUM, end: (i+1) * NUM, step: i+1}));
                a.push(g);
            }
            {
                let lck = try!(db.GetWriteLock());
                try!(lck.commitSegments(a.clone()));
            }
            let g3 = try!(db.merge(0, 2, None));
            assert!(g3.is_some());
            let g3 = g3.unwrap();
            {
                let lck = try!(db.GetWriteLock());
                try!(lck.commitMerge(g3));
            }

            Ok(())
        }
        assert!(f().is_ok());
    }

}

pub struct GenerateNumbers {
    pub cur: usize,
    pub end: usize,
    pub step: usize,
}

impl Iterator for GenerateNumbers {
    type Item = Result<kvp>;
    // TODO allow the number of digits to be customized?
    fn next(&mut self) -> Option<Result<kvp>> {
        if self.cur > self.end {
            None
        }
        else {
            let k = format!("{:08}", self.cur).into_bytes().into_boxed_slice();
            let v = format!("{}", self.cur * 2).into_bytes().into_boxed_slice();
            let r = kvp{Key:k, Value:Blob::Array(v)};
            self.cur = self.cur + self.step;
            Some(Ok(r))
        }
    }
}

pub struct GenerateWeirdPairs {
    pub cur: usize,
    pub end: usize,
    pub klen: usize,
    pub vlen: usize,
}

impl Iterator for GenerateWeirdPairs {
    type Item = Result<kvp>;
    fn next(&mut self) -> Option<Result<kvp>> {
        if self.cur > self.end {
            None
        }
        else {
            fn get_weird(i: usize) -> u8 {
                let f = i as f64;
                let f = f.sin() * 1000.0;
                let f = f.abs();
                let f = f.floor() as u32;
                let f = f & 0xff;
                let f = f as u8;
                f
            }

            let mut k = Vec::new();
            for i in 0 .. self.klen {
                k.push(get_weird(i + self.cur));
            }
            let k = k.into_boxed_slice();

            let mut v = Vec::new();
            for i in 0 .. self.vlen {
                v.push(get_weird(i * 2 + self.cur));
            }
            let v = v.into_boxed_slice();

            let r = kvp{Key:k, Value:Blob::Array(v)};
            self.cur = self.cur + 1;
            Some(Ok(r))
        }
    }
}

// TODO the following can be removed at some point.  it is here
// now only because the test suite has not yet been adapted to use
// KeyRef/ValueRef.
impl<'a> LivingCursor<'a> {
    pub fn Key(&self) -> Result<Box<[u8]>> {
        let k = try!(self.KeyRef());
        let k = k.into_boxed_slice();
        Ok(k)
    }

    pub fn Seek(&mut self, k: &[u8], sop:SeekOp) -> Result<SeekResult> {
        let k2 = KeyRef::for_slice(k);
        let r = self.SeekRef(&k2, sop);
        println!("{:?}", r);
        r
    }

}

// ----------------------------------------------------------------
// Stuff below is the beginning of porting stuff from Elmo
// ----------------------------------------------------------------

/*
    Copyright 2015 Zumero, LLC

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

pub struct sqlite4_num {
    neg: bool,
    approx: bool,
    e: i16,
    m: u64,
}

impl sqlite4_num {
    const SQLITE4_MX_EXP: i16 = 999;
    const SQLITE4_NAN_EXP: i16 = 2000;

    const NAN: sqlite4_num =
        sqlite4_num
        {
            neg: false,
            approx: true,
            e: sqlite4_num::SQLITE4_NAN_EXP,
            m: 0,
        };
    const POS_INF: sqlite4_num = sqlite4_num {m: 1, .. sqlite4_num::NAN};
    const NEG_INF: sqlite4_num = sqlite4_num {neg: true, .. sqlite4_num::POS_INF};
    const ZERO: sqlite4_num =
        sqlite4_num
        {
            neg: false,
            approx: false,
            e: 0,
            m: 0,
        };

    fn from_f64(d: f64) -> sqlite4_num {
        // TODO probably this function should be done by decoding the bits
        if d.is_nan() {
            sqlite4_num::NAN
        } else if d.is_sign_positive() && d.is_infinite() {
            sqlite4_num::POS_INF
        } else if d.is_sign_negative() && d.is_infinite() {
            sqlite4_num::NEG_INF
        } else if d==0.0 {
            sqlite4_num::ZERO
        } else {
            let LARGEST_UINT64 = u64::max_value();
            let TENTH_MAX = LARGEST_UINT64 / 10;
            let large = LARGEST_UINT64 as f64;
            let large10 = TENTH_MAX as f64;
            let neg = d<0.0;
            let mut d = if neg { -d } else { d };
            let mut e = 0;
            while d>large || (d>1.0 && d==((d as i64) as f64)) {
                d = d / 10.0;
                e = e + 1;
            }
            while d<large10 && d != ((d as i64) as f64) {
                d = d * 10.0;
                e = e - 1;
            }
            sqlite4_num
            {
                neg: neg,
                approx: true,
                e: e as i16,
                m: d as u64,
            }
        }
    }

    fn is_inf(&self) -> bool {
        (self.e > sqlite4_num::SQLITE4_MX_EXP) && (self.m != 0)
    }

    fn is_nan(&self) -> bool{
        (self.e > sqlite4_num::SQLITE4_MX_EXP) && (self.m == 0)
    }

    fn from_i64(n: i64) -> sqlite4_num {
        sqlite4_num
        {
            neg: n<0,
            approx: false,
            m: if n>=0 { (n as u64) } else if n != i64::min_value() { ((-n) as u64) } else { 1 + (i64::max_value() as u64) },
            e: 0,
        }
    }

    fn normalize(&self) -> sqlite4_num {
        let mut m = self.m;
        let mut e = self.e;

        while (m % 10) == 0 {
            e = e + 1;
            m = m / 10;
        }

        sqlite4_num {m: m, e: e, .. *self}
    }

    fn encode_for_index(&self, w: &mut Vec<u8>) {
        // TODO in sqlite4, the first byte of this encoding 
        // is designed to mesh with the
        // overall type order byte.

        if self.m == 0 {
            if self.is_nan() {
                w.push(0x06u8);
            } else {
                w.push(0x15u8);
            }
        } else if self.is_inf() {
            if self.neg {
                w.push(0x07u8);
            } else {
                w.push(0x23u8);
            }
        } else {
            let num = self.normalize();
            let mut m = num.m;
            let mut e = num.e;
            let mut iDigit;
            let mut aDigit = [0; 12];

            if (num.e%2) != 0 {
                aDigit[0] = (10 * (m % 10)) as u8;
                m = m / 10;
                e = e - 1;
                iDigit = 1;
            } else {
                iDigit = 0;
            }

            while m != 0 {
                aDigit[iDigit] = (m % 100) as u8;
                iDigit = iDigit + 1;
                m = m / 100;
            }

            e = (iDigit as i16) + (e/2);

            fn push_u16_be(w: &mut Vec<u8>, e: u16) {
                w.push(((e>>8) & 0xff_u16) as u8);
                w.push(((e>>0) & 0xff_u16) as u8);
            }

            if e>= 11 {
                if ! num.neg {
                    w.push(0x22u8);
                    push_u16_be(w, e as u16);
                } else {
                    w.push(0x08u8);
                    push_u16_be(w, !e as u16);
                }
            } else if e>=0 {
                if ! num.neg {
                    w.push(0x17u8+(e as u8));
                } else {
                    w.push(0x13u8-(e as u8));
                }
            } else {
                if ! num.neg {
                    w.push(0x16u8);
                    push_u16_be(w, !((-e) as u16));
                } else {
                    w.push(0x14u8);
                    push_u16_be(w, (-e) as u16);
                }
            }

            while iDigit>0 {
                iDigit = iDigit - 1;
                let mut d = aDigit[iDigit] * 2u8;
                if iDigit != 0 { d = d | 0x01u8; }
                if num.neg { d = !d; }
                w.push(d)
            }
        }
    }

}

enum BsonValue {
    BDouble(f64),
    BString(String),
    BInt64(i64),
    BInt32(i32),
    BUndefined,
    BObjectID([u8; 12]),
    BNull,
    BRegex(String, String),
    BJSCode(String),
    BJSCodeWithScope(String),
    BBinary(u8, Box<[u8]>),
    BMinKey,
    BMaxKey,
    BDateTime(i64),
    BTimeStamp(i64),
    BBoolean(bool),
    BArray(Vec<BsonValue>),
    BDocument(Vec<(String, BsonValue)>),
}

fn vec_push_f64_le(v: &mut Vec<u8>, f: f64) {
    let mut buf = [0; 8];
    write_f64_le(&mut buf, f);
    v.push_all(&buf);
}

fn vec_push_i64_le(v: &mut Vec<u8>, f: i64) {
    let mut buf = [0; 8];
    write_i64_le(&mut buf, f);
    v.push_all(&buf);
}

fn vec_push_i32_le(v: &mut Vec<u8>, f: i32) {
    let mut buf = [0; 4];
    write_i32_le(&mut buf, f);
    v.push_all(&buf);
}

fn vec_push_c_string(v: &mut Vec<u8>, s: &str) {
    // TODO
}

fn vec_push_bson_string(v: &mut Vec<u8>, s: &str) {
    // TODO
}

impl BsonValue {
    fn getTypeNumber_u8(&self) -> u8 {
        match self {
            &BsonValue::BDouble(_) => 1,
            &BsonValue::BString(_) => 2,
            &BsonValue::BDocument(_) => 3,
            &BsonValue::BArray(_) => 4,
            &BsonValue::BBinary(_, _) => 5,
            &BsonValue::BUndefined => 6,
            &BsonValue::BObjectID(_) => 7,
            &BsonValue::BBoolean(_) => 8,
            &BsonValue::BDateTime(_) => 9,
            &BsonValue::BNull => 10,
            &BsonValue::BRegex(_, _) => 11,
            &BsonValue::BJSCode(_) => 13,
            &BsonValue::BJSCodeWithScope(_) => 15,
            &BsonValue::BInt32(_) => 16,
            &BsonValue::BTimeStamp(_) => 17,
            &BsonValue::BInt64(_) => 18,
            &BsonValue::BMinKey => 255, // NOTE
            &BsonValue::BMaxKey => 127,
        }
    }

    fn getTypeOrder(&self) -> i32 {
        // same numbers as canonicalizeBSONType()
        match self {
            &BsonValue::BUndefined => 0,
            &BsonValue::BNull => 5,
            &BsonValue::BDouble(_) => 10,
            &BsonValue::BInt64(_) => 10,
            &BsonValue::BInt32(_) => 10,
            &BsonValue::BString(_) => 15,
            &BsonValue::BDocument(_) => 20,
            &BsonValue::BArray(_) => 25,
            &BsonValue::BBinary(_, _) => 30,
            &BsonValue::BObjectID(_) => 35,
            &BsonValue::BBoolean(_) => 40,
            &BsonValue::BDateTime(_) => 45,
            &BsonValue::BTimeStamp(_) => 47,
            &BsonValue::BRegex(_, _) => 50,
            &BsonValue::BJSCode(_) => 60,
            &BsonValue::BJSCodeWithScope(_) => 65,
            &BsonValue::BMinKey => -1,
            &BsonValue::BMaxKey => 127,
        }
    }

    //#[cfg(bson)]
    fn toBinary(&self, w: &mut Vec<u8>) {
        match self {
            &BsonValue::BDouble(f) => vec_push_f64_le(w, f),
            &BsonValue::BInt32(n) => vec_push_i32_le(w, n),
            &BsonValue::BDateTime(n) => vec_push_i64_le(w, n),
            &BsonValue::BTimeStamp(n) => vec_push_i64_le(w, n),
            &BsonValue::BInt64(n) => vec_push_i64_le(w, n),
            &BsonValue::BString(ref s) => vec_push_bson_string(w, &s),
            &BsonValue::BObjectID(ref a) => w.push_all(a),
            &BsonValue::BBoolean(b) => if b { w.push(1u8) } else { w.push(0u8) },
            &BsonValue::BNull => (),
            &BsonValue::BMinKey => (),
            &BsonValue::BMaxKey => (),
            &BsonValue::BRegex(ref expr, ref opt) => {
                vec_push_c_string(w, &expr); 
                vec_push_c_string(w, &opt);
            },
            &BsonValue::BUndefined => (),
            &BsonValue::BJSCode(ref s) => vec_push_bson_string(w, &s),
            &BsonValue::BJSCodeWithScope(ref s) => panic!("TODO write BJSCodeWithScope"),
            &BsonValue::BBinary(subtype, ref ba) => {
                vec_push_i32_le(w, ba.len() as i32);
                w.push(subtype);
                w.push_all(&ba);
            },
            &BsonValue::BArray(ref vals) => {
                let start = w.len();
                // placeholder for length
                vec_push_i32_le(w, 0);
                for (i, vsub) in vals.iter().enumerate() {
                    w.push(vsub.getTypeNumber_u8());
                    let s = format!("{}", i);
                    vec_push_c_string(w, &s);
                    vsub.toBinary(w);
                }
                w.push(0u8);
                let len = w.len() - start;
                write_i32_le(&mut w[start .. start + 4], len as i32);
            },
            &BsonValue::BDocument(ref pairs) => {
                // placeholder for length
                let start = w.len();
                vec_push_i32_le(w, 0);
                for t in pairs.iter() {
                    let (ref ksub, ref vsub) = *t;
                    w.push(vsub.getTypeNumber_u8());
                    vec_push_c_string(w, &ksub);;
                    vsub.toBinary(w);
                }
                w.push(0u8);
                let len = w.len() - start;
                write_i32_le(&mut w[start .. start + 4], len as i32);
            },
        }
    }
}

