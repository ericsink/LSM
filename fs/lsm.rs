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

#![feature(collections)]
#![feature(box_syntax)]

use std::io;
use std::io::Seek;
use std::io::Read;
use std::io::Write;
use std::io::SeekFrom;

const size_i32 :usize = 4; // TODO
const size_i16 :usize = 2; // TODO

pub enum Blob {
    Stream(Box<Read>),
    Array(Box<[u8]>),
    Tombstone,
}

pub struct kvp {
    Key : Box<[u8]>,
    Value : Blob,
}

// TODO make this a trait?
pub struct IPendingSegment {
    unused : i32
}

pub struct PageBlock {
    firstPage : usize,
    lastPage : usize,
}

impl PageBlock {
    fn new(first : usize, last : usize) -> PageBlock {
        PageBlock { firstPage:first, lastPage:last }
    }

    fn CountPages(&self) -> usize {
        self.lastPage - self.firstPage + 1
    }
}

pub struct Guid {
    //a : [u8; 16]
    hack : i32
}

// TODO return Result
pub trait IPages {
    fn PageSize(&self) -> usize;
    fn Begin(&mut self) -> IPendingSegment;
    fn GetBlock(&mut self, token:&IPendingSegment) -> PageBlock;
    fn End(&mut self, token:IPendingSegment, page:usize) -> Guid;
}

#[derive(PartialEq,Copy,Clone)]
enum SeekOp {
    SEEK_EQ = 0,
    SEEK_LE = 1,
    SEEK_GE = 2,
}

// TODO return Result
trait ICursor : Drop {
    fn Seek(&mut self, k:&[u8], sop:SeekOp);
    fn First(&mut self);
    fn Last(&mut self);
    fn Next(&mut self);
    fn Prev(&mut self);
    fn IsValid(&self) -> bool;

    // TODO we wish Key() could return a reference, but the lifetime
    // would need to be "until the next call", and Rust can't really
    // do that.
    fn Key(&self) -> Box<[u8]>; 

    // TODO similarly with Value().  When the Blob is an array, we would
    // prefer to return a reference to the bytes in the page.
    fn Value(&self) -> Blob;

    fn ValueLength(&self) -> i32; // because a negative length is a tombstone
    fn KeyCompare(&self, k:&[u8]) -> i32;

    fn CountKeysForward(&mut self) -> u32 {
        let mut i = 0;
        self.First();
        while self.IsValid() {
            i = i + 1;
            self.Next();
        }
        i
    }

    fn CountKeysBackward(&mut self) -> u32 {
        let mut i = 0;
        self.Last();
        while self.IsValid() {
            i = i + 1;
            self.Prev();
        }
        i
    }
}

impl Iterator for ICursor {
    type Item = kvp;
    fn next(& mut self) -> Option<kvp> {
        if self.IsValid() {
            return Some(kvp{Key:self.Key(), Value:self.Value()})
        } else {
            return None;
        }
    }
}

// TODO return Result
trait IWriteLock : Drop {
    fn CommitSegments(Iterator<Item=Guid>);
    fn CommitMerge(&Guid);
}

trait SeekRead : Seek + Read {
}

trait IDatabaseFile {
    fn OpenForReading() -> SeekRead;
    fn OpenForWriting() -> SeekRead;
}

struct DbSettings {
    AutoMergeEnabled : bool,
    AutoMergeMinimumPages : i32,
    DefaultPageSize : usize,
    PagesPerBlock : u32,
}

struct SegmentInfo {
    root : usize,
    age : i32,
    blocks : Vec<PageBlock>
}

trait IDatabase : Drop {
    fn WriteSegmentFromSortedSequence(q:Iterator<Item=kvp>) -> Guid;
    //fn WriteSegment : System.Collections.Generic.IDictionary<byte[],Stream> -> Guid;
    //fn WriteSegment : System.Collections.Generic.IDictionary<byte[],Blob> -> Guid;
    fn ForgetWaitingSegments(s:Iterator<Item=Guid>);

    fn GetFreeBlocks() -> Iterator<Item=PageBlock>;
    fn OpenCursor() -> ICursor; // why do we have to specify Item here?  and what lifetime?
    fn OpenSegmentCursor(Guid) ->ICursor;
    // TODO consider name such as OpenLivingCursorOnCurrentState()
    // TODO consider OpenCursorOnSegmentsInWaiting(seq<Guid>)
    // TODO consider ListSegmentsInCurrentState()
    // TODO consider OpenCursorOnSpecificSegment(seq<Guid>)

    // fn ListSegments : unit -> (Guid list)*Map<Guid,SegmentInfo>
    fn PageSize() -> usize;

    // fn RequestWriteLock : int->Async<IWriteLock>
    // fn RequestWriteLock : unit->Async<IWriteLock>

    // fn Merge : int*int*bool -> Async<Guid list> option
    // fn BackgroundMergeJobs : unit->Async<Guid list> list // TODO have Auto in the name of this?
}

mod utils {
    use std::io;
    use std::io::Seek;
    use std::io::Read;
    use std::io::Write;
    use std::io::SeekFrom;

    pub fn SeekPage(strm:&mut Seek, pageSize:usize, pageNumber:usize) {
        if 0==pageNumber { panic!("invalid page number") }
        let pos = (pageNumber - 1) * pageSize;
        strm.seek(SeekFrom::Start(pos as u64));
    }

    pub fn ReadFully(strm:&mut Read, buf: &mut [u8]) -> io::Result<usize> {
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
    pub fn Compare (x:&[u8], y:&[u8]) -> i32 {
        let xlen = x.len();
        let ylen = y.len();
        let len = if xlen<ylen { xlen } else { ylen };
        let mut i = 0;
        while i<len {
            let c = (x[i] as i32) - (y[i] as i32);
            if c != 0 {
                return c;
            }
            else {
                i = i + 1;
            }
        }
        (xlen - ylen) as i32
    }

    pub fn CompareWithPrefix (prefix:&[u8], x:&[u8], y:&[u8]) -> i32 {
        let plen = prefix.len();
        let xlen = x.len();
        let ylen = y.len();
        let len = if xlen<ylen { xlen } else { ylen };
        let mut i = 0;
        while i<len {
            let xval = 
                if i<plen {
                    prefix[i]
                } else {
                    x[i - plen]
                };
            let c = (xval as i32) - (y[i] as i32);
            if c != 0 {
                return c;
            }
            else {
                i = i + 1;
            }
        }
        (xlen - ylen) as i32
    }

    pub fn PrefixMatch (x:&[u8], y:&[u8], max:usize) -> usize {
        let xlen = x.len();
        let ylen = y.len();
        let len = if xlen<ylen { xlen } else { ylen };
        let lim = if len<max { len } else { max };
        let mut i = 0;
        while i<lim && x[i]==y[i] {
            i = i + 1;
        }
        i
    }

    fn StartsWith (x:&[u8], y:&[u8], max:usize) -> bool {
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
    pub fn SpaceNeededFor(v:u64) -> usize {
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

    pub fn read (buf:&[u8], cur:usize) -> (usize,u64) {
        let a0 = buf[cur] as u64;
        if a0 <= 240u64 { 
            (cur+1, a0)
        } else if a0 <= 248u64 {
            let a1 = buf[cur+1] as u64;
            let r = (240u64 + 256u64 * (a0 - 241u64) + a1);
            (cur+2, r)
        } else if a0 == 249u64 {
            let a1 = buf[cur+1] as u64;
            let a2 = buf[cur+2] as u64;
            let r = (2288u64 + 256u64 * a1 + a2);
            (cur+3, r)
        } else if a0 == 250u64 {
            let a1 = buf[cur+1] as u64;
            let a2 = buf[cur+2] as u64;
            let a3 = buf[cur+3] as u64;
            let r = (a1<<16) | (a2<<8) | a3;
            (cur+4, r)
        } else if a0 == 251u64 {
            let a1 = buf[cur+1] as u64;
            let a2 = buf[cur+2] as u64;
            let a3 = buf[cur+3] as u64;
            let a4 = buf[cur+4] as u64;
            let r = (a1<<24) | (a2<<16) | (a3<<8) | a4;
            (cur+5, r)
        } else if a0 == 252u64 {
            let a1 = buf[cur+1] as u64;
            let a2 = buf[cur+2] as u64;
            let a3 = buf[cur+3] as u64;
            let a4 = buf[cur+4] as u64;
            let a5 = buf[cur+5] as u64;
            let r = (a1<<32) | (a2<<24) | (a3<<16) | (a4<<8) | a5;
            (cur+6, r)
        } else if a0 == 253u64 {
            let a1 = buf[cur+1] as u64;
            let a2 = buf[cur+2] as u64;
            let a3 = buf[cur+3] as u64;
            let a4 = buf[cur+4] as u64;
            let a5 = buf[cur+5] as u64;
            let a6 = buf[cur+6] as u64;
            let r = (a1<<40) | (a2<<32) | (a3<<24) | (a4<<16) | (a5<<8) | a6;
            (cur+7, r)
        } else if a0 == 254u64 {
            let a1 = buf[cur+1] as u64;
            let a2 = buf[cur+2] as u64;
            let a3 = buf[cur+3] as u64;
            let a4 = buf[cur+4] as u64;
            let a5 = buf[cur+5] as u64;
            let a6 = buf[cur+6] as u64;
            let a7 = buf[cur+7] as u64;
            let r = (a1<<48) | (a2<<40) | (a3<<32) | (a4<<24) | (a5<<16) | (a6<<8) | a7;
            (cur+8, r)
        } else {
            let a1 = buf[cur+1] as u64;
            let a2 = buf[cur+2] as u64;
            let a3 = buf[cur+3] as u64;
            let a4 = buf[cur+4] as u64;
            let a5 = buf[cur+5] as u64;
            let a6 = buf[cur+6] as u64;
            let a7 = buf[cur+7] as u64;
            let a8 = buf[cur+8] as u64;
            let r = (a1<<56) | (a2<<48) | (a3<<40) | (a4<<32) | (a5<<24) | (a6<<16) | (a7<<8) | a8;
            (cur+9, r)
        }
    }

    pub fn write (buf:&mut [u8], cur:usize, v:u64) -> usize {
        if v<=240u64 { 
            buf[cur] = v as u8;
            cur + 1
        } else if v<=2287u64 { 
            buf[cur] = ((v - 240u64) / 256u64 + 241u64) as u8;
            buf[cur+1] = ((v - 240u64) % 256u64) as u8;
            cur + 2
        } else if v<=67823u64 { 
            buf[cur] = 249u8;
            buf[cur+1] = ((v - 2288u64) / 256u64) as u8;
            buf[cur+2] = ((v - 2288u64) % 256u64) as u8;
            cur + 3
        } else if v<=16777215u64 { 
            buf[cur] = 250u8;
            buf[cur+1] = (v >> 16) as u8;
            buf[cur+2] = (v >>  8) as u8;
            buf[cur+3] = (v >>  0) as u8;
            cur + 4
        } else if v<=4294967295u64 { 
            buf[cur] = 251u8;
            buf[cur+1] = (v >> 24) as u8;
            buf[cur+2] = (v >> 16) as u8;
            buf[cur+3] = (v >>  8) as u8;
            buf[cur+4] = (v >>  0) as u8;
            cur + 5
        } else if v<=1099511627775u64 { 
            buf[cur] = 252u8;
            buf[cur+1] = (v >> 32) as u8;
            buf[cur+2] = (v >> 24) as u8;
            buf[cur+3] = (v >> 16) as u8;
            buf[cur+4] = (v >>  8) as u8;
            buf[cur+5] = (v >>  0) as u8;
            cur + 6
        } else if v<=281474976710655u64 { 
            buf[cur] = 253u8;
            buf[cur+1] = (v >> 40) as u8;
            buf[cur+2] = (v >> 32) as u8;
            buf[cur+3] = (v >> 24) as u8;
            buf[cur+4] = (v >> 16) as u8;
            buf[cur+5] = (v >>  8) as u8;
            buf[cur+6] = (v >>  0) as u8;
            cur + 7
        } else if v<=72057594037927935u64 { 
            buf[cur] = 254u8;
            buf[cur+1] = (v >> 48) as u8;
            buf[cur+2] = (v >> 40) as u8;
            buf[cur+3] = (v >> 32) as u8;
            buf[cur+4] = (v >> 24) as u8;
            buf[cur+5] = (v >> 16) as u8;
            buf[cur+6] = (v >>  8) as u8;
            buf[cur+7] = (v >>  0) as u8;
            cur + 8
        } else {
            buf[cur] = 255u8;
            buf[cur+1] = (v >> 56) as u8;
            buf[cur+2] = (v >> 48) as u8;
            buf[cur+3] = (v >> 40) as u8;
            buf[cur+4] = (v >> 32) as u8;
            buf[cur+5] = (v >> 24) as u8;
            buf[cur+6] = (v >> 16) as u8;
            buf[cur+7] = (v >>  8) as u8;
            buf[cur+8] = (v >>  0) as u8;
            cur + 9
        }
    }
}

/*
fn push_i32_be(v:& mut Vec<u8>, i:i32)
{
    v.push((i>>24) as u8);
    v.push((i>>16) as u8);
    v.push((i>>8) as u8);
    v.push((i>>0) as u8);
}

fn push_i64_le(v:& mut Vec<u8>, i:i64)
{
    v.push((i>>0) as u8);
    v.push((i>>8) as u8);
    v.push((i>>16) as u8);
    v.push((i>>24) as u8);
    v.push((i>>32) as u8);
    v.push((i>>40) as u8);
    v.push((i>>48) as u8);
    v.push((i>>56) as u8);
}

fn push_i32_le(v:& mut Vec<u8>, i:i32)
{
    v.push((i>>0) as u8);
    v.push((i>>8) as u8);
    v.push((i>>16) as u8);
    v.push((i>>24) as u8);
}

fn push_cstring(v:& mut Vec<u8>, s:&String)
{
    v.push_all(s.as_bytes());
    v.push(0 as u8);
}
*/

fn write_i32_le(v:& mut [u8], i:i32)
{
    v[0] = (i>>0) as u8;
    v[1] = (i>>8) as u8;
    v[2] = (i>>16) as u8;
    v[3] = (i>>24) as u8;
}

fn write_i32_be(v:& mut [u8], i:i32)
{
    v[0] = (i>>24) as u8;
    v[1] = (i>>16) as u8;
    v[2] = (i>>8) as u8;
    v[3] = (i>>0) as u8;
}

fn read_i32_be(v:&[u8]) -> i32
{
    let a0 = v[0] as u64;
    let a1 = v[1] as u64;
    let a2 = v[2] as u64;
    let a3 = v[3] as u64;
    let r = (a0 << 24) | (a1 << 16) | (a2 << 8) | (a3 << 0);
    // assert r fits in a 32 bit signed int
    r as i32
}

fn read_i16_be(v:&[u8]) -> i16
{
    let a0 = v[0] as u64;
    let a1 = v[1] as u64;
    let r = (a0 << 8) | (a1 << 0);
    // assert r fits in a 16 bit signed int
    r as i16
}

fn write_i16_be(v:& mut [u8], i:i16)
{
    v[0] = (i>>8) as u8;
    v[1] = (i>>0) as u8;
}

struct PageBuilder {
    cur : usize,
    buf : Box<[u8]>,
}

// TODO bundling cur with the buf almost seems sad, because there are
// cases where we want buf to be mutable but not cur.  :-)

impl PageBuilder {
    fn new(pgsz : usize) -> PageBuilder { 
        let mut ba = vec![0;pgsz].into_boxed_slice();
        PageBuilder { cur:0, buf:ba } 
    }

    fn Reset(&mut self) {
        self.cur = 0;
    }

    fn Write(&self, strm:&mut Write) {
        strm.write_all(&*self.buf);
    }

    fn PageSize(&self) -> usize {
        self.buf.len()
    }

    fn Buffer(&self) -> &[u8] {
        &self.buf
    }
    
    fn Position(&self) -> usize {
        self.cur
    }

    fn Available(&self) -> usize {
        self.buf.len() - self.cur
    }

    fn SetPageFlag(&mut self, x:u8) {
        self.buf[1] = self.buf[1] | (x);
    }

    fn PutByte(&mut self, x:u8) {
        self.buf[self.cur] = x;
        self.cur = self.cur + 1;
    }

    fn PutStream2(&mut self, s:&mut Read, len:usize) -> io::Result<usize> {
        let n = try!(utils::ReadFully(s, &mut self.buf[self.cur .. self.cur + len]));
        self.cur = self.cur + n;
        let res : io::Result<usize> = Ok(n);
        res
    }

    fn PutStream(&mut self, s:&mut Read, len:usize) -> io::Result<usize> {
        let n = try!(self.PutStream2(s, len));
        // TODO if n != len fail
        let res : io::Result<usize> = Ok(len);
        res
    }

    fn PutArray(&mut self, ba:&[u8]) {
        // TODO this can't be the best way to copy a slice
        for i in 0..ba.len() {
            self.buf[self.cur + i] = ba[i];
        }
        self.cur = self.cur + ba.len();
    }

    // TODO should be u32
    fn PutInt32(&mut self, ov:i32) {
        let at = self.cur;
        write_i32_be(&mut self.buf[at .. at+size_i32], ov);
        self.cur = self.cur + size_i32;
    }

    // TODO should be u32
    fn SetSecondToLastInt32(&mut self, page:i32) {
        let len = self.buf.len();
        let at = len - 2 * size_i32;
        if self.cur > at { panic!("SetSecondToLastInt32 is squashing data"); }
        write_i32_be(&mut self.buf[at .. at+size_i32], page);
    }

    // TODO should be u32
    fn SetLastInt32(&mut self, page:i32) {
        let len = self.buf.len();
        let at = len - 1 * size_i32;
        if self.cur > at { panic!("SetLastInt32 is squashing data"); }
        write_i32_be(&mut self.buf[at .. at+size_i32], page);
    }

    fn PutInt16(&mut self, ov:i16) {
        let at = self.cur;
        write_i16_be(&mut self.buf[at .. at+size_i16], ov);
        self.cur = self.cur + size_i16;
    }

    fn PutInt16At(&mut self, at:usize, ov:i16) {
        write_i16_be(&mut self.buf[at .. at+size_i16], ov);
    }

    fn PutVarint(&mut self, ov:u64) {
        self.cur = Varint::write(&mut *self.buf, self.cur, ov);
    }

}

struct PageReader {
    cur : usize,
    buf : Box<[u8]>,
}

impl PageReader {

    pub fn Position(&self) -> usize {
        self.cur
    }

    fn PageSize(&self) -> usize {
        self.buf.len()
    }

    fn SetPosition(&mut self, x:usize) {
        self.cur = x;
    }

    fn Read(&mut self, strm:&mut Read) -> io::Result<usize> {
        utils::ReadFully(strm, &mut self.buf)
    }

    // TODO member this.Read(s:Stream, off, len) = utils.ReadFully(s, buf, off, len)

    fn Reset(&mut self) {
        self.cur = 0;
    }

    fn Compare(&self, len:usize, other:&[u8]) ->i32 {
        let slice = &self.buf[self.cur .. self.cur + len];
        bcmp::Compare(slice, other)
    }

    fn CompareWithPrefix(&self, prefix:&[u8], len:usize, other:&[u8]) ->i32 {
        let slice = &self.buf[self.cur .. self.cur + len];
        bcmp::CompareWithPrefix(prefix, slice, other)
    }

    fn PageType(&self) -> u8 {
        self.buf[0]
    }

    fn Skip(&mut self, len:usize) {
        self.cur = self.cur + len;
    }

    fn GetByte(&mut self) -> u8 {
        let r = self.buf[self.cur];
        self.cur = self.cur + 1;
        r
    }

    fn GetInt32(&mut self) -> i32 {
        let at = self.cur;
        let r = read_i32_be(&self.buf[at .. at+size_i32]);
        self.cur = self.cur + size_i32;
        r
    }

    fn GetInt32At(&self, at:usize) -> i32 {
        read_i32_be(&self.buf[at .. at+size_i32])
    }

    fn CheckPageFlag(&self, f:u8) -> bool {
        0 != (self.buf[1] & f)
    }

    fn GetSecondToLastInt32(&self) -> i32 {
        let len = self.buf.len();
        let at = len - 2 * size_i32;
        self.GetInt32At(at)
    }

    fn GetLastInt32(&self) -> i32 {
        let len = self.buf.len();
        let at = len - 1 * size_i32;
        self.GetInt32At(at)
    }

    fn GetInt16(&mut self) -> i16 {
        let at = self.cur;
        let r = read_i16_be(&self.buf[at .. at+size_i16]);
        self.cur = self.cur + size_i16;
        r
    }

    fn GetIntoArray(&self, a : &mut [u8]) {
        // TODO copy slice
        for i in 0 .. a.len() {
            a[i] = self.buf[self.cur + i];
        }
    }

    /*
    fn GetArray(&mut self, len : usize) -> Box<[u8]> {
        let at = self.cur;
        let ba = &self.buf[at .. at + len];
        self.cur = self.cur + len;
        Box::new(*ba)
    }
    */

    fn GetVarint(&mut self) -> u64 {
        let (newCur, v) = Varint::read(&*self.buf, self.cur);
        self.cur = newCur;
        v
    }

}

#[derive(PartialEq,Copy,Clone)]
enum Direction {
    FORWARD = 0,
    BACKWARD = 1,
    WANDERING = 2,
}

struct MultiCursor { 
    subcursors : Box<[Box<ICursor>]>, 
    cur : Option<usize>,
    dir : Direction,
}

impl MultiCursor {
    fn find(&self, compare_func : &Fn(&ICursor,&ICursor) -> i32) -> Option<usize> {
        if self.subcursors.is_empty() {
            None
        } else {
            let mut res = None::<usize>;
            for i in 0 .. self.subcursors.len() {
                match res {
                    Some(winning) => {
                        let x = &self.subcursors[i];
                        let y = &self.subcursors[winning];
                        let c = compare_func(&**x,&**y);
                        if c<0 {
                            res = Some(i)
                        }
                    },
                    None => {
                        res = Some(i)
                    }
                }
            }
            res
        }
    }

    fn findMin(&self) -> Option<usize> {
        let compare_func = |a:&ICursor,b:&ICursor| a.KeyCompare(&*b.Key());
        self.find(&compare_func)
    }

    fn findMax(&self) -> Option<usize> {
        let compare_func = |a:&ICursor,b:&ICursor| b.KeyCompare(&*a.Key());
        self.find(&compare_func)
    }

    fn Create(subs: Vec<Box<ICursor>>) -> MultiCursor {
        let s = subs.into_boxed_slice();
        MultiCursor { subcursors: s, cur : None, dir : Direction::WANDERING }
    }

}

impl Drop for MultiCursor {
    fn drop(&mut self) {
        // TODO
        println!("Dropping!");
    }
}

impl ICursor for MultiCursor {
    fn IsValid(&self) -> bool {
        match self.cur {
            Some(i) => self.subcursors[i].IsValid(),
            None => false
        }
    }

    fn First(&mut self) {
        for i in 0 .. self.subcursors.len() {
            self.subcursors[i].First();
        }
        self.cur = self.findMin();
        self.dir = Direction::WANDERING; // TODO why?
    }

    fn Last(&mut self) {
        for i in 0 .. self.subcursors.len() {
            self.subcursors[i].Last();
        }
        self.cur = self.findMax();
        self.dir = Direction::WANDERING; // TODO why?
    }

    // the following members are designed to panic if called when
    // the cursor is not valid.
    // this matches the C# behavior and the expected behavior of ICursor.
    // don't call these methods without checking IsValid() first.

    fn Key(&self) -> Box<[u8]> {
        match self.cur {
            Some(icur) => self.subcursors[icur].Key(),
            None => panic!()
        }
    }

    fn KeyCompare(&self, k:&[u8]) -> i32 {
        match self.cur {
            Some(icur) => self.subcursors[icur].KeyCompare(k),
            None => panic!()
        }
    }

    fn Value(&self) -> Blob {
        match self.cur {
            Some(icur) => self.subcursors[icur].Value(),
            None => panic!()
        }
    }

    fn ValueLength(&self) -> i32 {
        match self.cur {
            Some(icur) => self.subcursors[icur].ValueLength(),
            None => panic!()
        }
    }

    fn Next(&mut self) {
        match self.cur {
            Some(icur) => {
                let k = self.subcursors[icur].Key();
                for j in 0 .. self.subcursors.len() {
                    let csr = &mut self.subcursors[j];
                    if (self.dir != Direction::FORWARD) && (icur != j) { 
                        (*csr).Seek (&*k, SeekOp::SEEK_GE); 
                    }
                    if csr.IsValid() && (0 == csr.KeyCompare(&*k)) { 
                        csr.Next(); 
                    }
                }
                self.cur = self.findMin();
                self.dir = Direction::FORWARD;
            },
            None => panic!()
        }
    }

    fn Prev(&mut self) {
        match self.cur {
            Some(icur) => {
                let k = self.subcursors[icur].Key();
                for j in 0 .. self.subcursors.len() {
                    let csr = &mut self.subcursors[j];
                    if (self.dir != Direction::BACKWARD) && (icur != j) { 
                        (*csr).Seek (&*k, SeekOp::SEEK_LE); 
                    }
                    if csr.IsValid() && (0 == csr.KeyCompare(&*k)) { 
                        csr.Prev(); 
                    }
                }
            },
            None => panic!()
        }
        self.cur = self.findMax();
        self.dir = Direction::BACKWARD;
    }

    fn Seek(&mut self, k:&[u8], sop:SeekOp) {
        self.cur = None;
        self.dir = Direction::WANDERING;
        let mut found = false;
        for j in 0 .. self.subcursors.len() {
            self.subcursors[j].Seek(k,sop);
            if self.cur.is_none() && self.subcursors[j].IsValid() && ( (SeekOp::SEEK_EQ == sop) || (0 == self.subcursors[j].KeyCompare (k)) ) { 
                self.cur = Some(j);
                found = true;
                break;
            }
        }
        if !found {
            match sop {
                SeekOp::SEEK_GE => {
                    self.cur = self.findMin();
                    if self.cur.is_some() { 
                        self.dir = Direction::FORWARD; 
                    }
                },
                SeekOp::SEEK_LE => {
                    self.cur = self.findMax();
                    if self.cur.is_some() { 
                        self.dir = Direction::BACKWARD; 
                    }
                },
                SeekOp::SEEK_EQ => ()
            }
        }
    }

}

struct LivingCursor { 
    chain : Box<ICursor>
}

impl LivingCursor {
    fn skipTombstonesForward(&mut self) {
        while self.chain.IsValid() && self.chain.ValueLength()<0 {
            self.chain.Next();
        }
    }

    fn skipTombstonesBackward(&mut self) {
        while self.chain.IsValid() && self.chain.ValueLength()<0 {
            self.chain.Prev();
        }
    }

    pub fn Create(ch : Box<ICursor>) -> LivingCursor {
        LivingCursor { chain : ch }
    }
}

impl Drop for LivingCursor {
    fn drop(&mut self) {
        // TODO
        println!("Dropping!");
    }
}

impl ICursor for LivingCursor {
    fn First(&mut self) {
        self.chain.First();
        self.skipTombstonesForward();
    }

    fn Last(&mut self) {
        self.chain.Last();
        self.skipTombstonesBackward();
    }

    fn Key(&self) -> Box<[u8]> {
        self.chain.Key()
    }

    fn Value(&self) -> Blob {
        self.chain.Value()
    }

    fn ValueLength(&self) -> i32 {
        self.chain.ValueLength()
    }

    fn IsValid(&self) -> bool {
        self.chain.IsValid() && self.chain.ValueLength() >= 0
    }

    fn KeyCompare(&self, k:&[u8]) -> i32 {
        self.chain.KeyCompare(k)
    }

    fn Next(&mut self) {
        self.chain.Next();
        self.skipTombstonesForward();
    }

    fn Prev(&mut self) {
        self.chain.Prev();
        self.skipTombstonesBackward();
    }

    fn Seek(&mut self, k:&[u8], sop:SeekOp) {
        self.chain.Seek(k, sop);
        match sop {
            SeekOp::SEEK_GE => self.skipTombstonesForward(),
            SeekOp::SEEK_LE => self.skipTombstonesBackward(),
            SeekOp::SEEK_EQ => (),
        }
    }

}

mod bt {

    use std::io::Write;
    use std::collections::HashMap;

    use super::PageBlock;

    // page types
    enum PageType {
        LEAF_NODE = 1,
        PARENT_NODE = 2,
        OVERFLOW_NODE = 3,
    }

    // flags on values
    enum ValueFlag {
        FLAG_OVERFLOW = 1,
        FLAG_TOMBSTONE = 2,
    }

    // flags on pages
    enum PageFlag {
        FLAG_ROOT_NODE = 1,
        FLAG_BOUNDARY_NODE = 2,
        FLAG_ENDS_ON_BOUNDARY = 3,
    }

    struct pgitem {
        page : usize,
        key : Box<[u8]>, // TODO reference instead of box?
        // TODO constructor impl ?
    }

    struct ParentState {
        sofar : usize,
        nextGeneration : Vec<pgitem>,
        blk : PageBlock,
    }

    // TODO gratuitously different names of the items in these
    // two unions

    enum KeyLocation {
        Inline,
        Overflow(usize),
    }

    enum ValueLocation {
        Tombstone,
        Buffer(Box<[u8]>), // TODO reference instead of box?
        Overflowed(usize,usize),
    }

    struct LeafPair {
        key : Box<[u8]>,
        kLoc : KeyLocation,
        vLoc : ValueLocation,
    }

    struct LeafState {
        sofarLeaf : usize,
        keys : Vec<Box<LeafPair>>,
        prevLeaf : usize,
        prefixLen : usize,
        firstLeaf : usize,
        leaves : Vec<pgitem>,
        blk : PageBlock,
    }

    use super::utils;
    use super::IPages;
    use super::kvp;
    use super::PageBuilder;
    use std::io;
    use std::io::Read;
    use std::io::Seek;
    use super::IPendingSegment;
    use super::Varint;
    use super::Blob;
    use super::bcmp;
    use super::Guid;
    use super::size_i32;

    pub fn CreateFromSortedSequenceOfKeyValuePairs<I,SeekWrite>(fs: &mut SeekWrite, 
                                                            pageManager: &mut IPages, 
                                                            source: I,
                                                           ) -> io::Result<(Guid,usize)> where I:Iterator<Item=kvp>, SeekWrite : Seek+Write {

        fn writeOverflow<SeekWrite>(startingBlock: PageBlock, 
                                    ba: &mut Read, 
                                    pageManager: &mut IPages, 
                                    fs: &mut SeekWrite
                                   ) -> io::Result<(usize,PageBlock)> where SeekWrite : Seek+Write {
            fn buildFirstPage(ba:&mut Read, pbFirstOverflow : &mut PageBuilder, pageSize : usize) -> io::Result<(usize,bool)> {
                pbFirstOverflow.Reset();
                pbFirstOverflow.PutByte(PageType::OVERFLOW_NODE as u8);
                pbFirstOverflow.PutByte(0u8); // starts 0, may be changed later
                let room = (pageSize - (2 + size_i32));
                // something will be put in lastInt32 later
                match pbFirstOverflow.PutStream2(ba, room) {
                    Ok(put) => Ok((put, put<room)),
                    Err(e) => Err(e),
                }
            };

            fn buildRegularPage(ba:&mut Read, pbOverflow : &mut PageBuilder, pageSize : usize) -> io::Result<(usize,bool)> {
                pbOverflow.Reset();
                let room = pageSize;
                match pbOverflow.PutStream2(ba, room) {
                    Ok(put) => Ok((put, put<room)),
                    Err(e) => Err(e),
                }
            };

            fn buildBoundaryPage(ba:&mut Read, pbOverflow : &mut PageBuilder, pageSize : usize) -> io::Result<(usize,bool)> {
                pbOverflow.Reset();
                let room = (pageSize - size_i32);
                // something will be put in lastInt32 before the page is written
                match pbOverflow.PutStream2(ba, room) {
                    Ok(put) => Ok((put, put<room)),
                    Err(e) => Err(e),
                }
            }

            fn writeRegularPages<SeekWrite>(max :usize, 
                                 sofar :usize, 
                                 pb : &mut PageBuilder, 
                                 fs : &mut SeekWrite, 
                                 ba : &mut Read, 
                                 pageSize : usize
                                 ) -> io::Result<(usize,usize,bool)> where SeekWrite : Seek+Write {
                let mut i = 0;
                loop {
                    if i < max {
                        let (put, finished) = try!(buildRegularPage(ba, pb, pageSize));
                        if put==0 {
                            return Ok((i, sofar, true));
                        } else {
                            let sofar = sofar + put;
                            pb.Write(fs);
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
                             pageSize: usize,
                             pbOverflow: &mut PageBuilder,
                             pbFirstOverflow: &mut PageBuilder,
                             pageManager: &mut IPages,
                             token: &IPendingSegment
                             ) -> io::Result<(usize,PageBlock)> where SeekWrite : Seek+Write {
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
                    let (putFirst,finished) = try!(buildFirstPage (ba, pbFirstOverflow, pageSize));
                    if putFirst==0 { 
                        return Ok((sofar, firstBlk));
                    } else {
                        // note that we haven't written the first page yet.  we may have to fix
                        // a couple of things before it gets written out.
                        let sofar = sofar + putFirst;
                        if firstBlk.firstPage == firstBlk.lastPage {
                            // the first page landed on a boundary.
                            // we can just set the flag and write it now.
                            pbFirstOverflow.SetPageFlag(PageFlag::FLAG_BOUNDARY_NODE as u8);
                            let blk = pageManager.GetBlock(token);
                            pbFirstOverflow.SetLastInt32(blk.firstPage as i32);
                            pbFirstOverflow.Write(fs);
                            utils::SeekPage(fs, pageSize, blk.firstPage);
                            if !finished {
                                loop_sofar = sofar;
                                loop_firstBlk = blk;
                            } else {
                                return Ok((sofar, blk));
                            }
                        } else {
                            let firstRegularPageNumber = (firstBlk.firstPage + 1) as usize;
                            if finished {
                                // the first page is also the last one
                                pbFirstOverflow.SetLastInt32(0); // offset to last used page in this block, which is this one
                                pbFirstOverflow.Write(fs);
                                return Ok((sofar, PageBlock::new(firstRegularPageNumber,firstBlk.lastPage)));
                            } else {
                                // we need to write more pages,
                                // until the end of the block,
                                // or the end of the stream, 
                                // whichever comes first

                                utils::SeekPage(fs, pageSize, firstRegularPageNumber);

                                // availableBeforeBoundary is the number of pages until the boundary,
                                // NOT counting the boundary page, and the first page in the block
                                // has already been accounted for, so we're just talking about data pages.
                                let availableBeforeBoundary = 
                                    if firstBlk.lastPage > 0 
                                        { (firstBlk.lastPage - firstRegularPageNumber) }
                                    else 
                                        { usize::max_value() }
                                    ;

                                let (numRegularPages, sofar, finished) = 
                                    try!(writeRegularPages(availableBeforeBoundary, sofar, pbOverflow, fs, ba, pageSize));

                                if finished {
                                    // go back and fix the first page
                                    pbFirstOverflow.SetLastInt32(numRegularPages as i32);
                                    utils::SeekPage(fs, pageSize, firstBlk.firstPage);
                                    pbFirstOverflow.Write(fs);
                                    // now reset to the next page in the block
                                    let blk = PageBlock::new(firstRegularPageNumber + numRegularPages, firstBlk.lastPage);
                                    utils::SeekPage(fs, pageSize, blk.firstPage);
                                    return Ok((sofar,blk));
                                } else {
                                    // we need to write out a regular page except with a
                                    // boundary pointer in it.  and we need to set
                                    // FLAG_ENDS_ON_BOUNDARY on the first
                                    // overflow page in this block.

                                    let (putBoundary,finished) = try!(buildBoundaryPage (ba, pbOverflow, pageSize));
                                    if putBoundary==0 {
                                        // go back and fix the first page
                                        pbFirstOverflow.SetLastInt32(numRegularPages as i32);
                                        utils::SeekPage(fs, pageSize, firstBlk.firstPage);
                                        pbFirstOverflow.Write(fs);

                                        // now reset to the next page in the block
                                        let blk = PageBlock::new(firstRegularPageNumber + numRegularPages, firstBlk.lastPage);
                                        utils::SeekPage(fs, pageSize, firstBlk.lastPage);
                                        return Ok((sofar,blk));
                                    } else {
                                        // write the boundary page
                                        let sofar = sofar + putBoundary;
                                        let blk = pageManager.GetBlock(token);
                                        pbOverflow.SetLastInt32(blk.firstPage as i32);
                                        pbOverflow.Write(fs);

                                        // go back and fix the first page
                                        pbFirstOverflow.SetPageFlag(PageFlag::FLAG_ENDS_ON_BOUNDARY as u8);
                                        pbFirstOverflow.SetLastInt32((numRegularPages + 1) as i32);
                                        utils::SeekPage(fs, pageSize, firstBlk.firstPage);
                                        pbFirstOverflow.Write(fs);

                                        // now reset to the first page in the next block
                                        utils::SeekPage(fs, pageSize, blk.firstPage);
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

            let pageSize = pageManager.PageSize();
            let token = pageManager.Begin();
            let mut pbFirstOverflow = PageBuilder::new(pageSize);
            let mut pbOverflow = PageBuilder::new(pageSize);

            writeOneBlock(0, startingBlock, fs, ba, pageSize, &mut pbOverflow, &mut pbFirstOverflow, pageManager, &token)
        }

        fn writeLeaves<I,SeekWrite>(leavesBlk:PageBlock,
                                    pageManager: &mut IPages,
                                    source: I,
                                    vbuf: &mut [u8],
                                    fs: &mut SeekWrite, 
                                    pb: &mut PageBuilder,
                                    token: &IPendingSegment,
                                    ) -> io::Result<(PageBlock,Vec<pgitem>,usize)> where I: Iterator<Item=kvp> , SeekWrite : Seek+Write {
            // 2 for the page type and flags
            // 4 for the prev page
            // 2 for the stored count
            // 4 for lastInt32 (which isn't in pb.Available)
            let LEAF_PAGE_OVERHEAD = 2 + 4 + 2 + 4;

            fn buildLeaf(st: &LeafState, pb: &mut PageBuilder) {
                pb.Reset();
                pb.PutByte(PageType::LEAF_NODE as u8);
                pb.PutByte(0u8); // flags
                pb.PutInt32 (st.prevLeaf as i32); // prev page num.
                // TODO prefixLen is one byte.  should it be two?
                pb.PutByte(st.prefixLen as u8);
                if st.prefixLen > 0 {
                    pb.PutArray(&st.keys[0].key[0 .. st.prefixLen]);
                }
                pb.PutInt16 (st.keys.len() as i16);
                for lp in &st.keys {
                    match lp.kLoc {
                        KeyLocation::Inline => {
                            pb.PutByte(0u8); // flags
                            pb.PutVarint(lp.key.len() as u64);
                            pb.PutArray(&lp.key[st.prefixLen .. lp.key.len()]);
                        },
                        KeyLocation::Overflow(kpage) => {
                            pb.PutByte(ValueFlag::FLAG_OVERFLOW as u8);
                            pb.PutVarint(lp.key.len() as u64);
                            pb.PutInt32(kpage as i32);
                        },
                    }
                    match lp.vLoc {
                        ValueLocation::Tombstone => {
                            pb.PutByte(ValueFlag::FLAG_TOMBSTONE as u8);
                        },
                        ValueLocation::Buffer (ref vbuf) => {
                            pb.PutByte(0u8);
                            pb.PutVarint(vbuf.len() as u64);
                            pb.PutArray(&vbuf);
                        },
                        ValueLocation::Overflowed (vlen,vpage) => {
                            pb.PutByte(ValueFlag::FLAG_OVERFLOW as u8);
                            pb.PutVarint(vlen as u64);
                            pb.PutInt32(vpage as i32);
                        },
                    }
                }
            }

            fn writeLeaf<SeekWrite>(st: &mut LeafState, 
                         isRootPage: bool, 
                         pb: &mut PageBuilder, 
                         fs: &mut SeekWrite, 
                         pageSize: usize,
                         pageManager: &mut IPages,
                         token: &IPendingSegment,
                         ) where SeekWrite : Seek+Write { 
                buildLeaf(st, pb);
                let thisPageNumber = st.blk.firstPage;
                let firstLeaf = if st.leaves.is_empty() { thisPageNumber } else { st.firstLeaf };
                let nextBlk = 
                    if isRootPage {
                        PageBlock::new(thisPageNumber + 1, st.blk.lastPage)
                    } else if thisPageNumber == st.blk.lastPage {
                        pb.SetPageFlag(PageFlag::FLAG_BOUNDARY_NODE as u8);
                        let newBlk = pageManager.GetBlock(token);
                        pb.SetLastInt32(newBlk.firstPage as i32);
                        newBlk
                    } else {
                        PageBlock::new(thisPageNumber + 1, st.blk.lastPage)
                    };
                pb.Write(fs);
                if nextBlk.firstPage != (thisPageNumber+1) {
                    utils::SeekPage(fs, pageSize, nextBlk.firstPage);
                }
                // TODO isn't there a better way to copy a slice?
                let mut ba = Vec::new();
                ba.push_all(&st.keys[0].key);
                let pg = pgitem {page:thisPageNumber, key:ba.into_boxed_slice()};
                st.leaves.push(pg);
                st.sofarLeaf = 0;
                st.keys = Vec::new();
                st.prevLeaf = thisPageNumber;
                st.prefixLen = 0;
                st.firstLeaf = firstLeaf;
                st.blk = nextBlk;
            }

            // TODO can the overflow page number become a varint?
            const neededForOverflowPageNumber: usize = 4;

            // the max limit of an inline key is when that key is the only
            // one in the leaf, and its value is overflowed.

            let pageSize = pageManager.PageSize();
            let maxKeyInline = 
                pageSize 
                - LEAF_PAGE_OVERHEAD 
                - 1 // prefixLen
                - 1 // key flags
                - Varint::SpaceNeededFor(pageSize as u64) // approx worst case inline key len
                - 1 // value flags
                - 9 // worst case varint value len
                - neededForOverflowPageNumber; // overflowed value page

            fn kLocNeed(k: &[u8], kloc: &KeyLocation, prefixLen: usize) -> usize {
                let klen = k.len();
                match *kloc {
                    KeyLocation::Inline => {
                        1 + Varint::SpaceNeededFor(klen as u64) + klen - prefixLen
                    },
                    KeyLocation::Overflow(_) => {
                        1 + Varint::SpaceNeededFor(klen as u64) + neededForOverflowPageNumber
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
                        1 + Varint::SpaceNeededFor(vlen as u64) + neededForOverflowPageNumber
                    },
                }
            }

            fn leafPairSize(prefixLen: usize, lp: &LeafPair) -> usize {
                kLocNeed(&lp.key, &lp.kLoc, prefixLen)
                +
                vLocNeed(&lp.vLoc)
            }

            fn defaultPrefixLen (k:&[u8]) -> usize {
                // TODO max prefix.  relative to page size?  must fit in byte.
                if k.len() > 255 { 255 } else { k.len() }
            }

            // this is the body of writeLeaves
            //let source = seq { csr.First(); while csr.IsValid() do yield (csr.Key(), csr.Value()); csr.Next(); done }
            let mut st = LeafState {
                sofarLeaf:0,
                firstLeaf:0,
                prevLeaf:0,
                keys:Vec::new(),
                prefixLen:0,
                leaves:Vec::new(),
                blk:leavesBlk,
                };

            for mut pair in source {
                let k = pair.Key;
                // assert k <> null
                // but pair.Value might be null (a tombstone)

                // TODO is it possible for this to conclude that the key must be overflowed
                // when it would actually fit because of prefixing?

                let (blkAfterKey,kloc) = 
                    if k.len() <= maxKeyInline {
                        (st.blk, KeyLocation::Inline)
                    } else {
                        let vPage = st.blk.firstPage;
                        let (_,newBlk) = try!(writeOverflow(st.blk, &mut &*k, pageManager, fs));
                        (newBlk, KeyLocation::Overflow(vPage))
                    };

                // the max limit of an inline value is when the key is inline
                // on a new page.

                let availableOnNewPageAfterKey = 
                    pageSize 
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
                                            if a.len() == 0 {
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
                                            let vread = try!(utils::ReadFully(&mut *strm, &mut vbuf[0 .. maxValueInline+1]));
                                            let vbuf = &vbuf[0 .. vread];
                                            if vread < maxValueInline {
                                                // TODO this alloc+copy is unfortunate
                                                let mut va = Vec::new();
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
                                                let strm = a; // TODO need a Read for this
                                                let (len,newBlk) = try!(writeOverflow(blkAfterKey, &mut &*strm, pageManager, fs));
                                                (newBlk, ValueLocation::Overflowed(len,valuePage))
                                            }
                                        },
                                    }
                                }
                             },

                             KeyLocation::Overflow(_) => {
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
                                        if a.len() == 0 {
                                            (blkAfterKey, ValueLocation::Buffer(a))
                                        } else {
                                            let valuePage = blkAfterKey.firstPage;
                                            let strm = a; // TODO need a Read for this
                                            let (len,newBlk) = try!(writeOverflow(blkAfterKey, &mut &*strm, pageManager, fs));
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

                st.blk=blkAfterValue;

                // TODO ignore prefixLen for overflowed keys?
                let newPrefixLen = 
                    if st.keys.len()==0 {
                        defaultPrefixLen(&k)
                    } else {
                        bcmp::PrefixMatch(&*st.keys[0].key, &k, st.prefixLen)
                    };
                let sofar = 
                    if newPrefixLen < st.prefixLen {
                        // the prefixLen would change with the addition of this key,
                        // so we need to recalc sofar
                        // TODO is it a problem that we're doing this without List.rev ?
                        let mut sum = 0;
                        for lp in &st.keys {
                            sum = sum + leafPairSize(newPrefixLen, lp);
                        }
                        // TODO iter sum?
                        sum
                    } else {
                        st.sofarLeaf
                    };
                let available = pageSize - (sofar + LEAF_PAGE_OVERHEAD + 1 + newPrefixLen);
                let needed = kLocNeed(&k, &kloc, newPrefixLen) + vLocNeed(&vloc);
                let fit = (available >= needed);
                let writeThisPage = (! st.keys.is_empty()) && (! fit);

                if writeThisPage {
                    writeLeaf(&mut st, false, pb, fs, pageSize, pageManager, token)
                }

                // TODO ignore prefixLen for overflowed keys?
                let newPrefixLen = 
                    if st.keys.is_empty() {
                        defaultPrefixLen(&k)
                    } else {
                        bcmp::PrefixMatch(&*st.keys[0].key, &k, st.prefixLen)
                    };
                let sofar = 
                    if newPrefixLen < st.prefixLen {
                        // the prefixLen will change with the addition of this key,
                        // so we need to recalc sofar
                        // TODO is it a problem that we're doing this without List.rev ?
                        let mut sum = 0;
                        for lp in &st.keys {
                            sum = sum + leafPairSize(newPrefixLen, lp);
                        }
                        // TODO iter sum?
                        sum
                    } else {
                        st.sofarLeaf
                    };
                let lp = LeafPair {
                            key:k,
                            kLoc:kloc,
                            vLoc:vloc,
                            };

                st.sofarLeaf=sofar + leafPairSize(newPrefixLen, &lp);
                st.keys.push(box lp);
                st.prefixLen=newPrefixLen;
            }

            if !st.keys.is_empty() {
                let isRootNode = st.leaves.is_empty();
                writeLeaf(&mut st, isRootNode, pb, fs, pageSize, pageManager, token)
            }
            Ok((st.blk,st.leaves,st.firstLeaf))
        }

        fn writeParentNodes<SeekWrite>(startingBlk: PageBlock, 
                                       children: &[pgitem],
                                       pageSize: usize,
                                       fs: &mut SeekWrite,
                                       pageManager: &mut IPages,
                                       token: &IPendingSegment,
                                       lastLeaf: usize,
                                       firstLeaf: usize,
                                       pb: &mut PageBuilder,
                                      ) -> io::Result<(PageBlock, Vec<pgitem>)> where SeekWrite : Seek+Write {
            // 2 for the page type and flags
            // 2 for the stored count
            // 5 for the extra ptr we will add at the end, a varint, 5 is worst case (page num < 4294967295L)
            // 4 for lastInt32
            const PARENT_PAGE_OVERHEAD :usize = 2 + 2 + 5 + 4;

            fn calcAvailable(currentSize: usize, couldBeRoot: bool, pageSize: usize) -> usize {
                let basicSize = pageSize - currentSize;
                let allowanceForRootNode = if couldBeRoot { size_i32 } else { 0 }; // first/last Leaf, lastInt32 already
                basicSize - allowanceForRootNode
            }

            fn buildParentPage(items: &[&pgitem], 
                               lastPtr: usize, 
                               overflows: &HashMap<usize,usize>,
                               pb : &mut PageBuilder,
                              ) {
                pb.Reset();
                pb.PutByte(PageType::PARENT_NODE as u8);
                pb.PutByte(0u8);
                pb.PutInt16(items.len() as i16);
                // store all the ptrs, n+1 of them
                for x in items.iter() {
                    pb.PutVarint(x.page as u64);
                }
                pb.PutVarint(lastPtr as u64);
                // store all the keys, n of them
                for i in 0 .. items.len() {
                    let x = &items[i];
                    match overflows.get(&i) {
                        Some(pg) => {
                            pb.PutByte(ValueFlag::FLAG_OVERFLOW as u8);
                            pb.PutVarint(x.key.len() as u64);
                            pb.PutInt32(*pg as i32);
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
                                          items: &[&pgitem],
                                          overflows: &HashMap<usize,usize>,
                                          pair:&pgitem, 
                                          isRootNode: bool, 
                                          pb: &mut PageBuilder, 
                                          lastLeaf: usize,
                                          fs: &mut SeekWrite,
                                          pageManager: &mut IPages,
                                          pageSize: usize,
                                          token: &IPendingSegment,
                                          firstLeaf: usize,
                                         ) where SeekWrite : Seek+Write {
                let pagenum = pair.page;
                // assert st.sofar > 0
                let thisPageNumber = st.blk.firstPage;
                buildParentPage(items, pagenum, &overflows, pb);
                let nextBlk =
                    if isRootNode {
                        pb.SetPageFlag(PageFlag::FLAG_ROOT_NODE as u8);
                        pb.SetSecondToLastInt32(firstLeaf as i32);
                        pb.SetLastInt32(lastLeaf as i32);
                        PageBlock::new(thisPageNumber+1,st.blk.lastPage)
                    } else {
                        if (st.blk.firstPage == st.blk.lastPage) {
                            pb.SetPageFlag(PageFlag::FLAG_BOUNDARY_NODE as u8);
                            let newBlk = pageManager.GetBlock(token);
                            pb.SetLastInt32(newBlk.firstPage as i32);
                            newBlk
                        } else {
                            PageBlock::new(thisPageNumber+1,st.blk.lastPage)
                        }
                    };
                pb.Write(fs);
                if nextBlk.firstPage != (thisPageNumber+1) {
                    utils::SeekPage(fs, pageSize, nextBlk.firstPage);
                }
                st.sofar = 0;
                st.blk = nextBlk;
                // TODO isn't there a better way to copy a slice?
                let mut ba = Vec::new();
                ba.push_all(&pair.key);
                let pg = pgitem {page:thisPageNumber, key:ba.into_boxed_slice()};
                st.nextGeneration.push(pg);
            }

            // this is the body of writeParentNodes
            let mut st = ParentState {nextGeneration:Vec::new(),sofar:0,blk:startingBlk,};
            let mut items = Vec::new();
            let mut overflows = HashMap::new();
            for i in 0 .. children.len()-1 {
                let pair = &children[i];
                let pagenum = pair.page;

                let neededEitherWay = 1 + Varint::SpaceNeededFor(pair.key.len() as u64) + Varint::SpaceNeededFor(pagenum as u64);
                let neededForInline = neededEitherWay + pair.key.len();
                let neededForOverflow = neededEitherWay + size_i32;
                let couldBeRoot = st.nextGeneration.is_empty();

                let available = calcAvailable(st.sofar, couldBeRoot, pageSize);
                let fitsInline = (available >= neededForInline);
                let wouldFitInlineOnNextPage = ((pageSize - PARENT_PAGE_OVERHEAD) >= neededForInline);
                let fitsOverflow = (available >= neededForOverflow);
                let writeThisPage = (! fitsInline) && (wouldFitInlineOnNextPage || (! fitsOverflow));

                if writeThisPage {
                    // assert sofar > 0
                    writeParentPage(&mut st, &items, &overflows, pair, false, pb, lastLeaf, fs, pageManager, pageSize, token, firstLeaf);
                }

                if st.sofar == 0 {
                    st.sofar = PARENT_PAGE_OVERHEAD;
                    items.clear();
                }

                items.push(pair);
                if calcAvailable(st.sofar, st.nextGeneration.is_empty(), pageSize) >= neededForInline {
                    st.sofar = st.sofar + neededForInline;
                } else {
                    let keyOverflowFirstPage = st.blk.firstPage;
                    let (_,newBlk) = try!(writeOverflow(st.blk, &mut &*pair.key, pageManager, fs));
                    st.sofar = st.sofar + neededForOverflow;
                    st.blk = newBlk;
                    overflows.insert(items.len()-1,keyOverflowFirstPage);
                }
            }
            let isRootNode = st.nextGeneration.is_empty();
            writeParentPage(&mut st, &items, &overflows, &children[children.len()-1], isRootNode, pb, lastLeaf, fs,
            pageManager, pageSize, token, firstLeaf);
            Ok((st.blk,st.nextGeneration))
        }

        // this is the body of Create
        let pageSize = pageManager.PageSize();
        let mut pb = PageBuilder::new(pageSize);
        let token = pageManager.Begin();
        let startingBlk = pageManager.GetBlock(&token);
        utils::SeekPage(fs, pageSize, startingBlk.firstPage);

        let mut vbuf = vec![0;pageSize].into_boxed_slice();
        let (blkAfterLeaves, leaves, firstLeaf) = try!(writeLeaves(startingBlk, pageManager, source, &mut vbuf, fs, &mut pb, &token));

        // all the leaves are written.
        // now write the parent pages.
        // maybe more than one level of them.
        // keep writing until we have written a level which has only one node,
        // which is the root node.

        let lastLeaf = leaves[0].page;

        let rootPage = {
            let mut blk = blkAfterLeaves;
            let mut children = leaves;
            loop {
                let (newBlk,newChildren) = try!(writeParentNodes(blk, &children, pageSize, fs, pageManager, &token, lastLeaf, firstLeaf, &mut pb));
                blk = newBlk;
                children = newChildren;
                if children.len()==1 {
                    break;
                }
            }
            children[0].page
        };

        let g = pageManager.End(token, rootPage);
        Ok((g,rootPage))
    }

}

struct foo {
    num : usize,
    i : usize,
}

impl Iterator for foo {
    type Item = kvp;
    fn next(& mut self) -> Option<kvp> {
        if self.i >= self.num {
            None
        }
        else {
            fn create_array(n : usize) -> Box<[u8]> {
                let mut kv = Vec::new();
                for i in 0 .. n {
                    kv.push(i as u8);
                }
                let k = kv.into_boxed_slice();
                k
            }

            let k = format!("{}", self.i).into_bytes().into_boxed_slice();
            let v = format!("{}", self.i * 2).into_bytes().into_boxed_slice();
            let r = kvp{Key:k, Value:Blob::Array(v)};
            self.i = self.i + 1;
            Some(r)
        }
    }
}

struct SimplePageManager {
    pageSize : usize,
    nextPage : usize,
}

impl IPages for SimplePageManager {
    fn PageSize(&self) -> usize {
        self.pageSize
    }

    fn Begin(&mut self) -> IPendingSegment {
        IPendingSegment { unused : 0}
    }

    fn GetBlock(&mut self, token:&IPendingSegment) -> PageBlock {
        let blk = PageBlock::new(self.nextPage, self.nextPage + 10 - 1);
        self.nextPage = self.nextPage + 10;
        blk
    }

    fn End(&mut self, token:IPendingSegment, page:usize) -> Guid {
        Guid { hack : 0 }
    }

}

fn hack() -> io::Result<bool> {
    use std::fs::File;

    let mut f = try!(File::create("data.bin"));

    let src = foo {num:100, i:0};
    let mut mgr = SimplePageManager {pageSize: 4096, nextPage: 1};
    bt::CreateFromSortedSequenceOfKeyValuePairs(&mut f, &mut mgr, src);

    let res : io::Result<bool> = Ok(true);
    res
}

fn main() {
    hack();
}

/*

module bt =

    type private myOverflowReadStream(_fs:Stream, pageSize:int, _firstPage:int, _len:int) =
        inherit Stream()
        let fs = _fs
        let len = _len
        let firstPage = _firstPage
        let buf:byte[] = Array.zeroCreate pageSize
        let mutable currentPage = firstPage
        let mutable sofarOverall = 0
        let mutable sofarThisPage = 0
        let mutable firstPageInBlock = 0
        let mutable offsetToLastPageInThisBlock = 0 // add to firstPageInBlock to get the last one
        let mutable countRegularDataPagesInBlock = 0       
        let mutable boundaryPageNumber = 0
        let mutable bytesOnThisPage = 0
        let mutable offsetOnThisPage = 0

        // TODO consider supporting seek

        let ReadPage() = 
            utils.SeekPage(fs, buf.Length, currentPage)
            utils.ReadFully(fs, buf, 0, buf.Length)
            // assert PageType is OVERFLOW
            sofarThisPage <- 0
            if currentPage = firstPageInBlock then
                bytesOnThisPage <- buf.Length - (2 + sizeof<int32>)
                offsetOnThisPage <- 2
            else if currentPage = boundaryPageNumber then
                bytesOnThisPage <- buf.Length - sizeof<int32>
                offsetOnThisPage <- 0
            else
                // assert currentPage > firstPageInBlock
                // assert currentPage < boundaryPageNumber OR boundaryPageNumber = 0
                bytesOnThisPage <- buf.Length
                offsetOnThisPage <- 0

        let GetInt32At(at) :int =
            let a0 = uint64 buf.[at+0]
            let a1 = uint64 buf.[at+1]
            let a2 = uint64 buf.[at+2]
            let a3 = uint64 buf.[at+3]
            let r = (a0 <<< 24) ||| (a1 <<< 16) ||| (a2 <<< 8) ||| (a3 <<< 0)
            // assert r fits in a 32 bit signed int
            int r

        let GetLastInt32() = GetInt32At(buf.Length - sizeof<int32>)
        let PageType() = (buf.[0])
        let CheckPageFlag(f) = 0uy <> (buf.[1] &&& f)

        let ReadFirstPage() =
            firstPageInBlock <- currentPage
            ReadPage()
            if PageType() <> OVERFLOW_NODE then failwith "first overflow page has invalid page type"
            if CheckPageFlag(FLAG_BOUNDARY_NODE) then
                // first page landed on a boundary node
                // lastInt32 is the next page number, which we'll fetch later
                boundaryPageNumber <- currentPage
                offsetToLastPageInThisBlock <- 0
                countRegularDataPagesInBlock <- 0
            else 
                offsetToLastPageInThisBlock <- GetLastInt32()
                if CheckPageFlag(FLAG_ENDS_ON_BOUNDARY) then
                    boundaryPageNumber <- currentPage + offsetToLastPageInThisBlock
                    countRegularDataPagesInBlock <- offsetToLastPageInThisBlock - 1
                else
                    boundaryPageNumber <- 0
                    countRegularDataPagesInBlock <- offsetToLastPageInThisBlock

        do ReadFirstPage()

        override this.Length = int64 len
        override this.CanRead = sofarOverall < len // TODO always return true?

        override this.Read(ba,offset,wanted) =
            if sofarOverall >= len then
                0
            else    
                let mutable direct = false
                if (sofarThisPage >= bytesOnThisPage) then
                    if currentPage = boundaryPageNumber then
                        currentPage <- GetLastInt32()
                        ReadFirstPage()
                    else
                        // we need a new page.  and if it's a full data page,
                        // and if wanted is big enough to take all of it, then
                        // we want to read (at least) it directly into the
                        // buffer provided by the caller.  we already know
                        // this candidate page cannot be the first page in a
                        // block.
                        let maybeDataPage = currentPage + 1
                        let isDataPage = 
                            if boundaryPageNumber > 0 then
                                ((len - sofarOverall) >= buf.Length) && (countRegularDataPagesInBlock > 0) && (maybeDataPage > firstPageInBlock) && (maybeDataPage < boundaryPageNumber)
                            else
                                ((len - sofarOverall) >= buf.Length) && (countRegularDataPagesInBlock > 0) && (maybeDataPage > firstPageInBlock) && (maybeDataPage <= (firstPageInBlock + countRegularDataPagesInBlock))

                        if isDataPage && (wanted >= buf.Length) then
                            // assert (currentPage + 1) > firstPageInBlock
                            //
                            // don't increment currentPage here because below, we will
                            // calculate how many pages we actually want to do.
                            direct <- true
                            bytesOnThisPage <- buf.Length
                            sofarThisPage <- 0
                            offsetOnThisPage <- 0
                        else
                            currentPage <- currentPage + 1
                            ReadPage()

                if direct then
                    // currentPage has not been incremented yet
                    //
                    // skip the buffer.  note, therefore, that the contents of the
                    // buffer are "invalid" in that they do not correspond to currentPage
                    //
                    let numPagesWanted = wanted / buf.Length
                    // assert countRegularDataPagesInBlock > 0
                    let lastDataPageInThisBlock = firstPageInBlock + countRegularDataPagesInBlock 
                    let theDataPage = currentPage + 1
                    let numPagesAvailable = 
                        if boundaryPageNumber>0 then 
                            boundaryPageNumber - theDataPage 
                        else 
                            lastDataPageInThisBlock - theDataPage + 1
                    let numPagesToFetch = Math.Min(numPagesWanted, numPagesAvailable)
                    let bytesToFetch = numPagesToFetch * buf.Length
                    // assert bytesToFetch <= wanted

                    utils.SeekPage(fs, buf.Length, theDataPage)
                    utils.ReadFully(fs, ba, offset, bytesToFetch)
                    sofarOverall <- sofarOverall + bytesToFetch
                    currentPage <- currentPage + numPagesToFetch
                    sofarThisPage <- buf.Length
                    bytesToFetch
                else
                    let available = Math.Min (bytesOnThisPage - sofarThisPage, len - sofarOverall)
                    let num = Math.Min (available, wanted)
                    System.Array.Copy (buf, offsetOnThisPage + sofarThisPage, ba, offset, num)
                    sofarOverall <- sofarOverall + num
                    sofarThisPage <- sofarThisPage + num
                    num

        override this.CanSeek = false
        override this.Seek(_,_) = raise (NotSupportedException())
        override this.Position
            with get() = int64 sofarOverall
            and set(v) = this.Seek(v, SeekOrigin.Begin) |> ignore

        override this.CanWrite = false
        override this.SetLength(_) = raise (NotSupportedException())
        override this.Flush() = raise (NotSupportedException())
        override this.Write(_,_,_) = raise (NotSupportedException())

    let private readOverflow len fs pageSize (firstPage:int) =
        let ostrm = new myOverflowReadStream(fs, pageSize, firstPage, len)
        utils.ReadAll(ostrm)

    type private myCursor(_fs:Stream, pageSize:int, _rootPage:int, _hook:Action<ICursor>) =
        let fs = _fs
        let rootPage = _rootPage
        let pr = PageReader(pageSize)
        let hook = _hook

        let mutable currentPage:int = 0
        let mutable leafKeys:int[] = null
        let mutable countLeafKeys = 0 // only realloc leafKeys when it's too small
        let mutable previousLeaf:int = 0
        let mutable currentKey = -1
        let mutable prefix:byte[] = null

        let resetLeaf() =
            countLeafKeys <- 0
            previousLeaf <- 0
            currentKey <- -1
            prefix <- null

        let setCurrentPage (pagenum:int) = 
            // TODO consider passing a block list for the segment into this
            // cursor so that the code here can detect if it tries to stray
            // out of bounds.

            // TODO if currentPage = pagenum already...
            currentPage <- pagenum
            resetLeaf()
            if 0 = currentPage then 
                false
            else
                // refuse to go to a page beyond the end of the stream
                // TODO is this the right place for this check?    
                let pos = ((int64 currentPage) - 1L) * int64 pr.PageSize
                if pos + int64 pr.PageSize <= fs.Length then
                    utils.SeekPage(fs, pr.PageSize, currentPage)
                    pr.Read(fs)
                    true
                else
                    false

        let getFirstAndLastLeaf() = 
            if not (setCurrentPage rootPage) then failwith "failed to read root page"
            if pr.PageType = LEAF_NODE then
                (rootPage, rootPage)
            else if pr.PageType = PARENT_NODE then
                if not (pr.CheckPageFlag(FLAG_ROOT_NODE)) then failwith "root page lacks flag"
                let first = pr.GetSecondToLastInt32()
                let last = pr.GetLastInt32()
                (first, last)
            else failwith "root page has invalid page type"
              
        let (firstLeaf, lastLeaf) = getFirstAndLastLeaf()

        let nextInLeaf() =
            if (currentKey+1) < countLeafKeys then
                currentKey <- currentKey + 1
                true
            else
                false

        let prevInLeaf() =
            if (currentKey > 0) then
                currentKey <- currentKey - 1
                true
            else
                false

        let skipKey() =
            let kflag = pr.GetByte()
            let klen = pr.GetVarint()
            if 0uy = (kflag &&& FLAG_OVERFLOW) then
                pr.Skip(int klen - if null=prefix then 0 else prefix.Length)
            else
                pr.Skip(sizeof<int32>)

        let skipValue() =
            let vflag = pr.GetByte()
            if 0uy <> (vflag &&& FLAG_TOMBSTONE) then ()
            else 
                let vlen = pr.GetVarint()
                if 0uy <> (vflag &&& FLAG_OVERFLOW) then pr.Skip(sizeof<int32>)
                else pr.Skip(int vlen)

        let readLeaf() =
            resetLeaf()
            pr.Reset()
            if pr.GetByte() <> LEAF_NODE then failwith "leaf has invalid page type"
            pr.GetByte() |> ignore
            previousLeaf <- pr.GetInt32()
            let prefixLen = pr.GetByte() |> int
            if prefixLen > 0 then
                prefix <- pr.GetArray(prefixLen)
            else
                prefix <- null
            countLeafKeys <- pr.GetInt16() |> int
            // only realloc leafKeys if it's too small
            if leafKeys=null || leafKeys.Length<countLeafKeys then
                leafKeys <- Array.zeroCreate countLeafKeys
            for i in 0 .. (countLeafKeys-1) do
                leafKeys.[i] <- pr.Position
                skipKey()
                skipValue()

        let keyInLeaf n = 
            pr.SetPosition(leafKeys.[n])
            let kflag = pr.GetByte()
            let klen = pr.GetVarint() |> int
            if 0uy = (kflag &&& FLAG_OVERFLOW) then
                if prefix <> null then
                    let prefixLen = prefix.Length
                    let res:byte[] = Array.zeroCreate klen
                    System.Array.Copy(prefix, 0, res, 0, prefixLen)
                    pr.GetIntoArray(res, prefixLen, klen - prefixLen)
                    res
                else
                    pr.GetArray(klen) // TODO consider alloc here and use GetIntoArray
            else
                let pagenum = pr.GetInt32()
                let k = readOverflow klen fs pr.PageSize pagenum
                k

        let compareKeyInLeaf n other = 
            pr.SetPosition(leafKeys.[n])
            let kflag = pr.GetByte()
            let klen = pr.GetVarint() |> int
            if 0uy = (kflag &&& FLAG_OVERFLOW) then
                if prefix <> null then
                    pr.CompareWithPrefix(prefix, klen, other)
                else
                    pr.Compare(klen, other)
            else
                // TODO this could be more efficient. we could compare the key
                // in place in the overflow without fetching the entire thing.

                // TODO overflowed keys are not prefixed.  should they be?
                let pagenum = pr.GetInt32()
                let k = readOverflow klen fs pr.PageSize pagenum
                bcmp.Compare k other
           
        let rec searchLeaf k min max sop le ge = 
            if max < min then
                match sop with
                | SeekOp.SEEK_EQ -> -1
                | SeekOp.SEEK_LE -> le
                | _ -> ge
            else
                let mid = (max + min) / 2
                let cmp = compareKeyInLeaf mid k
                if 0 = cmp then mid
                else if cmp<0  then searchLeaf k (mid+1) max sop mid ge
                else searchLeaf k min (mid-1) sop le mid

        let readParentPage() =
            pr.Reset()
            if pr.GetByte() <> PARENT_NODE then failwith "parent page has invalid page type"
            pr.Skip(1) // page flags
            let count = pr.GetInt16()
            let ptrs:int[] = Array.zeroCreate (int (count+1))
            let keys:byte[][] = Array.zeroCreate (int count)
            for i in 0 .. int count do
                ptrs.[i] <-  pr.GetVarint() |> int
            for i in 0 .. int (count-1) do
                let kflag = pr.GetByte()
                let klen = pr.GetVarint() |> int
                if 0uy = (kflag &&& FLAG_OVERFLOW) then
                    keys.[i] <- pr.GetArray(klen)
                else
                    let pagenum = pr.GetInt32()
                    keys.[i] <- readOverflow klen fs pr.PageSize pagenum
            (ptrs,keys)

        // this is used when moving forward through the leaf pages.
        // we need to skip any overflows.  when moving backward,
        // this is not necessary, because each leaf has a pointer to
        // the leaf before it.
        let rec searchForwardForLeaf() = 
            let pt = pr.PageType
            if pt = LEAF_NODE then 
                true
            else if pt = PARENT_NODE then 
                // if we bump into a parent node, that means there are
                // no more leaves.
                false
            else
                let lastInt32 = pr.GetLastInt32()
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
                if pr.CheckPageFlag(FLAG_BOUNDARY_NODE) then
                    if setCurrentPage (lastInt32) then
                        searchForwardForLeaf()
                    else
                        false
                else 
                    let lastPage = currentPage + lastInt32
                    let endsOnBoundary = pr.CheckPageFlag(FLAG_ENDS_ON_BOUNDARY)
                    if endsOnBoundary then
                        if setCurrentPage lastPage then
                            let next = pr.GetLastInt32()
                            if setCurrentPage (next) then
                                searchForwardForLeaf()
                            else
                                false
                        else
                            false
                    else
                        if setCurrentPage (lastPage + 1) then
                            searchForwardForLeaf()
                        else
                            false

        let leafIsValid() =
            let ok = (leafKeys <> null) && (countLeafKeys > 0) && (currentKey >= 0) && (currentKey < countLeafKeys)
            ok

        let rec searchInParentPage k (ptrs:int[]) (keys:byte[][]) (i:int) :int =
            // TODO linear search?  really?
            if i < keys.Length then
                let cmp = bcmp.Compare k (keys.[int i])
                if cmp>0 then
                    searchInParentPage k ptrs keys (i+1)
                else
                    ptrs.[int i]
            else 0

        let rec search pg k sop =
            if setCurrentPage pg then
                if LEAF_NODE = pr.PageType then
                    readLeaf()
                    currentKey <- searchLeaf k 0 (countLeafKeys - 1) sop -1 -1
                    if SeekOp.SEEK_EQ <> sop then
                        if not (leafIsValid()) then
                            // if LE or GE failed on a given page, we might need
                            // to look at the next/prev leaf.
                            if SeekOp.SEEK_GE = sop then
                                let nextPage =
                                    if pr.CheckPageFlag(FLAG_BOUNDARY_NODE) then pr.GetLastInt32()
                                    else if currentPage = rootPage then 0
                                    else currentPage + 1
                                if (setCurrentPage (nextPage) && searchForwardForLeaf ()) then
                                    readLeaf()
                                    currentKey <- 0
                            else
                                if 0 = previousLeaf then
                                    resetLeaf()
                                else if setCurrentPage previousLeaf then
                                    readLeaf()
                                    currentKey <- countLeafKeys - 1
                else if PARENT_NODE = pr.PageType then
                    let (ptrs,keys) = readParentPage()
                    let found = searchInParentPage k ptrs keys 0
                    if 0 = found then
                        search ptrs.[ptrs.Length - 1] k sop
                    else
                        search found k sop

        let dispose itIsSafeToAlsoFreeManagedObjects this =
            if itIsSafeToAlsoFreeManagedObjects then
                if hook <> null then 
                    let fsHook = FuncConvert.ToFSharpFunc hook
                    fsHook(this)

        override this.Finalize() =
            dispose false this

        interface ICursor with
            member this.Dispose() =
                dispose true this
                GC.SuppressFinalize(this)

            member this.IsValid() =
                leafIsValid()

            member this.Seek(k,sop) =
                search rootPage k sop

            member this.Key() =
                keyInLeaf currentKey
            
            member this.Value() =
                pr.SetPosition(leafKeys.[currentKey])

                skipKey()

                let vflag = pr.GetByte()
                if 0uy <> (vflag &&& FLAG_TOMBSTONE) then Blob.Tombstone
                else 
                    let vlen = pr.GetVarint() |> int
                    if 0uy <> (vflag &&& FLAG_OVERFLOW) then 
                        let pagenum = pr.GetInt32()
                        let strm = new myOverflowReadStream(fs, pr.PageSize, pagenum, vlen) :> Stream
                        Blob.Stream strm
                    else 
                        let a = pr.GetArray (vlen)
                        Blob.Array a

            member this.ValueLength() =
                pr.SetPosition(leafKeys.[currentKey])

                skipKey()

                let vflag = pr.GetByte()
                if 0uy <> (vflag &&& FLAG_TOMBSTONE) then -1
                else
                    let vlen = pr.GetVarint() |> int
                    vlen

            member this.KeyCompare(k) =
                compareKeyInLeaf currentKey k

            member this.First() =
                if setCurrentPage firstLeaf then
                    readLeaf()
                    currentKey <- 0

            member this.Last() =
                if setCurrentPage lastLeaf then
                    readLeaf()
                    currentKey <- countLeafKeys - 1

            member this.Next() =
                if not (nextInLeaf()) then
                    let nextPage =
                        if pr.CheckPageFlag(FLAG_BOUNDARY_NODE) then pr.GetLastInt32()
                        else if pr.PageType = LEAF_NODE then
                            if currentPage = rootPage then 0
                            else currentPage + 1
                        else 0
                    if setCurrentPage (nextPage) && searchForwardForLeaf() then
                        readLeaf()
                        currentKey <- 0

            member this.Prev() =
                if not (prevInLeaf()) then
                    if 0 = previousLeaf then
                        resetLeaf()
                    else if setCurrentPage previousLeaf then
                        readLeaf()
                        currentKey <- countLeafKeys - 1
    
    let OpenCursor(fs, pageSize:int, rootPage:int, hook:Action<ICursor>) :ICursor =
        new myCursor(fs, pageSize, rootPage, hook) :> ICursor

[<AbstractClass;Sealed>]
type BTreeSegment =
    static member CreateFromSortedSequence(fs:Stream, pageManager:IPages, source:seq<kvp>) = 
        bt.CreateFromSortedSequenceOfKeyValuePairs (fs, pageManager, source)

    #if not
    static member CreateFromSortedSequence(fs:Stream, pageManager:IPages, pairs:seq<byte[]*Stream>, mess:string) = 
        let source = seq { for t in pairs do yield kvp(fst t,snd t) done }
        bt.CreateFromSortedSequenceOfKeyValuePairs (fs, pageManager, source)
    #endif

    static member SortAndCreate(fs:Stream, pageManager:IPages, pairs:System.Collections.Generic.IDictionary<byte[],Stream>) =
#if not
        let keys:byte[][] = (Array.ofSeq pairs.Keys)
        let sortfunc x y = bcmp.Compare x y
        Array.sortInPlaceWith sortfunc keys
        let sortedSeq = seq { for k in keys do yield kvp(k,pairs.[k]) done }
#else
        // TODO which is faster?  how does linq OrderBy implement sorting
        // of a sequence?
        // http://code.logos.com/blog/2010/04/a_truly_lazy_orderby_in_linq.html
        let s1 = pairs.AsEnumerable()
        let s2 = Seq.map (fun (x:System.Collections.Generic.KeyValuePair<byte[],Stream>) -> kvp(x.Key, if x.Value = null then Blob.Tombstone else x.Value |> Blob.Stream)) s1
        let sortedSeq = s2.OrderBy((fun (x:kvp) -> x.Key), ByteComparer())
#endif
        bt.CreateFromSortedSequenceOfKeyValuePairs (fs, pageManager, sortedSeq)

    static member SortAndCreate(fs:Stream, pageManager:IPages, pairs:System.Collections.Generic.IDictionary<byte[],Blob>) =
#if not
        let keys:byte[][] = (Array.ofSeq pairs.Keys)
        let sortfunc x y = bcmp.Compare x y
        Array.sortInPlaceWith sortfunc keys
        let sortedSeq = seq { for k in keys do yield kvp(k,pairs.[k]) done }
#else
        // TODO which is faster?  how does linq OrderBy implement sorting
        // of a sequence?
        // http://code.logos.com/blog/2010/04/a_truly_lazy_orderby_in_linq.html
        let s1 = pairs.AsEnumerable()
        let sortedSeq = s1.OrderBy((fun (x:kvp) -> x.Key), ByteComparer())
#endif
        bt.CreateFromSortedSequenceOfKeyValuePairs (fs, pageManager, sortedSeq)

    #if not
    static member SortAndCreate(fs:Stream, pageManager:IPages, pairs:Map<byte[],Stream>) =
        let keys:byte[][] = pairs |> Map.toSeq |> Seq.map fst |> Array.ofSeq
        let sortfunc x y = bcmp.Compare x y
        Array.sortInPlaceWith sortfunc keys
        let sortedSeq = seq { for k in keys do yield kvp(k,pairs.[k]) done }
        bt.CreateFromSortedSequenceOfKeyValuePairs (fs, pageManager, sortedSeq)
    #endif

    static member OpenCursor(fs, pageSize:int, rootPage:int, hook:Action<ICursor>) :ICursor =
        bt.OpenCursor(fs,pageSize,rootPage,hook)


type dbf(_path) = 
    let path = _path

    // TODO this code should move elsewhere, since this file wants to be a PCL

    interface IDatabaseFile with
        member this.OpenForWriting() =
            new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite) :> Stream
        member this.OpenForReading() =
            new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite) :> Stream


type private HeaderData =
    {
        // TODO currentState is an ordered copy of segments.Keys.  eliminate duplication?
        // or add assertions and tests to make sure they never get out of sync?
        currentState: Guid list
        segments: Map<Guid,SegmentInfo>
        headerOverflow: PageBlock option
        changeCounter: int64
        mergeCounter: int64
    }

type private PendingSegment() =
    let mutable blockList:PageBlock list = []
    interface IPendingSegment
    member this.AddBlock((b:PageBlock)) =
        if (not (List.isEmpty blockList)) && (b.firstPage = (List.head blockList).lastPage+1) then
            // note that by consolidating blocks here, the segment info list will
            // not have information about the fact that the two blocks were
            // originally separate.  that's okay, since all we care about here is
            // keeping track of which pages are used.  but the btree code itself
            // is still treating the last page of the first block as a boundary
            // page, even though its pointer to the next block goes to the very
            // next page, because its page manager happened to give it a block
            // which immediately follows the one it had.
            blockList <- PageBlock((List.head blockList).firstPage, b.lastPage) :: blockList.Tail
        else
            blockList <- b :: blockList
    member this.End(lastPage) =
        let lastBlock = List.head blockList
        let unused = 
            if lastPage < lastBlock.lastPage then
                blockList <- PageBlock(lastBlock.firstPage, lastPage) :: (List.tail blockList)
                Some (PageBlock(lastPage+1, lastBlock.lastPage))
            else
                None
        (Guid.NewGuid(), blockList, unused)


// used for testing purposes
type SimplePageManager(_pageSize) =
    let pageSize = _pageSize

    let WASTE_PAGES_AFTER_EACH_BLOCK = 3
    let PAGES_PER_BLOCK = 10

    let critSectionNextPage = obj()
    let mutable nextPage = 1

    let getBlock num =
        lock critSectionNextPage (fun () -> 
            let blk = PageBlock(nextPage, nextPage+num-1) 
            nextPage <- nextPage + num + WASTE_PAGES_AFTER_EACH_BLOCK
            blk
            )

    interface IPages with
        member this.PageSize = pageSize

        member this.Begin() = PendingSegment() :> IPendingSegment

        // note that we assume that a single pending segment is going
        // to be written by a single thread.  the concrete PendingSegment
        // class above is not threadsafe.

        member this.GetBlock(token) =
            let ps = token :?> PendingSegment
            let blk = getBlock PAGES_PER_BLOCK
            ps.AddBlock(blk)
            blk

        member this.End(token, lastPage) =
            let ps = token :?> PendingSegment
            let (g,_,_) = ps.End(lastPage)
            g

type Database(_io:IDatabaseFile, _settings:DbSettings) =
    let io = _io
    let settings = _settings
    let fsMine = io.OpenForWriting()

    let HEADER_SIZE_IN_BYTES = 4096

    let readHeader() =
        let read() =
            if fsMine.Length >= (HEADER_SIZE_IN_BYTES |> int64) then
                let pr = PageReader(HEADER_SIZE_IN_BYTES)
                pr.Read(fsMine)
                Some pr
            else
                None

        let parse (pr:PageReader) =
            let readSegmentList (pr:PageReader) =
                let readBlockList (prBlocks:PageReader) =
                    let rec f more cur =
                        if more > 0 then
                            let firstPage = prBlocks.GetVarint() |> int
                            let countPages = prBlocks.GetVarint() |> int
                            // blocks are stored as firstPage/count rather than as
                            // firstPage/lastPage, because the count will always be
                            // smaller as a varint
                            f (more-1) (PageBlock(firstPage,firstPage + countPages - 1) :: cur)
                        else
                            cur

                    let count = prBlocks.GetVarint() |> int
                    f count []

                let count = pr.GetVarint() |> int
                let a:Guid[] = Array.zeroCreate count
                let fldr acc i = 
                    let g = Guid(pr.GetArray(16))
                    a.[i] <- g
                    let root = pr.GetVarint() |> int
                    let age = pr.GetVarint() |> int
                    let blocks = readBlockList(pr)
                    let info = {root=root;age=age;blocks=blocks}
                    Map.add g info acc
                let b = List.fold fldr Map.empty [0 .. count-1]
                (List.ofArray a,b)

            // --------

            let pageSize = pr.GetInt32()
            let changeCounter = pr.GetVarint()
            let mergeCounter = pr.GetVarint()
            let lenSegmentList = pr.GetVarint() |> int

            let overflowed = pr.GetByte()
            let (prSegmentList, blk) = 
                if overflowed <> 0uy then
                    let lenChunk1 = pr.GetInt32()
                    let lenChunk2 = lenSegmentList - lenChunk1
                    let firstPageChunk2 = pr.GetInt32()
                    let extraPages = lenChunk2 / pageSize + if (lenChunk2 % pageSize) <> 0 then 1 else 0
                    let lastPageChunk2 = firstPageChunk2 + extraPages - 1
                    let pr2 = PageReader(lenSegmentList)
                    // copy from chunk1 into pr2
                    pr2.Read(fsMine, 0, lenChunk1)
                    // now get chunk2 and copy it in as well
                    utils.SeekPage(fsMine, pageSize, firstPageChunk2)
                    pr2.Read(fsMine, lenChunk1, lenChunk2)
                    (pr2, Some (PageBlock(firstPageChunk2, lastPageChunk2)))
                else
                    (pr, None)

            let (state,segments) = readSegmentList(prSegmentList)

            let hd = 
                {
                    currentState=state 
                    segments=segments
                    headerOverflow=blk
                    changeCounter=changeCounter
                    mergeCounter=mergeCounter
                }

            (hd, pageSize)

        let calcNextPage pageSize (len:int64) =
            let numPagesSoFar = if (int64 pageSize) > len then 1 else (int (len / (int64 pageSize)))
            numPagesSoFar + 1

        // --------

        fsMine.Seek(0L, SeekOrigin.Begin) |> ignore
        let hdr = read()
        match hdr with
            | Some pr ->
                let (h, pageSize) = parse pr
                let nextAvailablePage = calcNextPage pageSize fsMine.Length
                (h, pageSize, nextAvailablePage)
            | None ->
                let defaultPageSize = settings.DefaultPageSize
                let h = 
                    {
                        segments = Map.empty
                        currentState = []
                        headerOverflow = None
                        changeCounter = 0L
                        mergeCounter = 0L
                    }
                let nextAvailablePage = calcNextPage defaultPageSize (int64 HEADER_SIZE_IN_BYTES)
                (h, defaultPageSize, nextAvailablePage)

    let (firstReadOfHeader,pageSize,firstAvailablePage) = readHeader()

    let mutable header = firstReadOfHeader
    let mutable nextPage = firstAvailablePage
    let mutable segmentsInWaiting: Map<Guid,SegmentInfo> = Map.empty

    let consolidateBlockList blocks =
        let sortedBlocks = List.sortBy (fun (x:PageBlock) -> x.firstPage) blocks
        let fldr acc (t:PageBlock) =
            let (blk:PageBlock, pile) = acc
            if blk.lastPage + 1 = t.firstPage then
                (PageBlock(blk.firstPage, t.lastPage), pile)
            else
                (PageBlock(t.firstPage, t.lastPage), blk :: pile)
        let folded = List.fold fldr (List.head sortedBlocks, []) (List.tail sortedBlocks)
        let consolidated = (fst folded) :: (snd folded)
        consolidated

    let invertBlockList blocks =
        let sortedBlocks = List.sortBy (fun (x:PageBlock) -> x.firstPage) blocks
        let fldr acc (t:PageBlock) =
            let (prev:PageBlock, result) = acc
            (t, PageBlock(prev.lastPage+1, t.firstPage-1) :: result)
        let folded = List.fold fldr (List.head sortedBlocks, []) (List.tail sortedBlocks)
        // if is being used to calculate free blocks, it won't find any free
        // blocks between the last used page and the end of the file
        snd folded

    let listAllBlocks h =
        let headerBlock = PageBlock(1, HEADER_SIZE_IN_BYTES / pageSize)
        let currentBlocks = Map.fold (fun acc _ info -> info.blocks @ acc) [] h.segments
        let inWaitingBlocks = Map.fold (fun acc _ info -> info.blocks @ acc) [] segmentsInWaiting
        let segmentBlocks = headerBlock :: currentBlocks @ inWaitingBlocks
        match h.headerOverflow with
            | Some blk ->
                blk :: segmentBlocks
            | None ->
                segmentBlocks

    let initialFreeBlocks = header |> listAllBlocks |> consolidateBlockList |> invertBlockList |> List.sortBy (fun (x:PageBlock) -> -(x.CountPages)) 
    //do printfn "initialFreeBlocks: %A" initialFreeBlocks

    let mutable freeBlocks:PageBlock list = initialFreeBlocks

    let critSectionNextPage = obj()
    let getBlock specificSize =
        //printfn "getBlock: specificSize=%d" specificSize
        //printfn "freeBlocks: %A" freeBlocks
        lock critSectionNextPage (fun () -> 
            if specificSize > 0 then
                if List.isEmpty freeBlocks || specificSize > (List.head freeBlocks).CountPages then
                    let newBlk = PageBlock(nextPage, nextPage+specificSize-1) 
                    nextPage <- nextPage + specificSize
                    //printfn "newBlk: %A" newBlk
                    newBlk
                else
                    let headBlk = List.head freeBlocks
                    if headBlk.CountPages > specificSize then
                        // trim the block to size
                        let blk2 = PageBlock(headBlk.firstPage, headBlk.firstPage+specificSize-1) 
                        let remainder = PageBlock(headBlk.firstPage+specificSize, headBlk.lastPage)
                        freeBlocks <- remainder :: List.tail freeBlocks
                        // TODO problem: the list is probably no longer sorted.  is this okay?
                        // is a re-sort of the list really worth it?
                        //printfn "reusing blk prune: %A, specificSize:%d, freeBlocks now: %A" headBlk specificSize freeBlocks
                        blk2
                    else
                        freeBlocks <- List.tail freeBlocks
                        //printfn "reusing blk: %A, specificSize:%d, freeBlocks now: %A" headBlk specificSize freeBlocks
                        //printfn "blk.CountPages: %d" (headBlk.CountPages)
                        headBlk
            else
                if List.isEmpty freeBlocks then
                    let size = settings.PagesPerBlock
                    let newBlk = PageBlock(nextPage, nextPage+size-1) 
                    nextPage <- nextPage + size
                    //printfn "newBlk: %A" newBlk
                    newBlk
                else
                    let headBlk = List.head freeBlocks
                    freeBlocks <- List.tail freeBlocks
                    //printfn "reusing blk: %A, specificSize:%d, freeBlocks now: %A" headBlk specificSize freeBlocks
                    //printfn "blk.CountPages: %d" (headBlk.CountPages)
                    headBlk
            )

    let addFreeBlocks blocks =
        // this code should not be called in a release build.  it helps
        // finds problems by zeroing out pages in blocks that
        // have been freed.
        let stomp() =
            let bad:byte[] = Array.zeroCreate pageSize
            use fs = io.OpenForWriting()
            List.iter (fun (b:PageBlock) ->
                //printfn "stomping on block: %A" b
                for x in b.firstPage .. b.lastPage do
                    utils.SeekPage(fs, pageSize, x)
                    fs.Write(bad,0,pageSize)
                ) blocks

        //stomp() // TODO remove.  or maybe a setting?  probably not.

        lock critSectionNextPage (fun () ->
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

            let newList = freeBlocks @ blocks |> consolidateBlockList
            let sorted = List.sortBy (fun (x:PageBlock) -> -(x.CountPages)) newList
            freeBlocks <- sorted
        )
        //printfn "freeBlocks: %A" freeBlocks
        #if not
        printfn "lastPage: %d" (nextPage-1)
        let c1 = header |> listAllBlocks |> consolidateBlockList
        printfn "usedBlocks: %A" c1
        let c2 = c1 |> invertBlockList
        printfn "inverted: %A" c2
        let c3 = List.sortBy (fun (x:PageBlock) -> -(x.CountPages)) c2
        printfn "calc: %A" c3
        printfn ""
        #endif

    let critSectionCursors = obj()
    let mutable cursors:Map<Guid,ICursor list> = Map.empty

    let getCursor segs g fnFree =
        let seg = Map.find g segs
        let rootPage = seg.root
        let fs = io.OpenForReading()
        let hook (csr:ICursor) =
            fs.Close()
            lock critSectionCursors (fun () -> 
                let cur = Map.find g cursors
                let removed = List.filter (fun x -> not (Object.ReferenceEquals(csr, x))) cur
                // if we are removing the last cursor for a segment, we do need to
                // remove that segment guid from the cursors map, not just leave
                // it there with an empty list.
                if List.isEmpty removed then
                    cursors <- Map.remove g cursors
                    match fnFree with
                    | Some f -> f g seg
                    | None -> ()
                else
                    cursors <- Map.add g removed cursors
            )
            //printfn "done with cursor %O" g 
        let csr = BTreeSegment.OpenCursor(fs, pageSize, rootPage, Action<ICursor>(hook))
        // note that getCursor is (and must be) only called from within
        // lock critSectionCursors
        let cur = match Map.tryFind g cursors with
                   | Some c -> c
                   | None -> []
        cursors <- Map.add g (csr :: cur) cursors
        //printfn "added cursor %O: %A" g seg
        csr

    let checkForGoneSegment g seg =
        if not (Map.containsKey g header.segments) then
            // this segment no longer exists
            //printfn "cursor done, segment %O is gone: %A" g seg
            addFreeBlocks seg.blocks

    let critSectionSegmentsInWaiting = obj()

    let pageManager = 
        { new IPages with
            member this.PageSize = pageSize

            member this.Begin() = PendingSegment() :> IPendingSegment

            // note that we assume that a single pending segment is going
            // to be written by a single thread.  the concrete PendingSegment
            // class above is not threadsafe.

            member this.GetBlock(token) =
                let ps = token :?> PendingSegment
                let blk = getBlock 0 // specificSize=0 means we don't care how big of a block we get
                ps.AddBlock(blk)
                blk

            member this.End(token, lastPage) =
                let ps = token :?> PendingSegment
                let (g,blocks,unused) = ps.End(lastPage)
                let info = {age=(-1);blocks=blocks;root=lastPage}
                lock critSectionSegmentsInWaiting (fun () -> 
                    segmentsInWaiting <- Map.add g info segmentsInWaiting
                )
                //printfn "wrote %A: %A" g blocks
                match unused with
                | Some b -> addFreeBlocks [ b ]
                | None -> ()
                g
        }

    let critSectionHeader = obj()

    // a stored segmentinfo for a segment is a single blob of bytes.
    // root page
    // age
    // number of pairs
    // each pair is startBlock,countBlocks
    // all in varints

    let writeHeader hdr =
        let spaceNeededForSegmentInfo (info:SegmentInfo) =
            let a = List.sumBy (fun (t:PageBlock) -> Varint.SpaceNeededFor(t.firstPage |> int64) + Varint.SpaceNeededFor(t.CountPages |> int64)) info.blocks
            let b = Varint.SpaceNeededFor(info.root |> int64)
            let c = Varint.SpaceNeededFor(info.age |> int64)
            let d = Varint.SpaceNeededFor(List.length info.blocks |> int64)
            a + b + c + d

        let spaceForHeader h =
            Varint.SpaceNeededFor(List.length h.currentState |> int64) 
                + List.sumBy (fun g -> (Map.find g h.segments |> spaceNeededForSegmentInfo) + 16) h.currentState

        let buildSegmentList h =
            let space = spaceForHeader h
            let pb = PageBuilder(space)
            // TODO format version number
            pb.PutVarint(List.length h.currentState |> int64)
            List.iter (fun (g:Guid) -> 
                pb.PutArray(g.ToByteArray())
                let info = Map.find g h.segments
                pb.PutVarint(info.root |> int64)
                pb.PutVarint(info.age |> int64)
                pb.PutVarint(List.length info.blocks |> int64)
                // we store PageBlock as first/count instead of first/last, since the
                // count will always compress better as a varint.
                List.iter (fun (t:PageBlock) -> pb.PutVarint(t.firstPage |> int64); pb.PutVarint(t.CountPages |> int64);) info.blocks
                ) h.currentState
            //if 0 <> pb.Available then failwith "not exactly full"
            pb.Buffer

        let pb = PageBuilder(HEADER_SIZE_IN_BYTES)
        pb.PutInt32(pageSize)

        pb.PutVarint(hdr.changeCounter)
        pb.PutVarint(hdr.mergeCounter)

        let buf = buildSegmentList hdr
        pb.PutVarint(buf.Length |> int64)

        let headerOverflow =
            if (pb.Available >= (buf.Length + 1)) then
                pb.PutByte(0uy)
                pb.PutArray(buf)
                None
            else
                pb.PutByte(1uy)
                let fits = pb.Available - 4 - 4
                let extra = buf.Length - fits
                let extraPages = extra / pageSize + if (extra % pageSize) <> 0 then 1 else 0
                //printfn "extra pages: %d" extraPages
                let blk = getBlock (extraPages)
                utils.SeekPage(fsMine, pageSize, blk.firstPage)
                fsMine.Write(buf, fits, extra)
                pb.PutInt32(fits)
                pb.PutInt32(blk.firstPage)
                pb.PutArray(buf, 0, fits)
                Some blk

        fsMine.Seek(0L, SeekOrigin.Begin) |> ignore
        pb.Write(fsMine)
        fsMine.Flush()
        {hdr with headerOverflow=headerOverflow}

    let critSectionMerging = obj()
    // this keeps track of which segments are currently involved in a merge.
    // a segment can only be in one merge at a time.  in effect, this is a list
    // of merge locks for segments.  segments should be removed from this set
    // after the merge has been committed.
    let mutable merging = Set.empty

    let critSectionPendingMerges = obj()
    // this keeps track of merges which have been written but not
    // yet committed.
    let mutable pendingMerges:Map<Guid,Guid list> = Map.empty

    let tryMerge segs =
        let requestMerge () =
            lock critSectionMerging (fun () ->
                let want = Set.ofSeq segs
                let already = Set.intersect want merging
                if Set.isEmpty already then
                    merging <- Set.union merging want
                    true
                else
                    false
            )

        let merge () = 
            // TODO this is silly if segs has only one item in it
            //printfn "merge getting cursors: %A" segs
            let clist = lock critSectionCursors (fun () ->
                let h = header
                List.map (fun g -> getCursor h.segments g (Some checkForGoneSegment)) segs
            )
            use mc = MultiCursor.Create clist
            let pairs = CursorUtils.ToSortedSequenceOfKeyValuePairs mc
            use fs = io.OpenForWriting()
            let (g,_) = BTreeSegment.CreateFromSortedSequence(fs, pageManager, pairs)
            //printfn "merged %A to get %A" segs g
            g

        let storePendingMerge g =
            lock critSectionPendingMerges (fun () ->
                pendingMerges <- Map.add g segs pendingMerges
                // TODO assert segs are in merging set?
            )

        if requestMerge () then
            //printfn "requestMerge Some"
            let later() = 
                //printfn "inside later"
                let g = merge ()
                storePendingMerge g
                g
            Some later
        else
            //printfn "requestMerge None"
            None

    let removePendingMerge g =
        let doneMerge segs =
            lock critSectionMerging (fun () ->
                let removing = Set.ofSeq segs
                // TODO assert is subset?
                merging <- Set.difference merging removing
            )

        let segs = Map.find g pendingMerges
        doneMerge segs
        lock critSectionPendingMerges (fun () ->
            pendingMerges <- Map.remove g pendingMerges
        )

    // only call this if you have the writeLock
    let commitMerge (newGuid:Guid) =
        // TODO we could check to see if this guid is already in the list.

        let lstOld = Map.find newGuid pendingMerges
        let countOld = List.length lstOld                                         
        let oldGuidsAsSet = List.fold (fun acc g -> Set.add g acc) Set.empty lstOld
        let lstAges = List.map (fun g -> (Map.find g header.segments).age) lstOld
        let age = 1 + List.max lstAges

        let segmentsBeingReplaced = Set.fold (fun acc g -> Map.add g (Map.find g header.segments) acc ) Map.empty oldGuidsAsSet

        let oldHeaderOverflow = lock critSectionHeader (fun () -> 
            let ndxFirstOld = List.findIndex (fun g -> g=List.head lstOld) header.currentState
            let subListOld = List.skip ndxFirstOld header.currentState |> List.take countOld
            // if the next line fails, it probably means that somebody tried to merge a set
            // of segments that are not contiguous in currentState.
            if lstOld <> subListOld then failwith (sprintf "segments not found: lstOld = %A  currentState = %A" lstOld header.currentState)
            let before = List.take ndxFirstOld header.currentState
            let after = List.skip (ndxFirstOld + countOld) header.currentState
            let newState = before @ (newGuid :: after)
            let segmentsWithoutOld = Map.filter (fun g _ -> not (Set.contains g oldGuidsAsSet)) header.segments
            let newSegmentInfo = Map.find newGuid segmentsInWaiting
            let newSegments = Map.add newGuid {newSegmentInfo with age=age} segmentsWithoutOld
            let newHeaderBeforeWriting = {
                changeCounter=header.changeCounter
                mergeCounter=header.mergeCounter + 1L
                currentState=newState 
                segments=newSegments
                headerOverflow=None
                }
            let newHeader = writeHeader newHeaderBeforeWriting
            let oldHeaderOverflow = header.headerOverflow
            header <- newHeader
            oldHeaderOverflow
        )
        removePendingMerge newGuid
        // the segment we just committed can now be removed from
        // the segments in waiting list
        lock critSectionSegmentsInWaiting (fun () ->
            segmentsInWaiting <- Map.remove newGuid segmentsInWaiting
        )
        //printfn "segmentsBeingReplaced: %A" segmentsBeingReplaced
        // don't free blocks from any segment which still has a cursor
        lock critSectionCursors (fun () -> 
            let segmentsToBeFreed = Map.filter (fun g _ -> not (Map.containsKey g cursors)) segmentsBeingReplaced
            //printfn "oldGuidsAsSet: %A" oldGuidsAsSet
            let blocksToBeFreed = Seq.fold (fun acc info -> info.blocks @ acc) List.empty (Map.values segmentsToBeFreed)
            match oldHeaderOverflow with
            | Some blk ->
                let blocksToBeFreed = PageBlock(blk.firstPage, blk.lastPage) :: blocksToBeFreed
                addFreeBlocks blocksToBeFreed
            | None ->
                addFreeBlocks blocksToBeFreed
        )
        // note that we intentionally do not release the writeLock here.
        // you can change the segment list more than once while holding
        // the writeLock.  the writeLock gets released when you Dispose() it.

    // only call this if you have the writeLock
    let commitSegments (newGuids:seq<Guid>) fnHook =
        // TODO we could check to see if this guid is already in the list.

        let newGuidsAsSet = Seq.fold (fun acc g -> Set.add g acc) Set.empty newGuids

        let mySegmentsInWaiting = Map.filter (fun g _ -> Set.contains g newGuidsAsSet) segmentsInWaiting
        //printfn "committing: %A" mySegmentsInWaiting
        let oldHeaderOverflow = lock critSectionHeader (fun () -> 
            let newState = (List.ofSeq newGuids) @ header.currentState
            let newSegments = Map.fold (fun acc g info -> Map.add g {info with age=0} acc) header.segments mySegmentsInWaiting
            let newHeaderBeforeWriting = {
                changeCounter=header.changeCounter + 1L
                mergeCounter=header.mergeCounter
                currentState=newState
                segments=newSegments
                headerOverflow=None
                }
            let newHeader = writeHeader newHeaderBeforeWriting
            let oldHeaderOverflow = header.headerOverflow
            header <- newHeader
            oldHeaderOverflow
        )
        //printfn "after commit, currentState: %A" header.currentState
        //printfn "after commit, segments: %A" header.segments
        // all the segments we just committed can now be removed from
        // the segments in waiting list
        lock critSectionSegmentsInWaiting (fun () ->
            let remainingSegmentsInWaiting = Map.filter (fun g _ -> Set.contains g newGuidsAsSet |> not) segmentsInWaiting
            segmentsInWaiting <- remainingSegmentsInWaiting
        )
        match oldHeaderOverflow with
        | Some blk -> addFreeBlocks [ PageBlock(blk.firstPage, blk.lastPage) ]
        | None -> ()
        // note that we intentionally do not release the writeLock here.
        // you can change the segment list more than once while holding
        // the writeLock.  the writeLock gets released when you Dispose() it.

        match fnHook with
        | Some f -> f()
        | None -> ()

    let critSectionInTransaction = obj()
    let mutable inTransaction = false 
    let mutable waiting = Deque.empty

    let getWriteLock front timeout fnCommitSegmentsHook =
        let whence = Environment.StackTrace // TODO remove this.  it was just for debugging.
        let createWriteLockObject () =
            let isReleased = ref false
            let release() =
                isReleased := true
                let next = lock critSectionInTransaction (fun () ->
                    if Deque.isEmpty waiting then
                        //printfn "nobody waiting. tx done"
                        inTransaction <- false
                        None
                    else
                        //printfn "queue has %d waiting.  next." (Queue.length waiting)
                        let f = Deque.head waiting
                        waiting <- Deque.tail waiting
                        //printfn "giving writeLock to next"
                        Some f
                )
                match next with
                | Some f ->
                    f()
                    //printfn "done giving writeLock to next"
                | None -> ()
            {
            new System.Object() with
                override this.Finalize() =
                    let already = !isReleased
                    if not already then failwith (sprintf "a writelock must be explicitly disposed: %s" whence)

            interface IWriteLock with
                member this.Dispose() =
                    let already = !isReleased
                    if already then failwith "only dispose a writelock once"
                    release()
                    GC.SuppressFinalize(this)

                member this.CommitMerge(g:Guid) =
                    let already = !isReleased
                    if already then failwith "don't use a writelock after you dispose it"
                    commitMerge g
                    // note that we intentionally do not release the writeLock here.
                    // you can change the segment list more than once while holding
                    // the writeLock.  the writeLock gets released when you Dispose() it.

                member this.CommitSegments(newGuids:seq<Guid>) =
                    let already = !isReleased
                    if already then failwith "don't use a writelock after you dispose it"
                    commitSegments newGuids fnCommitSegmentsHook
                    // note that we intentionally do not release the writeLock here.
                    // you can change the segment list more than once while holding
                    // the writeLock.  the writeLock gets released when you Dispose() it.
            }

        lock critSectionInTransaction (fun () -> 
            if inTransaction then 
                let ev = new System.Threading.ManualResetEventSlim()
                let cb () = ev.Set()
                if front then
                    waiting <- Deque.cons cb waiting
                else
                    waiting <- Deque.conj cb waiting
                //printfn "Add to wait list: %O" whence
                async {
                    let! b = Async.AwaitWaitHandle(ev.WaitHandle, timeout)
                    ev.Dispose()
                    if b then
                        let lck = createWriteLockObject () 
                        return lck
                    else
                        return failwith "timeout waiting for write lock"
                }
            else 
                //printfn "No waiting: %O" whence
                inTransaction <- true
                async { 
                    let lck = createWriteLockObject () 
                    return lck
                }
            )

    let getPossibleMerge level min all =
        let h = header
        let segmentsOfAge = List.filter (fun g -> (Map.find g h.segments).age=level) h.currentState
        // TODO it would be nice to be able to have more than one merge happening in a level

        // TODO we are trusting segmentsOfAge to be contiguous.  need test cases to
        // verify that currentState always ends up with monotonically increasing age.
        let count = List.length segmentsOfAge
        if count > min then 
            //printfn "NEED MERGE %d -- %d" level count
            // (List.skip) we always merge the stuff at the end of the level so things
            // don't get split up when more segments get prepended to the
            // beginning.
            // TODO if we only do partial here, we might want to schedule a job to do more.
            let grp = if all then segmentsOfAge else List.skip (count - min) segmentsOfAge
            tryMerge grp
        else
            //printfn "no merge needed %d -- %d" level count
            None

    let wrapMergeForLater f = async {
        let g = f()
        //printfn "now waiting for writeLock"
        // merges go to the front of the queue
        use! tx = getWriteLock true (-1) None
        tx.CommitMerge g
        return [ g ]
    }

    let critSectionBackgroundMergeJobs = obj()
    let mutable backgroundMergeJobs = List.empty

    let startBackgroundMergeJob f =
        //printfn "starting background job"
        // TODO this is starving.
        async {
            //printfn "inside start background job"
            let! completor = Async.StartChild f
            lock critSectionBackgroundMergeJobs (fun () -> 
                backgroundMergeJobs <- completor :: backgroundMergeJobs 
                )
            //printfn "inside start background job step 2"
            let! result = completor
            //printfn "inside start background job step 3"
            ignore result
            lock critSectionBackgroundMergeJobs (fun () -> 
                backgroundMergeJobs <- List.filter (fun x -> not (Object.ReferenceEquals(x,completor))) backgroundMergeJobs
                )
        } |> Async.Start

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

    let dispose itIsSafeToAlsoFreeManagedObjects =
        //let blocks = consolidateBlockList header
        //printfn "%A" blocks
        if itIsSafeToAlsoFreeManagedObjects then
            // we don't want to close fsMine until all background jobs
            // are completed.
            let bg = backgroundMergeJobs
            if not (List.isEmpty bg) then
                bg |> Async.Parallel |> Async.RunSynchronously |> ignore

            fsMine.Close()

    static member DefaultSettings = 
        {
            AutoMergeEnabled = true
            AutoMergeMinimumPages = 4
            DefaultPageSize = 4096
            PagesPerBlock = 256
        }

    new(_io:IDatabaseFile) =
        new Database(_io, Database.DefaultSettings)

    override this.Finalize() =
        dispose false

    interface IDatabase with
        member this.Dispose() =
            dispose true
            // TODO what happens if there are open cursors?
            // we could throw.  but why?  maybe we should just
            // let them live until they're done.  does the db
            // object care?  this would be more tricky if we were
            // pooling and reusing read streams.  similar issues
            // for background writes as well.
            GC.SuppressFinalize(this)

        member this.WriteSegmentFromSortedSequence(pairs:seq<kvp>) =
            use fs = io.OpenForWriting()
            let (g,_) = BTreeSegment.CreateFromSortedSequence(fs, pageManager, pairs)
            g

        member this.WriteSegment(pairs:System.Collections.Generic.IDictionary<byte[],Stream>) =
            use fs = io.OpenForWriting()
            let (g,_) = BTreeSegment.SortAndCreate(fs, pageManager, pairs)
            g

        member this.WriteSegment(pairs:System.Collections.Generic.IDictionary<byte[],Blob>) =
            use fs = io.OpenForWriting()
            let (g,_) = BTreeSegment.SortAndCreate(fs, pageManager, pairs)
            g

        member this.Merge(level:int, howMany:int, all:bool) =
            let maybe = getPossibleMerge level howMany all
            match maybe with
            | Some f ->
                let blk = wrapMergeForLater f
                Some blk
            | None -> 
                None

        member this.BackgroundMergeJobs() = 
            backgroundMergeJobs

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

        member this.OpenCursor() =
            // TODO this cursor needs to expose the changeCounter and segment list
            // on which it is based. for optimistic writes. caller can grab a cursor,
            // do their writes, then grab the writelock, and grab another cursor, then
            // compare the two cursors to see if anything important changed.  if not,
            // commit their writes.  if so, nevermind the written segments and start over.

            // TODO we also need a way to open a cursor on segments in waiting
            let clist = lock critSectionCursors (fun () ->
                let h = header
                List.map (fun g -> getCursor h.segments g (Some checkForGoneSegment)) h.currentState
            )
            let mc = MultiCursor.Create clist
            LivingCursor.Create mc

        member this.OpenSegmentCursor(g:Guid) =
            let csr = lock critSectionCursors (fun () ->
                let h = header
                getCursor h.segments g (Some checkForGoneSegment)
            )
            csr

        member this.GetFreeBlocks() = freeBlocks

        member this.PageSize() = pageSize

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

// derive debug is %A
// [u8] is not on the heap.  it's like a primitive that is 7 bytes long.  it's a value type.
// no each()
// no exceptions.  use Result<>
// no currying, no partial application
// no significant whitespace
// strings are utf8, no converting things
// seriously miss full type inference
// weird that it's safe to use unsigned
// don't avoid mutability.  in rust, it's safe, and avoiding it is painful.
// braces vim %
// semicolons A ;, end-of-line comments
// typing tetris
// miss sprintf syntax
//
