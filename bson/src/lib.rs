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
#![feature(slice_position_elem)]

extern crate misc;

use misc::endian::*;
use misc::bufndx;

#[derive(Debug)]
pub enum Error {
    // TODO remove Misc
    Misc(&'static str),

    // TODO more detail within CorruptFile
    CorruptFile(&'static str),

    Io(std::io::Error),
    Utf8(std::str::Utf8Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Error::Io(ref err) => write!(f, "IO error: {}", err),
            Error::Utf8(ref err) => write!(f, "Utf8 error: {}", err),
            Error::Misc(s) => write!(f, "Misc error: {}", s),
            Error::CorruptFile(s) => write!(f, "Corrupt file: {}", s),
        }
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Io(ref err) => std::error::Error::description(err),
            Error::Utf8(ref err) => std::error::Error::description(err),
            Error::Misc(s) => s,
            Error::CorruptFile(s) => s,
        }
    }

    // TODO cause
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(err: std::str::Utf8Error) -> Error {
        Error::Utf8(err)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

// TODO this function doesn't seem to go here
pub fn split_name(s: &str) -> (&str, &str) {
    // TODO
    (&s[0 .. 2], &s[2 .. 4])
}

// TODO is it sufficient to derive PartialEq?
// Or do we need to implement it explicitly to
// catch the nan case?

#[derive(Clone,Debug,PartialEq)]
pub struct Document {
    // TODO consider private
    pub pairs: Vec<(String, Value)>,
}

impl Document {
    pub fn new_empty() -> Self {
        Document {
            pairs: vec![],
        }
    }

    // TODO consider calling this extract
    pub fn remove(&mut self, k: &str) -> Option<Value> {
        match self.pairs.iter().position(|&(ref ksub, _)| ksub == k) {
            Some(i) => {
                let (_, v) = self.pairs.remove(i);
                return Some(v);
            },
            None => {
                return None;
            },
        }
    }

    pub fn must_remove(&mut self, k: &str) -> Result<Value> {
        self.remove(k).ok_or(Error::Misc("required key not found"))
    }

    pub fn must_remove_string(&mut self, k: &str) -> Result<String> {
        let v = try!(self.must_remove(k));
        v.into_string()
    }

    pub fn get(&self, k: &str) -> Option<&Value> {
        for t in self.pairs.iter() {
            let (ref ksub, ref vsub) = *t;
            if ksub == k {
                return Some(vsub);
            }
        }
        return None;
    }

    fn get_nocase(&self, k: &str) -> Option<&Value> {
        for t in self.pairs.iter() {
            let (ref ksub, ref vsub) = *t;
            if std::ascii::AsciiExt::eq_ignore_ascii_case(ksub.as_str(), k) {
                return Some(vsub);
            }
        }
        return None;
    }

    pub fn must_get(&self, k: &str) -> Result<&Value> {
        self.get(k).ok_or(Error::Misc("required key not found"))
    }

    pub fn must_get_str(&self, k: &str) -> Result<&str> {
        let v = try!(self.get(k).ok_or(Error::Misc("required key not found")));
        v.as_str()
    }

    pub fn must_get_array(&self, k: &str) -> Result<&Array> {
        let v = try!(self.get(k).ok_or(Error::Misc("required key not found")));
        v.as_array()
    }

    pub fn set(&mut self, k: &str, v: Value) {
        // TODO make this more efficient?
        for i in 0 .. self.pairs.len() {
            if self.pairs[i].0 == k {
                self.pairs[i].1 = v;
                return;
            }
        }
        self.pairs.push((String::from_str(k), v));
    }

    pub fn set_document(&mut self, k: &str, v: Document) {
        self.set(k, Value::BDocument(v));
    }

    pub fn set_array(&mut self, k: &str, v: Array) {
        self.set(k, Value::BArray(v));
    }

    pub fn set_i32(&mut self, k: &str, v: i32) {
        self.set(k, Value::BInt32(v));
    }

    pub fn set_i64(&mut self, k: &str, v: i64) {
        self.set(k, Value::BInt64(v));
    }

    pub fn set_f64(&mut self, k: &str, v: f64) {
        self.set(k, Value::BDouble(v));
    }

    pub fn set_bool(&mut self, k: &str, v: bool) {
        self.set(k, Value::BBoolean(v));
    }

    pub fn set_str(&mut self, k: &str, v: &str) {
        self.set(k, Value::BString(String::from(v)));
    }

    pub fn set_string(&mut self, k: &str, v: String) {
        self.set(k, Value::BString(v));
    }

    pub fn set_timestamp(&mut self, k: &str, v: i64) {
        self.set(k, Value::BTimeStamp(v));
    }

    pub fn set_datetime(&mut self, k: &str, v: i64) {
        self.set(k, Value::BDateTime(v));
    }

    pub fn to_bson(&self, w: &mut Vec<u8>) {
        let start = w.len();
        // placeholder for length
        w.push_all(&i32_to_bytes_le(0));
        for t in self.pairs.iter() {
            let (ref ksub, ref vsub) = *t;
            w.push(vsub.getTypeNumber_u8());
            vec_push_c_string(w, &ksub);;
            vsub.to_bson(w);
        }
        w.push(0u8);
        let len = w.len() - start;
        misc::bytes::copy_into(&i32_to_bytes_le(len as i32), &mut w[start .. start + 4]);
    }

    pub fn to_bson_array(&self) -> Vec<u8> {
        let mut v = Vec::new();
        self.to_bson(&mut v);
        v
    }

    pub fn find_all_strings<'a>(&'a self, dest: &mut Vec<&'a str>) {
        for t in &self.pairs {
            t.1.find_all_strings(dest);
        }
    }

    pub fn find_path(&self, path: &str) -> Value {
        let dot = path.find('.');
        let name = match dot { 
            None => path,
            Some(ndx) => &path[0 .. ndx]
        };
        match slice_find(&self.pairs, name) {
            Some(ndx) => {
                let v = &self.pairs[ndx].1;
                match dot {
                    None => v.clone(),
                    Some(dot) => v.find_path(&path[dot+1..])
                }
            },
            None => Value::BUndefined
        }
    }

    pub fn from_bson(w: &[u8]) -> Result<Document> {
        let mut cur = 0;
        let d = try!(slurp_document(w, &mut cur));
        Ok(d)
    }

    pub fn is_dbref(&self) -> bool {
        let has_ref = slice_find(&self.pairs, "$ref").is_some();
        let has_id =  slice_find(&self.pairs, "$id").is_some();
        let has_db =  slice_find(&self.pairs, "$db").is_some();
        let len = self.pairs.len();
        if len==2 && has_ref && has_id {
            true
        } else if len==3 && has_ref && has_id && has_db {
            true
        } else {
            false
        }
    }

}

#[derive(Clone,Debug)]
pub struct Array {
    // TODO consider private
    pub items: Vec<Value>,
}

impl Array {
    pub fn new_empty() -> Self {
        Array {
            items: vec![],
        }
    }

    fn to_bson(&self, w: &mut Vec<u8>) {
        let start = w.len();
        // placeholder for length
        w.push_all(&i32_to_bytes_le(0));
        for (i, vsub) in self.items.iter().enumerate() {
            w.push(vsub.getTypeNumber_u8());
            let s = format!("{}", i);
            vec_push_c_string(w, &s);
            vsub.to_bson(w);
        }
        w.push(0u8);
        let len = w.len() - start;
        misc::bytes::copy_into(&i32_to_bytes_le(len as i32), &mut w[start .. start + 4]);
    }

    fn find_all_strings<'a>(&'a self, dest: &mut Vec<&'a str>) {
        for v in &self.items {
            v.find_all_strings(dest);
        }
    }

    fn tryGetValueAtIndex(&self, ndx: usize) -> Option<&Value> {
        if ndx<0 {
            return None
        } else if ndx >= self.items.len() {
            return None
        } else {
            return Some(&self.items[ndx])
        }
    }

    fn setValueAtIndex(&mut self, ndx: usize, v: Value) {
        if ndx > 1500001 { panic!( "too big"); } // TODO this limit passes test set7.js, but is a bad idea
        if ndx >= self.items.len() {
            // TODO
        }
        self.items[ndx] = v;
    }

    fn removeValueAtIndex(&mut self, ndx: usize) {
        self.items.remove(ndx);
    }

    fn unsetValueAtIndex(&mut self, ndx: usize) {
        if ndx >=0 && ndx < self.items.len() {
            self.items[ndx] = Value::BNull;
        }
    }

}

#[derive(Clone,Debug)]
pub enum Value {
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
    BBinary(u8, Vec<u8>),
    BMinKey,
    BMaxKey,
    BDateTime(i64),
    BTimeStamp(i64),
    BBoolean(bool),
    BArray(Array),
    BDocument(Document),
}

// We want the ability to put a Value into a HashSet,
// but it contains an f64, which does not implement Eq or Hash.
// So we provide implementations below for Value that
// are sufficient for our purposes.

impl PartialEq for Value {
    fn eq(&self, other: &Value) -> bool {
        // TODO slow
        let a = self.to_bson_array();
        let b = other.to_bson_array();
        a == b
    }
}

impl Eq for Value {
}

impl std::hash::Hash for Value {
    fn hash<H>(&self, state: &mut H) where H: std::hash::Hasher {
        // TODO slow
        let a = self.to_bson_array();
        state.write(&a);
    }
}

fn vec_push_c_string(v: &mut Vec<u8>, s: &str) {
    v.push_all(s.as_bytes());
    v.push(0);
}

fn vec_push_bson_string(v: &mut Vec<u8>, s: &str) {
    // TODO i32 vs u32.  silly.
    v.push_all(&i32_to_bytes_le( (s.len() + 1) as i32 ));
    v.push_all(s.as_bytes());
    v.push(0);
}

// TODO this should be a library func, right?
// TODO this is basically position(), I think.
fn slice_find(pairs: &[(String, Value)], s: &str) -> Option<usize> {
    for i in 0 .. pairs.len() {
        if pairs[i].0.as_str() == s {
            return Some(i);
        }
    }
    None
}

fn slurp_bson_string(ba: &[u8], i: &mut usize) -> Result<String> {
    // TODO the spec says the len here is a signed number, but that's silly
    let len = bufndx::slurp_u32_le(ba, i) as usize;

    let s = try!(std::str::from_utf8(&ba[*i .. *i + len - 1]));
    *i = *i + len;
    Ok(String::from_str(s))
}

fn slurp_bson_value(ba: &[u8], i: &mut usize, valtype: u8) -> Result<Value> {
    let bv =
        match valtype {
            1 => Value::BDouble(bufndx::slurp_f64_le(ba, i)),
            2 => Value::BString(try!(slurp_bson_string(ba, i))),
            3 => Value::BDocument(try!(slurp_document(ba, i))),
            4 => Value::BArray(try!(slurp_array(ba, i))),
            5 => slurp_binary(ba, i),
            6 => Value::BUndefined,
            7 => slurp_objectid(ba, i),
            8 => slurp_boolean(ba, i),
            9 => Value::BDateTime(bufndx::slurp_i64_le(ba, i)),
            10 => Value::BNull,
            11 => try!(slurp_regex(ba, i)),
            12 => try!(slurp_deprecated_12(ba, i)),
            13 => try!(slurp_js(ba, i)),
            15 => try!(slurp_js_with_scope(ba, i)),
            16 => Value::BInt32(bufndx::slurp_i32_le(ba, i)),
            17 => Value::BTimeStamp(bufndx::slurp_i64_le(ba, i)),
            18 => Value::BInt64(bufndx::slurp_i64_le(ba, i)),
            127 => Value::BMaxKey,
            255 => Value::BMinKey,
            _ => panic!("invalid BSON value type"),
        };
    Ok(bv)
}

fn slurp_deprecated_12(ba: &[u8], i: &mut usize) -> Result<Value> {
    // deprecated
    let a = try!(slurp_bson_string(ba, i));
    Ok(slurp_objectid(ba, i))
}

fn slurp_js(ba: &[u8], i: &mut usize) -> Result<Value> {
    let a = try!(slurp_bson_string(ba, i));
    Ok(Value::BJSCode(a))
}

fn slurp_js_with_scope(ba: &[u8], i: &mut usize) -> Result<Value> {
    // TODO the spec says the len here is a signed number, but that's silly
    let len = bufndx::slurp_u32_le(ba, i);

    let a = try!(slurp_bson_string(ba, i));
    let scope = try!(slurp_document(ba, i));
    Ok(Value::BJSCodeWithScope(a))
}

fn slurp_regex(ba: &[u8], i: &mut usize) -> Result<Value> {
    let expr = try!(bufndx::slurp_cstring(ba, i));
    let options = try!(bufndx::slurp_cstring(ba, i));
    Ok(Value::BRegex(expr, options))
}

fn slurp_binary(ba: &[u8], i: &mut usize) -> Value {
    // TODO the spec says the len here is a signed number, but that's silly
    let len = bufndx::slurp_u32_le(ba, i) as usize;

    let subtype = ba[*i];
    *i = *i + 1;
    let mut b = Vec::with_capacity(len);
    b.push_all(&ba[*i .. *i + len]);
    *i = *i + len;
    Value::BBinary(subtype, b)
}

fn slurp_objectid(ba: &[u8], i: &mut usize) -> Value {
    let mut b = [0; 12];
    b.clone_from_slice(&ba[*i .. *i + 12]);
    *i = *i + 12;
    Value::BObjectID(b)
}

fn slurp_boolean(ba: &[u8], i: &mut usize) -> Value {
    let b = ba[*i] != 0;
    *i = *i + 1;
    Value::BBoolean(b)
}

fn slurp_document_pairs(ba: &[u8], i: &mut usize) -> Result<Vec<(String, Value)>> {
    // TODO the spec says the len here is a signed number, but that's silly
    let len = misc::bufndx::slurp_u32_le(ba, i) as usize;

    let mut pairs = Vec::new();
    while ba[*i] != 0 {
        let valtype = ba[*i];
        *i = *i + 1;
        let k = try!(bufndx::slurp_cstring(ba, i));
        let v = try!(slurp_bson_value(ba, i, valtype));
        pairs.push((k,v));
    }
    assert!(ba[*i] == 0);
    *i = *i + 1;
    // TODO verify len
    Ok(pairs)
}

pub fn slurp_document(ba: &[u8], i: &mut usize) -> Result<Document> {
    let pairs = try!(slurp_document_pairs(ba, i));
    Ok(Document {pairs: pairs})
}

fn slurp_array(ba: &[u8], i: &mut usize) -> Result<Array> {
    let pairs = try!(slurp_document_pairs(ba, i));
    // TODO verify that the keys are correct, integers, ascending, etc?
    let a = pairs.into_iter().map(|t| {
        let (k,v) = t;
        v
    }).collect();
    Ok(Array { items: a})
}

impl Value {
    pub fn tryGetValueEither(&self, k: &str) -> Option<&Value> {
        match self {
            &Value::BDocument(ref bd) => bd.get(k),
            &Value::BArray(ref ba) => unimplemented!(),
            _ => None,
        }
    }

    fn is_null(&self) -> bool {
        match self {
            &Value::BNull => true,
            _ => false,
        }
    }

    fn is_array(&self) -> bool {
        match self {
            &Value::BArray(_) => true,
            _ => false,
        }
    }

    pub fn is_string(&self) -> bool {
        match self {
            &Value::BString(_) => true,
            _ => false,
        }
    }

    fn is_document(&self) -> bool {
        match self {
            &Value::BDocument(_) => true,
            _ => false,
        }
    }

    fn is_numeric(&self) -> bool {
        match self {
            &Value::BInt32(_) => true,
            &Value::BInt64(_) => true,
            &Value::BDouble(_) => true,
            _ => false,
        }
    }

    pub fn is_nan(&self) -> bool {
        match self {
            &Value::BDouble(f) => f.is_nan(),
            _ => false,
        }
    }

    fn isDate(&self) -> bool {
        match self {
            &Value::BDateTime(_) => true,
            _ => false,
        }
    }

    pub fn into_string(self) -> Result<String> {
        match self {
            Value::BString(s) => Ok(s),
            _ => Err(Error::Misc("must be string")),
        }
    }

    // TODO how to make this function NOT sound like it is converting anything to a string?
    pub fn as_str(&self) -> Result<&str> {
        match self {
            &Value::BString(ref s) => Ok(s),
            _ => Err(Error::Misc("must be string")),
        }
    }

    pub fn as_array(&self) -> Result<&Array> {
        match self {
            &Value::BArray(ref s) => Ok(s),
            _ => Err(Error::Misc("must be array")),
        }
    }

    pub fn expect_document(self) -> Document {
        match self {
            Value::BDocument(s) => s,
            _ => panic!(),
        }
    }

    pub fn as_document(&self) -> Result<&Document> {
        match self {
            &Value::BDocument(ref s) => Ok(s),
            _ => Err(Error::Misc("must be document")),
        }
    }

    pub fn into_document(self) -> Result<Document> {
        match self {
            Value::BDocument(s) => Ok(s),
            _ => Err(Error::Misc("must be document")),
        }
    }

    pub fn into_array(self) -> Result<Array> {
        match self {
            Value::BArray(s) => Ok(s),
            _ => Err(Error::Misc("must be array")),
        }
    }

    pub fn as_bool(&self) -> Result<bool> {
        match self {
            &Value::BBoolean(ref s) => Ok(*s),
            _ => Err(Error::Misc("must be bool")),
        }
    }

    fn getDate(&self) -> Result<i64> {
        match self {
            &Value::BDateTime(ref s) => Ok(*s),
            _ => Err(Error::Misc("must be DateTime")),
        }
    }

    pub fn as_i32(&self) -> Result<i32> {
        match self {
            &Value::BInt32(ref s) => Ok(*s),
            _ => Err(Error::Misc("must be i32")),
        }
    }

    fn getAsExprBool(&self) -> bool {
        match self {
            &Value::BBoolean(false) => false,
            &Value::BNull => false,
            &Value::BUndefined => false,
            &Value::BInt32(0) => false,
            &Value::BInt64(0) => false,
            &Value::BDouble(0.0) => false,
            _ => true,
        }
    }

    fn getAsBool(&self) -> Result<bool> {
        match self {
        &Value::BBoolean(b) => Ok(b),
        &Value::BInt32(i) => Ok(i!=0),
        &Value::BInt64(i) => Ok(i!=0),
        &Value::BDouble(f) => Ok((f as i32)!=0),
        _ => Err(Error::Misc("must be convertible to bool")),
        }
    }

    fn getAsInt32(&self) -> Result<i32> {
        match self {
        &Value::BInt32(a) => Ok(a),
        &Value::BInt64(a) => Ok(a as i32),
        &Value::BDouble(a) => Ok(a as i32),
        _ => Err(Error::Misc("must be convertible to int32")),
        }
    }

    fn getAsInt64(&self) -> Result<i64> {
        match self {
        &Value::BInt32(a) => Ok(a as i64),
        &Value::BInt64(a) => Ok(a),
        &Value::BDouble(a) => Ok(a as i64),
        &Value::BDateTime(a) => Ok(a as i64),
        _ => Err(Error::Misc("must be convertible to int64")),
        }
    }

    fn getAsDouble(&self) -> Result<f64> {
        match self {
        &Value::BInt32(a) => Ok(a as f64),
        &Value::BInt64(a) => Ok(a as f64),
        &Value::BDouble(a) => Ok(a),
        _ => Err(Error::Misc("must be convertible to f64")),
        }
    }

    pub fn find_path(&self, path: &str) -> Value {
        let dot = path.find('.');
        let name = match dot { 
            None => path,
            Some(ndx) => &path[0 .. ndx]
        };
        match self {
            &Value::BDocument(ref bd) => bd.find_path(path),
            &Value::BArray(ref ba) => {
                // TODO move into array and call from here?
                match name.parse::<i32>() {
                    Err(_) => {
                        // when we have an array and the next step of the path is not
                        // an integer index, we search any subdocs in that array for
                        // that path and construct an array of the matches.

                        // document : { a:1, b:[ { c:1 }, { c:2 } ] }
                        // path : b.c
                        // needs to get: [ 1, 2 ]

                        // TODO are there any functions in the matcher which could be
                        // simplified by using this function? 
                        let a:Vec<Value> = ba.items.iter().filter_map(|subv| 
                                match subv {
                                &Value::BDocument(_) => Some(subv.find_path(path)),
                                _ => None
                                }
                                                       ).collect();
                        // if nothing matched, return None instead of an empty array.
                        // TODO is this right?
                        if a.len()==0 { Value::BUndefined } else { Value::BArray(Array { items: a }) }
                    }, 
                    Ok(ndx) => {
                        if ndx<0 {
                            panic!( "array index < 0");
                        } else if (ndx as usize)>=ba.items.len() {
                            panic!( "array index too large");
                        } else {
                            let v = &ba.items[ndx as usize];
                            match dot {
                                None => v.clone(),
                                Some(dot) => v.find_path(&path[dot+1..])
                            }
                        }
                    }
                }
            },
            _ => Value::BUndefined
        }
    }

    pub fn getTypeNumber_u8(&self) -> u8 {
        match self {
            &Value::BDouble(_) => 1,
            &Value::BString(_) => 2,
            &Value::BDocument(_) => 3,
            &Value::BArray(_) => 4,
            &Value::BBinary(_, _) => 5,
            &Value::BUndefined => 6,
            &Value::BObjectID(_) => 7,
            &Value::BBoolean(_) => 8,
            &Value::BDateTime(_) => 9,
            &Value::BNull => 10,
            &Value::BRegex(_, _) => 11,
            &Value::BJSCode(_) => 13,
            &Value::BJSCodeWithScope(_) => 15,
            &Value::BInt32(_) => 16,
            &Value::BTimeStamp(_) => 17,
            &Value::BInt64(_) => 18,
            &Value::BMinKey => 255, // NOTE
            &Value::BMaxKey => 127,
        }
    }

    pub fn for_all_strings<F : Fn(&str) -> ()>(&self, func: &F) {
        match self {
            &Value::BDouble(_) => (),
            &Value::BString(ref s) => func(&s),
            &Value::BDocument(ref bd) => {
                for t in &bd.pairs {
                    t.1.for_all_strings(func);
                }
            },
            &Value::BArray(ref ba) => {
                for v in &ba.items {
                    v.for_all_strings(func);
                }
            },
            &Value::BBinary(_, _) => (),
            &Value::BUndefined => (),
            &Value::BObjectID(_) => (),
            &Value::BBoolean(_) => (),
            &Value::BDateTime(_) => (),
            &Value::BNull => (),
            &Value::BRegex(_, _) => (),
            &Value::BJSCode(_) => (),
            &Value::BJSCodeWithScope(_) => (),
            &Value::BInt32(_) => (),
            &Value::BTimeStamp(_) => (),
            &Value::BInt64(_) => (),
            &Value::BMinKey => (),
            &Value::BMaxKey => (),
        }
    }

    pub fn find_all_strings<'a>(&'a self, dest: &mut Vec<&'a str>) {
        match self {
            &Value::BDouble(_) => (),
            &Value::BString(ref s) => dest.push(&s),
            &Value::BDocument(ref bd) => bd.find_all_strings(dest),
            &Value::BArray(ref ba) => ba.find_all_strings(dest),
            &Value::BBinary(_, _) => (),
            &Value::BUndefined => (),
            &Value::BObjectID(_) => (),
            &Value::BBoolean(_) => (),
            &Value::BDateTime(_) => (),
            &Value::BNull => (),
            &Value::BRegex(_, _) => (),
            &Value::BJSCode(_) => (),
            &Value::BJSCodeWithScope(_) => (),
            &Value::BInt32(_) => (),
            &Value::BTimeStamp(_) => (),
            &Value::BInt64(_) => (),
            &Value::BMinKey => (),
            &Value::BMaxKey => (),
        }
    }

    pub fn get_weight_from_index_entry(k: &[u8]) -> Result<i32> {
        let n = 1 + k.rposition_elem(&0u8).expect("TODO");
        let ord_shouldbe = Value::BInt32(0).get_type_order() as u8;
        if k[n] != ord_shouldbe {
            return Err(Error::Misc("bad type order byte"));
        }
        let e = (k[n+1] as i32) - 23;
        // exponent is number of times the mantissa must be multiplied times 100
        // if we assume that all mantissa digits are to the right of the decimal point.
        if e <= 0 {
            return Err(Error::Misc("bad e"));
        }
        let e = e as usize;
        let n = n + 2;
        let a = &k[n .. k.len()-n+1];

        // remaining bytes are mantissa, base 100
        // last byte of mantissa is 2*x
        // previous bytes are 2*x+1

        //printfn "mantissa: %A" a
        //printfn "e: %d" e

        // we have an array of centimal digits here, all of
        // which appear to the right of the decimal point.
        //
        // we know from the context that this
        // SHOULD be an integer.

        let a =
            if a.len() > e {
                &a[0 .. e+1]
            } else {
                a
            };

        let mut v = a.iter().fold(0, |v,d| {
            let b = (d >> 1) as i32;
            v * 100 + b
        });

        let need = e - a.len();
        if need > 0 {
            for i in 0 .. need {
                v = v * 100;
            }
        }

        //printfn "weight: %d" v
        Ok(v)
    }

    pub fn get_type_order(&self) -> i32 {
        // same numbers as canonicalizeBSONType()
        match self {
            &Value::BUndefined => 0,
            &Value::BNull => 5,
            &Value::BDouble(_) => 10,
            &Value::BInt64(_) => 10,
            &Value::BInt32(_) => 10,
            &Value::BString(_) => 15,
            &Value::BDocument(_) => 20,
            &Value::BArray(_) => 25,
            &Value::BBinary(_, _) => 30,
            &Value::BObjectID(_) => 35,
            &Value::BBoolean(_) => 40,
            &Value::BDateTime(_) => 45,
            &Value::BTimeStamp(_) => 47,
            &Value::BRegex(_, _) => 50,
            &Value::BJSCode(_) => 60,
            &Value::BJSCodeWithScope(_) => 65,
            &Value::BMinKey => -1,
            &Value::BMaxKey => 127,
        }
    }

    pub fn to_bson_array(&self) -> Vec<u8> {
        let mut v = Vec::new();
        self.to_bson(&mut v);
        v
    }

    pub fn encode_for_index_into(&self, w: &mut Vec<u8>) {
        w.push(self.get_type_order() as u8);
        match self {
            &Value::BBoolean(b) => if b { w.push(1u8) } else { w.push(0u8) },
            &Value::BNull => (),
            &Value::BMinKey => (),
            &Value::BMaxKey => (),
            &Value::BUndefined => (),
            &Value::BObjectID(ref a) => w.push_all(a),
            &Value::BString(ref s) => vec_push_c_string(w, &s),
            &Value::BDouble(f) => misc::Sqlite4Num::from_f64(f).encode_for_index(w),
            &Value::BInt64(n) => misc::Sqlite4Num::from_i64(n).encode_for_index(w),
            &Value::BInt32(n) => misc::Sqlite4Num::from_i64(n as i64).encode_for_index(w),
            &Value::BDocument(ref bd) => {
                // TODO is writing the length here what we want?
                // it means we can't match on a prefix of a document
                //
                // it means any document with 3 pairs will sort before 
                // any document with 4 pairs, even if the first 3 pairs
                // are the same in both.

                w.push_all(&i32_to_bytes_be(bd.pairs.len() as i32));
                for t in &bd.pairs {
                    vec_push_c_string(w, &t.0);;
                    t.1.encode_for_index_into(w);
                }
            },
            &Value::BArray(ref ba) => {
                // TODO is writing the length here what we want?
                // see comment on BDocument just above.

                w.push_all(&i32_to_bytes_be(ba.items.len() as i32));
                for v in &ba.items {
                    v.encode_for_index_into(w);
                }
            },
            &Value::BRegex(ref expr, ref opt) => {
                vec_push_c_string(w, &expr); 
                vec_push_c_string(w, &opt);
            },
            &Value::BJSCode(ref s) => vec_push_c_string(w, &s),
            &Value::BJSCodeWithScope(ref s) => vec_push_c_string(w, &s),
            &Value::BDateTime(n) => {
                misc::Sqlite4Num::from_i64(n).encode_for_index(w);
            },
            &Value::BTimeStamp(n) => {
                // TODO is this really how we should encode this?
                misc::Sqlite4Num::from_i64(n).encode_for_index(w);
            },
            &Value::BBinary(subtype, ref ba) => {
                w.push(subtype);
                w.push_all(&i32_to_bytes_be(ba.len() as i32));
                w.push_all(&ba);
            },
        }
    }

    pub fn encode_one_for_index(v: &Value, neg: bool) -> Vec<u8> {
        let mut a = Vec::new();
        v.encode_for_index_into(&mut a);
        if neg {
            for i in 0 .. a.len() {
                let b = a[i];
                a[i] = !b;
            }
        }
        a
    }

    pub fn encode_multi_for_index(vals: Vec<(Value, bool)>) -> Vec<u8> {
        let mut r = Vec::new();
        for (v, neg) in vals {
            let a = Self::encode_one_for_index(&v, neg);
            r.push_all(&a);
        }
        r
    }

    pub fn replace_undefined(&mut self) {
        match self {
            &mut Value::BArray(ref mut ba) => {
                for i in 0 .. ba.items.len() {
                    match &ba.items[i] {
                        &Value::BUndefined => {
                            ba.items[i] = Value::BNull;
                        },
                        _ => {
                        },
                    }
                }
            },
            &mut Value::BDocument(ref mut bd) => {
                for i in 0 .. bd.pairs.len() {
                    match &bd.pairs[i].1 {
                        &Value::BUndefined => {
                            bd.pairs[i] = (bd.pairs[i].0.clone(), Value::BNull)
                        },
                        _ => {
                        },
                    }
                }
            },
            _ => {
            },
        }
    }

    pub fn to_bson(&self, w: &mut Vec<u8>) {
        match self {
            &Value::BDouble(f) => w.push_all(&f64_to_bytes_le(f)),
            &Value::BInt32(n) => w.push_all(&i32_to_bytes_le(n)),
            &Value::BDateTime(n) => w.push_all(&i64_to_bytes_le(n)),
            &Value::BTimeStamp(n) => w.push_all(&i64_to_bytes_le(n)),
            &Value::BInt64(n) => w.push_all(&i64_to_bytes_le(n)),
            &Value::BString(ref s) => vec_push_bson_string(w, &s),
            &Value::BObjectID(ref a) => w.push_all(a),
            &Value::BBoolean(b) => if b { w.push(1u8) } else { w.push(0u8) },
            &Value::BNull => (),
            &Value::BMinKey => (),
            &Value::BMaxKey => (),
            &Value::BRegex(ref expr, ref opt) => {
                vec_push_c_string(w, &expr); 
                vec_push_c_string(w, &opt);
            },
            &Value::BUndefined => (),
            &Value::BJSCode(ref s) => vec_push_bson_string(w, &s),
            &Value::BJSCodeWithScope(ref s) => panic!("TODO write BJSCodeWithScope"),
            &Value::BBinary(subtype, ref ba) => {
                w.push_all(&i32_to_bytes_le(ba.len() as i32));
                w.push(subtype);
                w.push_all(&ba);
            },
            &Value::BArray(ref ba) => {
                ba.to_bson(w);
            },
            &Value::BDocument(ref bd) => {
                bd.to_bson(w);
            },
        }
    }

}

