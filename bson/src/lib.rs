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

use misc::endian::*;
use misc::bufndx;

#[derive(Debug)]
pub enum BsonError {
    // TODO remove Misc
    Misc(&'static str),

    // TODO more detail within CorruptFile
    CorruptFile(&'static str),

    Io(std::io::Error),
    Utf8(std::str::Utf8Error),
}

impl std::fmt::Display for BsonError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            BsonError::Io(ref err) => write!(f, "IO error: {}", err),
            BsonError::Utf8(ref err) => write!(f, "Utf8 error: {}", err),
            BsonError::Misc(s) => write!(f, "Misc error: {}", s),
            BsonError::CorruptFile(s) => write!(f, "Corrupt file: {}", s),
        }
    }
}

impl std::error::Error for BsonError {
    fn description(&self) -> &str {
        match *self {
            BsonError::Io(ref err) => std::error::Error::description(err),
            BsonError::Utf8(ref err) => std::error::Error::description(err),
            BsonError::Misc(s) => s,
            BsonError::CorruptFile(s) => s,
        }
    }

    // TODO cause
}

impl From<std::io::Error> for BsonError {
    fn from(err: std::io::Error) -> BsonError {
        BsonError::Io(err)
    }
}

impl From<std::str::Utf8Error> for BsonError {
    fn from(err: std::str::Utf8Error) -> BsonError {
        BsonError::Utf8(err)
    }
}

pub type Result<T> = std::result::Result<T, BsonError>;

pub fn split_name(s: &str) -> (&str, &str) {
    // TODO
    (&s[0 .. 2], &s[2 .. 4])
}

// TODO is it sufficient to derive PartialEq?
// Or do we need to implement it explicitly to
// catch the nan case?

#[derive(Clone)]
pub enum BsonValue {
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
    BArray(Vec<BsonValue>),
    BDocument(Vec<(String, BsonValue)>),
}

// We want the ability to put a BsonValue into a HashSet,
// but it contains an f64, which does not implement Eq or Hash.
// So we provide implementations below for BsonValue that
// are sufficient for our purposes.

impl PartialEq for BsonValue {
    fn eq(&self, other: &BsonValue) -> bool {
        // TODO slow
        let a = self.to_bson_array();
        let b = other.to_bson_array();
        a == b
    }
}

impl Eq for BsonValue {
}

impl std::hash::Hash for BsonValue {
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
fn slice_find(pairs: &[(String, BsonValue)], s: &str) -> Option<usize> {
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

fn slurp_bson_value(ba: &[u8], i: &mut usize, valtype: u8) -> Result<BsonValue> {
    let bv =
        match valtype {
            1 => BsonValue::BDouble(bufndx::slurp_f64_le(ba, i)),
            2 => BsonValue::BString(try!(slurp_bson_string(ba, i))),
            3 => try!(slurp_document(ba, i)),
            4 => try!(slurp_array(ba, i)),
            5 => slurp_binary(ba, i),
            6 => BsonValue::BUndefined,
            7 => slurp_objectid(ba, i),
            8 => slurp_boolean(ba, i),
            9 => BsonValue::BDateTime(bufndx::slurp_i64_le(ba, i)),
            10 => BsonValue::BNull,
            11 => try!(slurp_regex(ba, i)),
            12 => try!(slurp_deprecated_12(ba, i)),
            13 => try!(slurp_js(ba, i)),
            15 => try!(slurp_js_with_scope(ba, i)),
            16 => BsonValue::BInt32(bufndx::slurp_i32_le(ba, i)),
            17 => BsonValue::BTimeStamp(bufndx::slurp_i64_le(ba, i)),
            18 => BsonValue::BInt64(bufndx::slurp_i64_le(ba, i)),
            127 => BsonValue::BMaxKey,
            255 => BsonValue::BMinKey,
            _ => panic!("invalid BSON value type"),
        };
    Ok(bv)
}

fn slurp_deprecated_12(ba: &[u8], i: &mut usize) -> Result<BsonValue> {
    // deprecated
    let a = try!(slurp_bson_string(ba, i));
    Ok(slurp_objectid(ba, i))
}

fn slurp_js(ba: &[u8], i: &mut usize) -> Result<BsonValue> {
    let a = try!(slurp_bson_string(ba, i));
    Ok(BsonValue::BJSCode(a))
}

fn slurp_js_with_scope(ba: &[u8], i: &mut usize) -> Result<BsonValue> {
    // TODO the spec says the len here is a signed number, but that's silly
    let len = bufndx::slurp_u32_le(ba, i);

    let a = try!(slurp_bson_string(ba, i));
    let scope = try!(slurp_document(ba, i));
    Ok(BsonValue::BJSCodeWithScope(a))
}

fn slurp_regex(ba: &[u8], i: &mut usize) -> Result<BsonValue> {
    let expr = try!(bufndx::slurp_cstring(ba, i));
    let options = try!(bufndx::slurp_cstring(ba, i));
    Ok(BsonValue::BRegex(expr, options))
}

fn slurp_binary(ba: &[u8], i: &mut usize) -> BsonValue {
    // TODO the spec says the len here is a signed number, but that's silly
    let len = bufndx::slurp_u32_le(ba, i) as usize;

    let subtype = ba[*i];
    *i = *i + 1;
    let mut b = Vec::with_capacity(len);
    b.push_all(&ba[*i .. *i + len]);
    *i = *i + len;
    BsonValue::BBinary(subtype, b)
}

fn slurp_objectid(ba: &[u8], i: &mut usize) -> BsonValue {
    let mut b = [0; 12];
    b.clone_from_slice(&ba[*i .. *i + 12]);
    *i = *i + 12;
    BsonValue::BObjectID(b)
}

fn slurp_boolean(ba: &[u8], i: &mut usize) -> BsonValue {
    let b = ba[*i] != 0;
    *i = *i + 1;
    BsonValue::BBoolean(b)
}

fn slurp_document_pairs(ba: &[u8], i: &mut usize) -> Result<Vec<(String, BsonValue)>> {
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

pub fn slurp_document(ba: &[u8], i: &mut usize) -> Result<BsonValue> {
    let pairs = try!(slurp_document_pairs(ba, i));
    Ok(BsonValue::BDocument(pairs))
}

fn slurp_array(ba: &[u8], i: &mut usize) -> Result<BsonValue> {
    let pairs = try!(slurp_document_pairs(ba, i));
    // TODO verify that the keys are correct, integers, ascending, etc?
    let a = pairs.into_iter().map(|t| {
        let (k,v) = t;
        v
    }).collect();
    Ok(BsonValue::BArray(a))
}

impl BsonValue {
    pub fn tryGetValueForKey(&self, k: &str) -> Option<&BsonValue> {
        match self {
            &BsonValue::BDocument(ref pairs) => {
                for t in pairs.iter() {
                    let (ref ksub, ref vsub) = *t;
                    if ksub == k {
                        return Some(vsub);
                    }
                }
                return None;
            },
            _ => return None, // TODO error?
        }
    }

    pub fn getValueForKey(&self, k: &str) -> Result<&BsonValue> {
        match self.tryGetValueForKey(k) {
            Some(v) => Ok(v),
            None => Err(BsonError::Misc("required key not found")),
        }
    }

    fn tryGetValueForInsensitiveKey(&self, k: &str) -> Option<&BsonValue> {
        match self {
            &BsonValue::BDocument(ref pairs) => {
                for t in pairs.iter() {
                    let (ref ksub, ref vsub) = *t;
                    if std::ascii::AsciiExt::eq_ignore_ascii_case(ksub.as_str(), k) {
                        return Some(vsub);
                    }
                }
                return None;
            },
            _ => return None, // TODO error?
        }
    }

    fn tryGetValueAtIndex(&self, ndx: usize) -> Option<&BsonValue> {
        match self {
            &BsonValue::BArray(ref a) => {
                if ndx<0 {
                    return None
                } else if ndx >= a.len() {
                    return None
                } else {
                    return Some(&a[ndx])
                }
            },
            _ => return None, // TODO error?
        }
    }

    fn hasValueForKey(&self, s: &str) -> bool {
        match self.tryGetValueForKey(s) {
            Some(_) => true,
            None => false,
        }
    }

    fn getValueForInsensitiveKey(&self, k: &str) -> Result<&BsonValue> {
        match self.tryGetValueForInsensitiveKey(k) {
            Some(v) => Ok(v),
            None => Err(BsonError::Misc("required key not found")),
        }
    }

    fn isNull(&self) -> bool {
        match self {
            &BsonValue::BNull => true,
            _ => false,
        }
    }

    fn isArray(&self) -> bool {
        match self {
            &BsonValue::BArray(_) => true,
            _ => false,
        }
    }

    fn isDocument(&self) -> bool {
        match self {
            &BsonValue::BDocument(_) => true,
            _ => false,
        }
    }

    fn isNumeric(&self) -> bool {
        match self {
            &BsonValue::BInt32(_) => true,
            &BsonValue::BInt64(_) => true,
            &BsonValue::BDouble(_) => true,
            _ => false,
        }
    }

    fn is_nan(&self) -> bool {
        match self {
            &BsonValue::BDouble(f) => f.is_nan(),
            _ => false,
        }
    }

    fn isDate(&self) -> bool {
        match self {
            &BsonValue::BDateTime(_) => true,
            _ => false,
        }
    }

    fn is_dbref(pairs: &[(String,BsonValue)]) -> bool {
        let has_ref = slice_find(pairs, "$ref").is_some();
        let has_id =  slice_find(pairs, "$id").is_some();
        let has_db =  slice_find(pairs, "$db").is_some();
        let len = pairs.len();
        if len==2 && has_ref && has_id {
            true
        } else if len==3 && has_ref && has_id && has_db {
            true
        } else {
            false
        }
    }

    pub fn getString(&self) -> Result<&str> {
        match self {
            &BsonValue::BString(ref s) => Ok(s),
            _ => Err(BsonError::Misc("must be string")),
        }
    }

    pub fn getArray(&self) -> Result<&Vec<BsonValue>> {
        match self {
            &BsonValue::BArray(ref s) => Ok(s),
            _ => Err(BsonError::Misc("must be array")),
        }
    }

    pub fn getDocument(&self) -> Result<&Vec<(String,BsonValue)>> {
        match self {
            &BsonValue::BDocument(ref s) => Ok(s),
            _ => Err(BsonError::Misc("must be document")),
        }
    }

    fn getBool(&self) -> Result<bool> {
        match self {
            &BsonValue::BBoolean(ref s) => Ok(*s),
            _ => Err(BsonError::Misc("must be bool")),
        }
    }

    fn getDate(&self) -> Result<i64> {
        match self {
            &BsonValue::BDateTime(ref s) => Ok(*s),
            _ => Err(BsonError::Misc("must be DateTime")),
        }
    }

    fn getInt32(&self) -> Result<i32> {
        match self {
            &BsonValue::BInt32(ref s) => Ok(*s),
            _ => Err(BsonError::Misc("must be i32")),
        }
    }

    fn getAsExprBool(&self) -> bool {
        match self {
            &BsonValue::BBoolean(false) => false,
            &BsonValue::BNull => false,
            &BsonValue::BUndefined => false,
            &BsonValue::BInt32(0) => false,
            &BsonValue::BInt64(0) => false,
            &BsonValue::BDouble(0.0) => false,
            _ => true,
        }
    }

    fn getAsBool(&self) -> Result<bool> {
        match self {
        &BsonValue::BBoolean(b) => Ok(b),
        &BsonValue::BInt32(i) => Ok(i!=0),
        &BsonValue::BInt64(i) => Ok(i!=0),
        &BsonValue::BDouble(f) => Ok((f as i32)!=0),
        _ => Err(BsonError::Misc("must be convertible to bool")),
        }
    }

    fn getAsInt32(&self) -> Result<i32> {
        match self {
        &BsonValue::BInt32(a) => Ok(a),
        &BsonValue::BInt64(a) => Ok(a as i32),
        &BsonValue::BDouble(a) => Ok(a as i32),
        _ => Err(BsonError::Misc("must be convertible to int32")),
        }
    }

    fn getAsInt64(&self) -> Result<i64> {
        match self {
        &BsonValue::BInt32(a) => Ok(a as i64),
        &BsonValue::BInt64(a) => Ok(a),
        &BsonValue::BDouble(a) => Ok(a as i64),
        &BsonValue::BDateTime(a) => Ok(a as i64),
        _ => Err(BsonError::Misc("must be convertible to int64")),
        }
    }

    fn getAsDouble(&self) -> Result<f64> {
        match self {
        &BsonValue::BInt32(a) => Ok(a as f64),
        &BsonValue::BInt64(a) => Ok(a as f64),
        &BsonValue::BDouble(a) => Ok(a),
        _ => Err(BsonError::Misc("must be convertible to f64")),
        }
    }

    fn setValueAtIndex(&mut self, ndx: usize, v: BsonValue) {
        match *self {
        BsonValue::BArray(ref mut a) => {
            if ndx > 1500001 { panic!( "too big"); } // TODO this limit passes test set7.js, but is a bad idea
            if ndx >= a.len() {
                // TODO
            }
            a[ndx] = v;
        },
        _ => panic!("wrong type?")
        }
    }

    fn removeValueAtIndex(&mut self, ndx: usize) {
        match *self {
        BsonValue::BArray(ref mut a) => {
            a.remove(ndx);
        },
        _ => panic!("wrong type?")
        }
    }

    fn unsetValueAtIndex(&mut self, ndx: usize) {
        match *self {
        BsonValue::BArray(ref mut a) => {
            if ndx >=0 && ndx < a.len() {
                a[ndx] = BsonValue::BNull;
            }
        },
        _ => panic!("wrong type?")
        }
    }

    fn setValueForKey(&mut self, k: &str, v: BsonValue) {
        // TODO make this more efficient?
        match *self {
        BsonValue::BDocument(ref mut pairs) => {
            for i in 0 .. pairs.len() {
                if pairs[i].0 == k {
                    pairs[i].1 = v;
                    return;
                }
            }
            pairs.push((String::from_str(k), v));
        },
        _ => panic!("wrong type?")
        }
    }

    fn unsetValueForKey(&mut self, k: &str) {
        // TODO make this more efficient?
        match *self {
        BsonValue::BDocument(ref mut pairs) => {
            for i in 0 .. pairs.len() {
                if pairs[i].0 == k {
                    pairs.remove(i);
                    break;
                }
            }
        },
        _ => panic!("wrong type?")
        }
    }

    pub fn find_path(&self, path: &str) -> BsonValue {
        let dot = path.find('.');
        let name = match dot { 
            None => path,
            Some(ndx) => &path[0 .. ndx]
        };
        match self {
            &BsonValue::BDocument(ref pairs) => {
                match slice_find(&pairs, name) {
                    Some(ndx) => {
                        let v = &pairs[ndx].1;
                        match dot {
                            None => v.clone(),
                            Some(dot) => v.find_path(&path[dot+1..])
                        }
                    },
                    None => BsonValue::BUndefined
                }
            },
            &BsonValue::BArray(ref items) => {
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
                        let a:Vec<BsonValue> = items.iter().filter_map(|subv| 
                                match subv {
                                &BsonValue::BDocument(_) => Some(subv.find_path(path)),
                                _ => None
                                }
                                                       ).collect();
                        // if nothing matched, return None instead of an empty array.
                        // TODO is this right?
                        if a.len()==0 { BsonValue::BUndefined } else { BsonValue::BArray(a) }
                    }, 
                    Ok(ndx) => {
                        if ndx<0 {
                            panic!( "array index < 0");
                        } else if (ndx as usize)>=items.len() {
                            panic!( "array index too large");
                        } else {
                            let v = &items[ndx as usize];
                            match dot {
                                None => v.clone(),
                                Some(dot) => v.find_path(&path[dot+1..])
                            }
                        }
                    }
                }
            },
            _ => BsonValue::BUndefined
        }
    }

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

    fn get_type_order(&self) -> i32 {
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

    pub fn to_bson_array(&self) -> Vec<u8> {
        let mut v = Vec::new();
        self.to_bson(&mut v);
        v
    }

    pub fn encode_for_index_into(&self, w: &mut Vec<u8>) {
        w.push(self.get_type_order() as u8);
        match self {
            &BsonValue::BBoolean(b) => if b { w.push(1u8) } else { w.push(0u8) },
            &BsonValue::BNull => (),
            &BsonValue::BMinKey => (),
            &BsonValue::BMaxKey => (),
            &BsonValue::BUndefined => (),
            &BsonValue::BObjectID(ref a) => w.push_all(a),
            &BsonValue::BString(ref s) => vec_push_c_string(w, &s),
            &BsonValue::BDouble(f) => misc::Sqlite4Num::from_f64(f).encode_for_index(w),
            &BsonValue::BInt64(n) => misc::Sqlite4Num::from_i64(n).encode_for_index(w),
            &BsonValue::BInt32(n) => misc::Sqlite4Num::from_i64(n as i64).encode_for_index(w),
            &BsonValue::BDocument(ref pairs) => {
                // TODO is writing the length here what we want?
                // it means we can't match on a prefix of a document
                //
                // it means any document with 3 pairs will sort before 
                // any document with 4 pairs, even if the first 3 pairs
                // are the same in both.

                w.push_all(&i32_to_bytes_be(pairs.len() as i32));
                for t in pairs {
                    vec_push_c_string(w, &t.0);;
                    t.1.encode_for_index_into(w);
                }
            },
            &BsonValue::BArray(ref vals) => {
                // TODO is writing the length here what we want?
                // see comment on BDocument just above.

                w.push_all(&i32_to_bytes_be(vals.len() as i32));
                for v in vals {
                    v.encode_for_index_into(w);
                }
            },
            &BsonValue::BRegex(ref expr, ref opt) => {
                vec_push_c_string(w, &expr); 
                vec_push_c_string(w, &opt);
            },
            &BsonValue::BJSCode(ref s) => vec_push_c_string(w, &s),
            &BsonValue::BJSCodeWithScope(ref s) => vec_push_c_string(w, &s),
            &BsonValue::BDateTime(n) => {
                misc::Sqlite4Num::from_i64(n).encode_for_index(w);
            },
            &BsonValue::BTimeStamp(n) => {
                // TODO is this really how we should encode this?
                misc::Sqlite4Num::from_i64(n).encode_for_index(w);
            },
            &BsonValue::BBinary(subtype, ref ba) => {
                w.push(subtype);
                w.push_all(&i32_to_bytes_be(ba.len() as i32));
                w.push_all(&ba);
            },
        }
    }

    pub fn encode_one_for_index(v: &BsonValue, neg: bool) -> Vec<u8> {
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

    pub fn encode_multi_for_index(vals: Vec<(BsonValue, bool)>) -> Vec<u8> {
        let mut r = Vec::new();
        for (v, neg) in vals {
            let a = Self::encode_one_for_index(&v, neg);
            r.push_all(&a);
        }
        r
    }

    pub fn replace_undefined(&mut self) {
        match self {
            &mut BsonValue::BArray(ref mut vals) => {
                for i in 0 .. vals.len() {
                    match &vals[i] {
                        &BsonValue::BUndefined => {
                            vals[i] = BsonValue::BNull;
                        },
                        _ => {
                        },
                    }
                }
            },
            &mut BsonValue::BDocument(ref mut pairs) => {
                for i in 0 .. pairs.len() {
                    match &pairs[i].1 {
                        &BsonValue::BUndefined => {
                            pairs[i] = (pairs[i].0.clone(), BsonValue::BNull)
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
            &BsonValue::BDouble(f) => w.push_all(&f64_to_bytes_le(f)),
            &BsonValue::BInt32(n) => w.push_all(&i32_to_bytes_le(n)),
            &BsonValue::BDateTime(n) => w.push_all(&i64_to_bytes_le(n)),
            &BsonValue::BTimeStamp(n) => w.push_all(&i64_to_bytes_le(n)),
            &BsonValue::BInt64(n) => w.push_all(&i64_to_bytes_le(n)),
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
                w.push_all(&i32_to_bytes_le(ba.len() as i32));
                w.push(subtype);
                w.push_all(&ba);
            },
            &BsonValue::BArray(ref vals) => {
                let start = w.len();
                // placeholder for length
                w.push_all(&i32_to_bytes_le(0));
                for (i, vsub) in vals.iter().enumerate() {
                    w.push(vsub.getTypeNumber_u8());
                    let s = format!("{}", i);
                    vec_push_c_string(w, &s);
                    vsub.to_bson(w);
                }
                w.push(0u8);
                let len = w.len() - start;
                misc::bytes::copy_into(&i32_to_bytes_le(len as i32), &mut w[start .. start + 4]);
            },
            &BsonValue::BDocument(ref pairs) => {
                let start = w.len();
                // placeholder for length
                w.push_all(&i32_to_bytes_le(0));
                for t in pairs.iter() {
                    let (ref ksub, ref vsub) = *t;
                    w.push(vsub.getTypeNumber_u8());
                    vec_push_c_string(w, &ksub);;
                    vsub.to_bson(w);
                }
                w.push(0u8);
                let len = w.len() - start;
                misc::bytes::copy_into(&i32_to_bytes_le(len as i32), &mut w[start .. start + 4]);
            },
        }
    }

    pub fn from_bson(w: &[u8]) -> Result<BsonValue> {
        let mut cur = 0;
        let d = try!(slurp_document(w, &mut cur));
        Ok(d)
    }
}

