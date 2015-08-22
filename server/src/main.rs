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

// This server exists so that we can run the jstests suite (from the MongoDB
// source repo on GitHub) against Elmo.  It is *not* expected that an actual
// server listening on a socket would be useful for the common use cases on 
// a mobile device.

#![feature(box_syntax)]
#![feature(convert)]
#![feature(associated_consts)]
#![feature(vec_push_all)]
#![feature(result_expect)]

extern crate misc;

use misc::endian;
use misc::bufndx;

extern crate bson;

extern crate elmo;

extern crate elmo_sqlite3;

use std::io;
use std::io::Read;
use std::io::Write;

#[derive(Debug)]
enum Error {
    // TODO remove Misc
    Misc(&'static str),

    // TODO more detail within CorruptFile
    CorruptFile(&'static str),

    Whatever(Box<std::error::Error>),

    // TODO do we need the following now that we have Whatever?
    Bson(bson::Error),
    Io(std::io::Error),
    Utf8(std::str::Utf8Error),
    Elmo(elmo::Error),
}

fn wrap_err<E: std::error::Error + 'static>(err: E) -> Error {
    Error::Whatever(box err)
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Error::Bson(ref err) => write!(f, "bson error: {}", err),
            Error::Elmo(ref err) => write!(f, "elmo error: {}", err),
            Error::Io(ref err) => write!(f, "IO error: {}", err),
            Error::Utf8(ref err) => write!(f, "Utf8 error: {}", err),
            Error::Misc(s) => write!(f, "Misc error: {}", s),
            Error::CorruptFile(s) => write!(f, "Corrupt file: {}", s),
            Error::Whatever(ref err) => write!(f, "Other error: {}", err),
        }
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Bson(ref err) => std::error::Error::description(err),
            Error::Elmo(ref err) => std::error::Error::description(err),
            Error::Io(ref err) => std::error::Error::description(err),
            Error::Utf8(ref err) => std::error::Error::description(err),
            Error::Misc(s) => s,
            Error::CorruptFile(s) => s,
            Error::Whatever(ref err) => std::error::Error::description(&**err),
        }
    }

    // TODO cause
}

impl From<bson::Error> for Error {
    fn from(err: bson::Error) -> Error {
        Error::Bson(err)
    }
}

impl From<elmo::Error> for Error {
    fn from(err: elmo::Error) -> Error {
        Error::Elmo(err)
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(err: std::str::Utf8Error) -> Error {
        Error::Utf8(err)
    }
}

impl From<Box<std::error::Error>> for Error {
    fn from(err: Box<std::error::Error>) -> Error {
        Error::Whatever(err)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
struct Reply {
    req_id : i32,
    response_to : i32,
    flags : i32,
    cursor_id : i64,
    starting_from : i32,
    docs : Vec<bson::Document>,
}

#[derive(Debug)]
// TODO consider calling this Msg2004
struct MsgQuery {
    req_id : i32,
    flags : i32,
    full_collection_name : String,
    number_to_skip : i32,
    number_to_return : i32,
    query : bson::Document,
    return_fields_selector : Option<bson::Document>,
}

#[derive(Debug)]
struct MsgGetMore {
    req_id : i32,
    full_collection_name : String,
    number_to_return : i32,
    cursor_id : i64,
}

#[derive(Debug)]
struct MsgKillCursors {
    req_id : i32,
    cursor_ids : Vec<i64>,
}

#[derive(Debug)]
enum Request {
    Query(MsgQuery),
    GetMore(MsgGetMore),
    KillCursors(MsgKillCursors),
}

impl Reply {
    fn encode(&self) -> Box<[u8]> {
        let mut w = Vec::new();
        // length placeholder
        w.push_all(&[0u8; 4]);
        w.push_all(&endian::i32_to_bytes_le(self.req_id));
        w.push_all(&endian::i32_to_bytes_le(self.response_to));
        w.push_all(&endian::u32_to_bytes_le(1u32)); 
        w.push_all(&endian::i32_to_bytes_le(self.flags));
        w.push_all(&endian::i64_to_bytes_le(self.cursor_id));
        w.push_all(&endian::i32_to_bytes_le(self.starting_from));
        w.push_all(&endian::u32_to_bytes_le(self.docs.len() as u32));
        for doc in &self.docs {
            doc.to_bson(&mut w);
        }
        misc::bytes::copy_into(&endian::u32_to_bytes_le(w.len() as u32), &mut w[0 .. 4]);
        w.into_boxed_slice()
    }
}

fn vec_rows_to_values(v: Vec<elmo::Row>) -> Vec<bson::Value> {
    v.into_iter().map(|r| r.doc).collect::<Vec<_>>()
}

fn vec_values_to_docs(v: Vec<bson::Value>) -> Result<Vec<bson::Document>> {
    let a = try!(v.into_iter().map(|d| d.into_document()).collect::<std::result::Result<Vec<_>, bson::Error>>());
    Ok(a)
}

fn vec_docs_to_values(v: Vec<bson::Document>) -> Vec<bson::Value> {
    v.into_iter().map(|d| bson::Value::BDocument(d)).collect::<Vec<_>>()
}

fn parse_request(ba: &[u8]) -> Result<Request> {
    let mut i = 0;
    let (message_len,req_id,response_to,op_code) = slurp_header(ba, &mut i);
    match op_code {
        2004 => {
            let flags = bufndx::slurp_i32_le(ba, &mut i);
            let full_collection_name = try!(bufndx::slurp_cstring(ba, &mut i));
            let number_to_skip = bufndx::slurp_i32_le(ba, &mut i);
            let number_to_return = bufndx::slurp_i32_le(ba, &mut i);
            let query = try!(bson::slurp_document(ba, &mut i));
            let return_fields_selector = if i < ba.len() { Some(try!(bson::slurp_document(ba, &mut i))) } else { None };

            let msg = MsgQuery {
                req_id: req_id,
                flags: flags,
                full_collection_name: full_collection_name,
                number_to_skip: number_to_skip,
                number_to_return: number_to_return,
                query: query,
                return_fields_selector: return_fields_selector,
            };
            Ok(Request::Query(msg))
        },

        2005 => {
            let flags = bufndx::slurp_i32_le(ba, &mut i);
            let full_collection_name = try!(bufndx::slurp_cstring(ba, &mut i));
            let number_to_return = bufndx::slurp_i32_le(ba, &mut i);
            let cursor_id = bufndx::slurp_i64_le(ba, &mut i);

            let msg = MsgGetMore {
                req_id: req_id,
                full_collection_name: full_collection_name,
                number_to_return: number_to_return,
                cursor_id: cursor_id,
            };
            Ok(Request::GetMore(msg))
        },

        2007 => {
            let flags = bufndx::slurp_i32_le(ba, &mut i);
            let number_of_cursor_ids = bufndx::slurp_i32_le(ba, &mut i);
            let mut cursor_ids = Vec::new();
            for _ in 0 .. number_of_cursor_ids {
                cursor_ids.push(bufndx::slurp_i64_le(ba, &mut i));
            }

            let msg = MsgKillCursors {
                req_id: req_id,
                cursor_ids: cursor_ids,
            };
            Ok(Request::KillCursors(msg))
        },

        _ => {
            Err(Error::CorruptFile("unknown message opcode TODO"))
        },
    }
}

// TODO do these really need to be signed?
fn slurp_header(ba: &[u8], i: &mut usize) -> (i32,i32,i32,i32) {
    let message_len = bufndx::slurp_i32_le(ba, i);
    let req_id = bufndx::slurp_i32_le(ba, i);
    let response_to = bufndx::slurp_i32_le(ba, i);
    let op_code = bufndx::slurp_i32_le(ba, i);
    let v = (message_len, req_id, response_to, op_code);
    v
}

fn read_message_bytes(stream: &mut Read) -> Result<Option<Box<[u8]>>> {
    let mut a = [0; 4];
    let got = try!(misc::io::read_fully(stream, &mut a));
    if got == 0 {
        return Ok(None);
    }
    let message_len = endian::u32_from_bytes_le(a) as usize;
    let mut msg = vec![0; message_len]; 
    misc::bytes::copy_into(&a, &mut msg[0 .. 4]);
    let got = try!(misc::io::read_fully(stream, &mut msg[4 .. message_len]));
    if got != message_len - 4 {
        return Err(Error::CorruptFile("end of file at the wrong time"));
    }
    Ok(Some(msg.into_boxed_slice()))
}

fn create_reply(req_id: i32, docs: Vec<bson::Document>, cursor_id: i64) -> Reply {
    let msg = Reply {
        req_id: 0,
        response_to: req_id,
        flags: 0,
        cursor_id: cursor_id,
        starting_from: 0,
        // TODO
        docs: docs,
    };
    msg
}

fn reply_err(req_id: i32, err: Error) -> Reply {
    let mut doc = bson::Document::new_empty();
    // TODO stack trace was nice here
    //pairs.push(("errmsg", BString("exception: " + errmsg)));
    //pairs.push(("code", BInt32(code)));
    doc.set_i32("ok", 0);
    create_reply(req_id, vec![doc], 0)
}

// TODO mongo has a way of automatically killing a cursor after 10 minutes idle

struct Server<'a> {
    conn: elmo::Connection,
    cursor_num: i64,
    // TODO this is problematic when/if the Iterator has a reference to or the same lifetime
    // as self.conn.
    cursors: std::collections::HashMap<i64, (String, Box<Iterator<Item=Result<elmo::Row>> + 'a>)>,
}

impl<'b> Server<'b> {

    fn reply_whatsmyuri(&self, req: &MsgQuery) -> Result<Reply> {
        let mut doc = bson::Document::new_empty();
        doc.set_str("you", "127.0.0.1:65460");
        doc.set_i32("ok", 1);
        Ok(create_reply(req.req_id, vec![doc], 0))
    }

    fn reply_getlog(&self, req: &MsgQuery) -> Result<Reply> {
        let mut doc = bson::Document::new_empty();
        doc.set_i32("totalLinesWritten", 1);
        doc.set_array("log", bson::Array::new_empty());
        doc.set_i32("ok", 1);
        Ok(create_reply(req.req_id, vec![doc], 0))
    }

    fn reply_replsetgetstatus(&self, req: &MsgQuery) -> Result<Reply> {
        let mut mine = bson::Document::new_empty();
        mine.set_i32("_id", 0);
        mine.set_str("name", "whatever");
        mine.set_i32("state", 1);
        mine.set_f64("health", 1.0);
        mine.set_str("stateStr", "PRIMARY");
        mine.set_i32("uptime", 0);
        mine.set_timestamp("optime", 0);
        mine.set_datetime("optimeDate", 0);
        mine.set_timestamp("electionTime", 0);
        mine.set_timestamp("electionDate", 0);
        mine.set_bool("self", true);

        let mut doc = bson::Document::new_empty();
        doc.set_document("mine", mine);
        doc.set_str("set", "TODO");
        doc.set_datetime("date", 0);
        doc.set_i32("myState", 1);
        doc.set_i32("ok", 1);

        Ok(create_reply(req.req_id, vec![doc], 0))
    }

    fn reply_ismaster(&self, req: &MsgQuery) -> Result<Reply> {
        let mut doc = bson::Document::new_empty();
        doc.set_bool("ismaster", true);
        doc.set_bool("secondary", false);
        doc.set_i32("maxWireVersion", 3);
        doc.set_i32("minWireVersion", 2);
        // ver >= 2:  we don't support the older fire-and-forget write operations. 
        // ver >= 3:  we don't support the older form of explain
        // TODO if we set minWireVersion to 3, which is what we want to do, so
        // that we can tell the client that we don't support the older form of
        // explain, what happens is that we start getting the old fire-and-forget
        // write operations instead of the write commands that we want.
        doc.set_i32("ok", 1);
        Ok(create_reply(req.req_id, vec![doc], 0))
    }

    fn reply_admin_cmd(&self, req: &MsgQuery, db: &str) -> Result<Reply> {
        if req.query.pairs.is_empty() {
            Err(Error::Misc("empty query"))
        } else {
            // this code assumes that the first key is always the command
            let cmd = req.query.pairs[0].0.as_str();
            // TODO let cmd = cmd.ToLower();
            let res =
                match cmd {
                    "whatsmyuri" => self.reply_whatsmyuri(req),
                    "getLog" => self.reply_getlog(req),
                    "replSetGetStatus" => self.reply_replsetgetstatus(req),
                    "isMaster" => self.reply_ismaster(req),
                    _ => Err(Error::Misc("unknown cmd"))
                };
            res
        }
    }

    fn reply_delete(&self, req: &MsgQuery, db: &str) -> Result<Reply> {
        let q = &req.query;
        let coll = try!(q.must_get_str("delete"));
        let deletes = try!(q.must_get_array("deletes"));
        // TODO limit
        // TODO ordered
        let result = try!(self.conn.delete(db, coll, &deletes.items));
        let mut doc = bson::Document::new_empty();
        doc.set_i32("ok", result as i32);
        doc.set_i32("ok", 1);
        Ok(create_reply(req.req_id, vec![doc], 0))
    }

    fn reply_insert(&self, mut req: MsgQuery, db: &str) -> Result<Reply> {
        let coll = try!(req.query.must_remove_string("insert"));

        let docs = try!(req.query.must_remove("documents"));
        let docs = try!(docs.into_array());
        let mut docs = try!(vec_values_to_docs(docs.items));

        // TODO ordered
        // TODO do we need to keep ownership of docs?
        let results = try!(self.conn.insert(db, &coll, &mut docs));
        let mut errors = Vec::new();
        for i in 0 .. results.len() {
            if results[i].is_err() {
                let msg = format!("{:?}", results[i]);
                let err = bson::Value::BDocument(bson::Document {pairs: vec![(String::from("index"), bson::Value::BInt32(i as i32)), (String::from("errmsg"), bson::Value::BString(msg))]});
                errors.push(err);
            }
        }
        let mut doc = bson::Document::new_empty();
        doc.set_i32("n", ((results.len() - errors.len()) as i32));
        if errors.len() > 0 {
            doc.set_array("writeErrors", bson::Array {items: errors});
        }
        doc.set_i32("ok", 1);
        Ok(create_reply(req.req_id, vec![doc], 0))
    }

    fn store_cursor<T: Iterator<Item=Result<elmo::Row>> + 'b>(&mut self, ns: &str, seq: T) -> i64 {
        self.cursor_num = self.cursor_num + 1;
        self.cursors.insert(self.cursor_num, (String::from(ns), box seq));
        self.cursor_num
    }

    fn remove_cursors_for_collection(&mut self, ns: &str) {
        let remove = self.cursors.iter().filter_map(|(&num, &(ref s, _))| if s.as_str() == ns { Some(num) } else { None }).collect::<Vec<_>>();
        for cursor_num in remove {
            self.cursors.remove(&cursor_num);
        }
    }

    // grab is just a take() which doesn't take ownership of the iterator
    // TODO investigate by_ref()
    fn grab<T: Iterator<Item=Result<elmo::Row>>>(seq: &mut T, n: usize) -> Result<Vec<elmo::Row>> {
        let mut r = Vec::new();
        for _ in 0 .. n {
            match seq.next() {
                None => {
                    break;
                },
                Some(v) => {
                    r.push(try!(v));
                },
            }
        }
        Ok(r)
    }

    // this is the older way of returning a cursor.
    fn do_limit<T: Iterator<Item=Result<elmo::Row>>>(ns: &str, seq: &mut T, number_to_return: i32) -> Result<(Vec<elmo::Row>, bool)> {
        if number_to_return < 0 || number_to_return == 1 {
            // hard limit.  do not return a cursor.
            let n = if number_to_return < 0 {
                -number_to_return
            } else {
                number_to_return
            };
            if n < 0 {
                // TODO can rust overflow handling deal with this?
                panic!("overflow");
            }
            let docs = try!(seq.take(n as usize).collect::<Result<Vec<_>>>());
            Ok((docs, false))
        } else if number_to_return == 0 {
            // return whatever the default size is
            // TODO for now, just return them all and close the cursor
            let docs = try!(seq.collect::<Result<Vec<_>>>());
            Ok((docs, false))
        } else {
            // soft limit.  keep cursor open.
            let docs = try!(Self::grab(seq, number_to_return as usize));
            if docs.len() > 0 {
                Ok((docs, true))
            } else {
                Ok((docs, false))
            }
        }
    }

    // this is a newer way of returning a cursor.  used by the agg framework.
    fn reply_with_cursor<T: Iterator<Item=Result<elmo::Row>> + 'static>(&mut self, ns: &str, mut seq: T, cursor_options: Option<&bson::Value>, default_batch_size: usize) -> Result<bson::Document> {
        let number_to_return =
            match cursor_options {
                Some(&bson::Value::BDocument(ref bd)) => {
                    if bd.pairs.iter().any(|&(ref k, _)| k != "batchSize") {
                        return Err(Error::Misc("invalid cursor option"));
                    }
                    match bd.pairs.iter().find(|&&(ref k, ref _v)| k == "batchSize") {
                        Some(&(_, bson::Value::BInt32(n))) => {
                            if n < 0 {
                                return Err(Error::Misc("batchSize < 0"));
                            }
                            Some(n as usize)
                        },
                        Some(&(_, bson::Value::BDouble(n))) => {
                            if n < 0.0 {
                                return Err(Error::Misc("batchSize < 0"));
                            }
                            Some(n as usize)
                        },
                        Some(&(_, bson::Value::BInt64(n))) => {
                            if n < 0 {
                                return Err(Error::Misc("batchSize < 0"));
                            }
                            Some(n as usize)
                        },
                        Some(_) => {
                            return Err(Error::Misc("batchSize not numeric"));
                        },
                        None => {
                            Some(default_batch_size)
                        },
                    }
                },
                Some(_) => {
                    return Err(Error::Misc("invalid cursor option"));
                },
                None => {
                    // TODO in the case where the cursor is not requested, how
                    // many should we return?  For now we return all of them,
                    // which for now we flag by setting number_to_return to None,
                    // which is handled as a special case below.
                    None
                },
        };

        let (docs, cursor_id) =
            match number_to_return {
                None => {
                    let docs = try!(seq.collect::<Result<Vec<_>>>());
                    (docs, None)
                },
                Some(0) => {
                    // if 0, return nothing but keep the cursor open.
                    // but we need to eval the first item in the seq,
                    // to make sure that an error gets found now.
                    // but we can't consume that first item and let it
                    // get lost.  so we grab a batch but then put it back.

                    // TODO peek, or something
                    let cursor_id = self.store_cursor(ns, seq);
                    (Vec::new(), Some(cursor_id))
                },
                Some(n) => {
                    let docs = try!(Self::grab(&mut seq, n));
                    if docs.len() == n {
                        // if we grabbed the same number we asked for, we assume the
                        // sequence has more, so we store the cursor and return it.
                        let cursor_id = self.store_cursor(ns, seq);
                        (docs, Some(cursor_id))
                    } else {
                        // but if we got less than we asked for, we assume we have
                        // consumed the whole sequence.
                        (docs, None)
                    }
                },
            };


        let mut doc = bson::Document::new_empty();
        match cursor_id {
            Some(cursor_id) => {
                let mut cursor = bson::Document::new_empty();
                cursor.set_i64("id", cursor_id);
                cursor.set_str("ns", ns);
                cursor.set_array("firstBatch", bson::Array { items: vec_rows_to_values(docs)});
            },
            None => {
                doc.set_array("result", bson::Array { items: vec_rows_to_values(docs)});
            },
        }
        doc.set_i32("ok", 1);
        Ok(doc)
    }

    fn reply_create_collection(&self, req: &MsgQuery, db: &str) -> Result<Reply> {
        let q = &req.query;
        let coll = try!(req.query.must_get_str("create"));
        let mut options = bson::Document::new_empty();
        // TODO maybe just pass everything through instead of looking for specific options
        match q.get("autoIndexId") {
            Some(&bson::Value::BBoolean(b)) => options.set_bool("autoIndexId", b),
            // TODO error on bad values?
            _ => (),
        }
        match q.get("temp") {
            Some(&bson::Value::BBoolean(b)) => options.set_bool("temp", b),
            // TODO error on bad values?
            _ => (),
        }
        match q.get("capped") {
            Some(&bson::Value::BBoolean(b)) => options.set_bool("capped", b),
            // TODO error on bad values?
            _ => (),
        }
        match q.get("size") {
            Some(&bson::Value::BInt32(n)) => options.set_i64("size", n as i64),
            Some(&bson::Value::BInt64(n)) => options.set_i64("size", n as i64),
            Some(&bson::Value::BDouble(n)) => options.set_i64("size", n as i64),
            // TODO error on bad values?
            _ => (),
        }
        match q.get("max") {
            Some(&bson::Value::BInt32(n)) => options.set_i64("max", n as i64),
            Some(&bson::Value::BInt64(n)) => options.set_i64("max", n as i64),
            Some(&bson::Value::BDouble(n)) => options.set_i64("max", n as i64),
            // TODO error on bad values?
            _ => (),
        }
        // TODO more options here ?
        let result = try!(self.conn.create_collection(db, coll, options));
        let mut doc = bson::Document::new_empty();
        doc.set_i32("ok", 1);
        Ok(create_reply(req.req_id, vec![doc], 0))
    }

    fn reply_create_indexes(&mut self, req: &MsgQuery, db: &str) -> Result<Reply> {
        let coll = try!(req.query.must_get_str("createIndexes"));
        let indexes = try!(req.query.must_get_array("indexes"));
        let indexes = indexes.items.iter().map(
            |d| {
                // TODO get these from d
                let spec = bson::Document::new_empty();
                let options = bson::Document::new_empty();
                elmo::IndexInfo {
                    db: String::from(db),
                    coll: String::from(coll),
                    name: String::from("TODO"),
                    spec: spec,
                    options: options,
                }
            }
            ).collect::<Vec<_>>();

        let result = try!(self.conn.create_indexes(indexes));
        // TODO createdCollectionAutomatically
        // TODO numIndexesBefore
        // TODO numIndexesAfter
        let mut doc = bson::Document::new_empty();
        doc.set_i32("ok", 1);
        Ok(create_reply(req.req_id, vec![doc], 0))
    }

    fn reply_delete_indexes(&mut self, req: &MsgQuery, db: &str) -> Result<Reply> {
        let coll = try!(req.query.must_get_str("deleteIndexes"));
        {
            // TODO is it safe/correct/necessary to remove the cursors BEFORE?
            let full_coll = format!("{}.{}", db, coll);
            self.remove_cursors_for_collection(&full_coll);
        }
        let index = try!(req.query.must_get("index"));
        let (count_indexes_before, num_indexes_deleted) = try!(self.conn.delete_indexes(db, coll, index));
        let mut doc = bson::Document::new_empty();
        doc.set_i32("ok", 1);
        Ok(create_reply(req.req_id, vec![doc], 0))
    }

    fn reply_drop_collection(&mut self, req: &MsgQuery, db: &str) -> Result<Reply> {
        let coll = try!(req.query.must_get_str("drop"));
        {
            // TODO is it safe/correct/necessary to remove the cursors BEFORE?
            let full_coll = format!("{}.{}", db, coll);
            self.remove_cursors_for_collection(&full_coll);
        }
        let deleted = try!(self.conn.drop_collection(db, coll));
        let mut doc = bson::Document::new_empty();
        if deleted {
            doc.set_i32("ok", 1);
        } else {
            // mongo shell apparently cares about this exact error message string
            doc.set_str("errmsg", "ns not found");
            doc.set_i32("ok", 0);
        }
        Ok(create_reply(req.req_id, vec![doc], 0))
    }

    fn reply_drop_database(&mut self, req: &MsgQuery, db: &str) -> Result<Reply> {
        // TODO remove cursors?
        let deleted = try!(self.conn.drop_database(db));
        let mut doc = bson::Document::new_empty();
        if deleted {
            doc.set_i32("ok", 1);
        } else {
            doc.set_str("errmsg", "database not found");
            doc.set_i32("ok", 0);
        }
        Ok(create_reply(req.req_id, vec![doc], 0))
    }

    fn reply_list_collections(&mut self, req: &MsgQuery, db: &str) -> Result<Reply> {
        let results = try!(self.conn.list_collections());
        let seq = {
            // we need db to get captured by this closure which outlives
            // this function, so we create String from it and use a move
            // closure.

            let db = String::from(db);
            let results = results.into_iter().filter_map(
                move |c| {
                    if db.as_str() == c.db {
                        let mut doc = bson::Document::new_empty();
                        doc.set_string("name", c.coll);
                        doc.set_document("options", c.options);
                        let r = elmo::Row {
                            doc: bson::Value::BDocument(doc),
                        };
                        Some(Ok(r))
                    } else {
                        None
                    }
                }
                );
            results
        };

        // TODO filter in query?

        let default_batch_size = 100;
        let cursor_options = req.query.get("cursor");
        let ns = format!("{}.$cmd.listCollections", db);
        let doc = try!(self.reply_with_cursor(&ns, seq, cursor_options, default_batch_size));
        // note that this uses the newer way of returning a cursor ID, so we pass 0 below
        Ok(create_reply(req.req_id, vec![doc], 0))
    }

    fn reply_list_indexes(&mut self, req: &MsgQuery, db: &str) -> Result<Reply> {
        // TODO check coll
        let results = try!(self.conn.list_indexes());
        let seq = {
            // we need db to get captured by this closure which outlives
            // this function, so we create String from it and use a move
            // closure.

            let db = String::from(db);
            let results = results.into_iter().filter_map(
                move |ndx| {
                    if ndx.db.as_str() == db {
                        let mut doc = bson::Document::new_empty();
                        doc.set_string("ns", ndx.full_collection_name());
                        doc.set_string("name", ndx.name);
                        doc.set_document("key", ndx.spec);
                        // TODO it seems the automatic index on _id is NOT supposed to be marked unique
                        // TODO if unique && ndxInfo.ndx<>"_id_" then pairs.Add("unique", BBoolean unique)
                        let r = elmo::Row {
                            doc: bson::Value::BDocument(doc),
                        };
                        Some(Ok(r))
                    } else {
                        None
                    }
                }
                );
            results
        };

        // TODO filter in query?

        let default_batch_size = 100;
        let cursor_options = req.query.get("cursor");
        let ns = format!("{}.$cmd.listIndexes", db);
        let doc = try!(self.reply_with_cursor(&ns, seq, cursor_options, default_batch_size));
        // note that this uses the newer way of returning a cursor ID, so we pass 0 below
        Ok(create_reply(req.req_id, vec![doc], 0))
    }

    fn splitname(s: &str) -> Result<(&str, &str)> {
        match s.find('.') {
            None => Err(Error::Misc("bad namespace")),
            Some(dot) => Ok((&s[0 .. dot], &s[dot+1 ..]))
        }
    }

    fn try_get_optional_prefix<'a>(v: &'a bson::Document, k: &str) -> Option<&'a bson::Value> {
        assert_eq!(&k[0 .. 1], "$");
        match v.get(k) {
            Some(r) => Some(r),
            None => {
                match v.get(&k[1 ..]) {
                    Some(r) => Some(r),
                    None => None,
                }
            },
        }
    }

    fn try_remove_optional_prefix(v: &mut bson::Document, k: &str) -> Option<bson::Value> {
        assert_eq!(&k[0 .. 1], "$");
        match v.remove(k) {
            Some(r) => Some(r),
            None => {
                match v.remove(&k[1 ..]) {
                    Some(r) => Some(r),
                    None => None,
                }
            },
        }
    }

    fn reply_query(&mut self, req: MsgQuery, db: &str) -> Result<Reply> {
        let MsgQuery {
            req_id,
            flags,
            full_collection_name,
            number_to_skip,
            number_to_return,
            mut query,
            return_fields_selector,
        } = req;

        let (db, coll) = try!(Self::splitname(&full_collection_name));

        // This *might* just have the query in it.  OR it might have the 
        // query in a key called query, which might also be called $query,
        // along with other stuff (like orderby) as well.
        // This other stuff is called query modifiers.  
        // Sigh.

        let seq = 
            match Self::try_remove_optional_prefix(&mut query, "$query") {
                Some(q) => {
                    // TODO what if somebody queries on a field named query?  ambiguous.

                    let orderby = Self::try_remove_optional_prefix(&mut query, "$orderby");
                    let min = Self::try_remove_optional_prefix(&mut query, "$min");
                    let max = Self::try_remove_optional_prefix(&mut query, "$max");
                    let hint = Self::try_remove_optional_prefix(&mut query, "$hint");
                    let explain = Self::try_remove_optional_prefix(&mut query, "$explain");
                    let q = try!(q.into_document());
                    let seq = try!(self.conn.find(
                            db, 
                            coll, 
                            q,
                            orderby,
                            return_fields_selector,
                            min,
                            max,
                            hint,
                            explain
                            ));
                    seq
                },
                None => {
                    let seq = try!(self.conn.find(
                            db, 
                            coll, 
                            query,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None
                            ));
                    seq
                },
            };

        // TODO let s = crud.seqOnlyDoc s

        if number_to_skip < 0 {
            unimplemented!();
        }

        let seq = seq.skip(number_to_skip as usize);

        let mut seq = seq.map(
            |r| r.map_err(wrap_err)
        );

        //let docs = try!(Self::grab(&mut seq, number_to_return as usize));
        //Ok(create_reply(req_id, docs, 0))

        let (docs, more) = try!(Self::do_limit(&full_collection_name, &mut seq, number_to_return));
        let cursor_id = if more {
            self.store_cursor(&full_collection_name, seq)
            //0
        } else {
            0
        };
        let docs = vec_rows_to_values(docs);
        let docs = try!(vec_values_to_docs(docs));
        Ok(create_reply(req_id, docs, cursor_id))
    }

    fn reply_cmd(&mut self, req: MsgQuery, db: &str) -> Result<Reply> {
        if req.query.pairs.is_empty() {
            Err(Error::Misc("empty query"))
        } else {
            // this code assumes that the first key is always the command
            let cmd = req.query.pairs[0].0.clone();
            // TODO let cmd = cmd.ToLower();
            let res =
                // TODO isMaster needs to be in here?
                match cmd.as_str() {
                    //"explain" => reply_explain req db
                    //"aggregate" => reply_aggregate req db
                    "insert" => self.reply_insert(req, db),
                    "delete" => self.reply_delete(&req, db),
                    //"distinct" => reply_distinct req db
                    //"update" => reply_Update req db
                    //"findandmodify" => reply_FindAndModify req db
                    //"count" => reply_Count req db
                    //"validate" => reply_Validate req db
                    "createindexes" => self.reply_create_indexes(&req, db),
                    "deleteindexes" => self.reply_delete_indexes(&req, db),
                    "drop" => self.reply_drop_collection(&req, db),
                    "dropdatabase" => self.reply_drop_database(&req, db),
                    "listcollections" => self.reply_list_collections(&req, db),
                    "listindexes" => self.reply_list_indexes(&req, db),
                    "create" => self.reply_create_collection(&req, db),
                    //"features" => reply_features &req db
                    _ => Err(Error::Misc("unknown cmd"))
                };
            res
        }
    }

    fn reply_2004(&mut self, req: MsgQuery) -> Reply {
        // reallocating the strings here so we can pass ownership of req down the line.
        // TODO we could deconstruct req now?
        let parts = req.full_collection_name.split('.').map(|s| String::from(s)).collect::<Vec<_>>();
        let req_id = req.req_id;
        let r = 
            if parts.len() < 2 {
                // TODO failwith (sprintf "bad collection name: %s" (req.full_collection_name))
                Err(Error::Misc("bad collection name"))
            } else {
                let db = &parts[0];
                if db == "admin" {
                    if parts[1] == "$cmd" {
                        //reply_AdminCmd req
                        // TODO probably want to pass ownership of req down here
                        self.reply_admin_cmd(&req, db)
                    } else {
                        //failwith "not implemented"
                        Err(Error::Misc("TODO"))
                    }
                } else {
                    if parts[1] == "$cmd" {
                        if parts.len() == 4 && parts[2]=="sys" && parts[3]=="inprog" {
                            //reply_cmd_sys_inprog req db
                            Err(Error::Misc("TODO"))
                        } else {
                            self.reply_cmd(req, db)
                        }
                    } else if parts.len()==3 && parts[1]=="system" && parts[2]=="indexes" {
                        //reply_system_indexes req db
                        Err(Error::Misc("TODO"))
                    } else if parts.len()==3 && parts[1]=="system" && parts[2]=="namespaces" {
                        //reply_system_namespaces req db
                        Err(Error::Misc("TODO"))
                    } else {
                        self.reply_query(req, db)
                    }
                }
            };
        println!("reply: {:?}", r);
        match r {
            Ok(r) => r,
            Err(e) => reply_err(req_id, e),
        }
    }

    fn reply_2005(&mut self, req: MsgGetMore) -> Reply {
        match self.cursors.remove(&req.cursor_id) {
            Some((ns, mut seq)) => {
                match Self::do_limit(&ns, &mut seq, req.number_to_return) {
                    Ok((docs, more)) => {
                        if more {
                            // put the cursor back for next time
                            self.cursors.insert(req.cursor_id, (ns, box seq));
                        }
                        let docs = vec_rows_to_values(docs);
                        match vec_values_to_docs(docs) {
                            Ok(docs) => {
                                create_reply(req.req_id, docs, 0)
                            },
                            Err(e) => {
                                reply_err(req.req_id, Error::Misc("TODO"))
                            },
                        }
                    },
                    Err(e) => {
                        reply_err(req.req_id, Error::Misc("TODO"))
                    },
                }
            },
            None => {
                reply_err(req.req_id, Error::Misc("TODO"))
            },
        }
    }

    fn handle_one_message(&mut self, stream: &mut std::net::TcpStream) -> Result<()> {
        fn send_reply(stream: &mut std::net::TcpStream, resp: Reply) -> Result<()> {
            //println!("resp: {:?}", resp);
            let ba = resp.encode();
            //println!("ba: {:?}", ba);
            let wrote = try!(misc::io::write_fully(stream, &ba));
            if wrote != ba.len() {
                return Err(Error::Misc("network write failed"));
            } else {
                Ok(())
            }
        }

        let ba = try!(read_message_bytes(stream));
        match ba {
            None => Ok(()),
            Some(ba) => {
                //println!("{:?}", ba);
                let msg = try!(parse_request(&ba));
                println!("request: {:?}", msg);
                match msg {
                    Request::KillCursors(req) => {
                        for cursor_id in req.cursor_ids {
                            self.cursors.remove(&cursor_id);
                        }
                        // there is no reply to this
                        Ok(())
                    },
                    Request::Query(req) => {
                        let resp = self.reply_2004(req);
                        send_reply(stream, resp)
                    },
                    Request::GetMore(req) => {
                        let resp = self.reply_2005(req);
                        send_reply(stream, resp)
                    },
                }
            }
        }
    }

    fn handle_client(&mut self, mut stream: std::net::TcpStream) -> Result<()> {
        loop {
            let r = self.handle_one_message(&mut stream);
            if r.is_err() {
                // TODO if this is just plain end of file, no need to error.
                return r;
            }
        }
    }

}

// TODO args:  filename, ipaddr, port
pub fn serve() {
    let listener = std::net::TcpListener::bind("127.0.0.1:27017").unwrap();

    // accept connections and process them, spawning a new thread for each one
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                std::thread::spawn(move|| {
                    // connection succeeded
                    // TODO how to use filename arg.  lifetime problem.
                    let conn = elmo_sqlite3::connect("foo.db").expect("TODO");
                    let conn = elmo::Connection::new(conn);
                    let mut s = Server {
                        conn: conn,
                        cursors: std::collections::HashMap::new(),
                        cursor_num: 0,
                    };
                    s.handle_client(stream).expect("TODO");
                });
            }
            Err(e) => { /* connection failed */ }
        }
    }

    // close the socket server
    drop(listener);
}

pub fn main() {
    serve();
}

