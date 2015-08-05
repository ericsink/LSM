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
use bson::BsonValue;

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
    docs : Vec<BsonValue>,
}

#[derive(Debug)]
// TODO consider calling this Msg2004
struct MsgQuery {
    req_id : i32,
    flags : i32,
    full_collection_name : String,
    number_to_skip : i32,
    number_to_return : i32,
    query : BsonValue,
    return_fields_selector : Option<BsonValue>,
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

fn create_reply(req_id: i32, docs: Vec<BsonValue>, cursor_id: i64) -> Reply {
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
    let mut doc = BsonValue::BDocument(vec![]);
    // TODO stack trace was nice here
    //pairs.push(("errmsg", BString("exception: " + errmsg)));
    //pairs.push(("code", BInt32(code)));
    doc.add_pair_i32("ok", 0);
    create_reply(req_id, vec![doc], 0)
}

// TODO mongo has a way of automatically killing a cursor after 10 minutes idle

struct Server {
    conn: elmo::Connection,
    cursor_num: i64,
    cursors: std::collections::HashMap<i64, (String, Box<Iterator<Item=BsonValue>>)>,
}

impl Server {

    fn reply_whatsmyuri(&self, req: &MsgQuery) -> Result<Reply> {
        let mut doc = BsonValue::BDocument(vec![]);
        doc.add_pair_str("you", "127.0.0.1:65460");
        doc.add_pair_i32("ok", 1);
        Ok(create_reply(req.req_id, vec![doc], 0))
    }

    fn reply_getlog(&self, req: &MsgQuery) -> Result<Reply> {
        let mut doc = BsonValue::BDocument(vec![]);
        doc.add_pair_i32("totalLinesWritten", 1);
        doc.add_pair("log", BsonValue::BArray(vec![]));
        doc.add_pair_i32("ok", 1);
        Ok(create_reply(req.req_id, vec![doc], 0))
    }

    fn reply_replsetgetstatus(&self, req: &MsgQuery) -> Result<Reply> {
        let mut mine = BsonValue::BDocument(vec![]);
        mine.add_pair_i32("_id", 0);
        mine.add_pair_str("name", "whatever");
        mine.add_pair_i32("state", 1);
        mine.add_pair_f64("health", 1.0);
        mine.add_pair_str("stateStr", "PRIMARY");
        mine.add_pair_i32("uptime", 0);
        mine.add_pair_timestamp("optime", 0);
        mine.add_pair_datetime("optimeDate", 0);
        mine.add_pair_timestamp("electionTime", 0);
        mine.add_pair_timestamp("electionDate", 0);
        mine.add_pair_bool("self", true);

        let mut doc = BsonValue::BDocument(vec![]);
        doc.add_pair("mine", mine);
        doc.add_pair_str("set", "TODO");
        doc.add_pair_datetime("date", 0);
        doc.add_pair_i32("myState", 1);
        doc.add_pair_i32("ok", 1);

        Ok(create_reply(req.req_id, vec![doc], 0))
    }

    fn reply_ismaster(&self, req: &MsgQuery) -> Result<Reply> {
        let mut doc = BsonValue::BDocument(vec![]);
        doc.add_pair_bool("ismaster", true);
        doc.add_pair_bool("secondary", false);
        doc.add_pair_i32("maxWireVersion", 3);
        doc.add_pair_i32("minWireVersion", 2);
        // ver >= 2:  we don't support the older fire-and-forget write operations. 
        // ver >= 3:  we don't support the older form of explain
        // TODO if we set minWireVersion to 3, which is what we want to do, so
        // that we can tell the client that we don't support the older form of
        // explain, what happens is that we start getting the old fire-and-forget
        // write operations instead of the write commands that we want.
        doc.add_pair_i32("ok", 1);
        Ok(create_reply(req.req_id, vec![doc], 0))
    }

    fn reply_admin_cmd(&self, req: &MsgQuery, db: &str) -> Result<Reply> {
        match req.query {
            BsonValue::BDocument(ref pairs) => {
                if pairs.is_empty() {
                    Err(Error::Misc("empty query"))
                } else {
                    // this code assumes that the first key is always the command
                    let cmd = pairs[0].0.as_str();
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
            },
            _ => Err(Error::Misc("query must be a document")),
        }
    }

    fn reply_insert(&self, req: &MsgQuery, db: &str) -> Result<Reply> {
        let q = &req.query;
        let coll = try!(try!(q.getValueForKey("insert")).getString());
        let docs = try!(try!(q.getValueForKey("documents")).getArray());
        // TODO ordered
        let results = try!(self.conn.insert(db, coll, &docs));
        let mut errors = Vec::new();
        for i in 0 .. results.len() {
            if results[i].is_err() {
                let msg = format!("{:?}", results[i]);
                let err = BsonValue::BDocument(vec![(String::from("index"), BsonValue::BInt32(i as i32)), (String::from("errmsg"), BsonValue::BString(msg))]);
                errors.push(err);
            }
        }
        let mut doc = BsonValue::BDocument(vec![]);
        doc.add_pair_i32("n", ((results.len() - errors.len()) as i32));
        if errors.len() > 0 {
            doc.add_pair("writeErrors", BsonValue::BArray(errors));
        }
        doc.add_pair_i32("ok", 1);
        Ok(create_reply(req.req_id, vec![doc], 0))
    }

    fn store_cursor<T: Iterator<Item=BsonValue> + 'static>(&mut self, ns: &str, seq: T) -> i64 {
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
    fn grab<T: Iterator<Item=BsonValue>>(seq: &mut T, n: usize) -> Vec<BsonValue> {
        let mut r = Vec::new();
        for _ in 0 .. n {
            match seq.next() {
                None => {
                    break;
                },
                Some(v) => {
                    r.push(v);
                },
            }
        }
        r
    }

    // this is the older way of returning a cursor.
    fn do_limit<T: Iterator<Item=BsonValue> + 'static>(&mut self, ns: &str, mut seq: T, number_to_return: i32) -> (Vec<BsonValue>, Option<i64>) {
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
            let docs = seq.take(n as usize).collect::<Vec<_>>();
            (docs, None)
        } else if number_to_return == 0 {
            // return whatever the default size is
            // TODO for now, just return them all and close the cursor
            let docs = seq.collect::<Vec<_>>();
            (docs, None)
        } else {
            // soft limit.  keep cursor open.
            let docs = Self::grab(&mut seq, number_to_return as usize);
            if docs.len() > 0 {
                let cursor_id = self.store_cursor(ns, seq);
                (docs, Some(cursor_id))
            } else {
                (docs, None)
            }
        }
    }

    // this is a newer way of returning a cursor.  used by the agg framework.
    fn reply_with_cursor<T: Iterator<Item=BsonValue> + 'static>(&mut self, ns: &str, mut seq: T, cursor_options: Option<&BsonValue>, default_batch_size: usize) -> Result<BsonValue> {
        let number_to_return =
            match cursor_options {
                Some(&BsonValue::BDocument(ref pairs)) => {
                    if pairs.iter().any(|&(ref k, _)| k != "batchSize") {
                        return Err(Error::Misc("invalid cursor option"));
                    }
                    match pairs.iter().find(|&&(ref k, ref _v)| k == "batchSize") {
                        Some(&(_, BsonValue::BInt32(n))) => {
                            if n < 0 {
                                return Err(Error::Misc("batchSize < 0"));
                            }
                            Some(n as usize)
                        },
                        Some(&(_, BsonValue::BDouble(n))) => {
                            if n < 0.0 {
                                return Err(Error::Misc("batchSize < 0"));
                            }
                            Some(n as usize)
                        },
                        Some(&(_, BsonValue::BInt64(n))) => {
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
                    let docs = seq.collect::<Vec<_>>();
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
                    let docs = Self::grab(&mut seq, n);
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


        let mut doc = BsonValue::BDocument(vec![]);
        match cursor_id {
            Some(cursor_id) => {
                let mut cursor = BsonValue::BDocument(vec![]);
                cursor.add_pair_i64("id", cursor_id);
                cursor.add_pair_str("ns", ns);
                cursor.add_pair_array("firstBatch", docs);
            },
            None => {
                doc.add_pair_array("result", docs);
            },
        }
        doc.add_pair_i32("ok", 1);
        Ok(doc)
    }

    fn reply_drop_collection(&mut self, req: &MsgQuery, db: &str) -> Result<Reply> {
        let coll = try!(try!(req.query.getValueForKey("insert")).getString());
        let full_coll = format!("{}.{}", db, coll);
        self.remove_cursors_for_collection(&full_coll);
        let deleted = try!(self.conn.drop_collection(db, coll));
        let mut doc = BsonValue::BDocument(vec![]);
        if deleted {
            doc.add_pair_i32("ok", 1);
        } else {
            // mongo shell apparently cares about this exact error message string
            doc.add_pair_str("errmsg", "ns not found");
            doc.add_pair_i32("ok", 0);
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
                move |(c_db, c_coll, c_options)| {
                    if db.as_str() == c_db {
                        let mut doc = BsonValue::BDocument(vec![]);
                        doc.add_pair_string("name", c_coll);
                        doc.add_pair("options", c_options);
                        Some(doc)
                    } else {
                        None
                    }
                }
                );
            results
        };

        // TODO filter in query?

        let default_batch_size = 100;
        let cursor_options = req.query.tryGetValueForKey("cursor");
        let ns = format!("{}.$cmd.listCollections", db);
        let doc = try!(self.reply_with_cursor(&ns, seq, cursor_options, default_batch_size));
        // note that this uses the newer way of returning a cursor ID, so we pass 0 below
        Ok(create_reply(req.req_id, vec![doc], 0))
    }

    fn reply_cmd(&mut self, req: &MsgQuery, db: &str) -> Result<Reply> {
        match req.query {
            BsonValue::BDocument(ref pairs) => {
                if pairs.is_empty() {
                    Err(Error::Misc("empty query"))
                } else {
                    // this code assumes that the first key is always the command
                    let cmd = pairs[0].0.as_str();
                    // TODO let cmd = cmd.ToLower();
                    let res =
                        // TODO isMaster needs to be in here?
                        match cmd {
                            //"explain" => reply_explain req db
                            //"aggregate" => reply_aggregate req db
                            "insert" => self.reply_insert(req, db),
                            //"delete" => reply_Delete req db
                            //"distinct" => reply_distinct req db
                            //"update" => reply_Update req db
                            //"findandmodify" => reply_FindAndModify req db
                            //"count" => reply_Count req db
                            //"validate" => reply_Validate req db
                            //"createindexes" => reply_createIndexes req db
                            //"deleteindexes" => reply_deleteIndexes req db
                            "drop" => self.reply_drop_collection(req, db),
                            //"dropdatabase" => reply_DropDatabase req db
                            "listcollections" => self.reply_list_collections(req, db),
                            //"listindexes" => reply_listIndexes req db
                            //"create" => reply_CreateCollection req db
                            //"features" => reply_features req db
                            _ => Err(Error::Misc("unknown cmd"))
                        };
                    res
                }
            },
            _ => Err(Error::Misc("query must be a document")),
        }
    }

    fn reply_2004(&mut self, req: MsgQuery) -> Reply {
        let pair = req.full_collection_name.split('.').collect::<Vec<_>>();
        let r = 
            if pair.len() < 2 {
                // TODO failwith (sprintf "bad collection name: %s" (req.full_collection_name))
                Err(Error::Misc("bad collection name"))
            } else {
                let db = pair[0];
                let coll = pair[1];
                if db == "admin" {
                    if coll == "$cmd" {
                        //reply_AdminCmd req
                        self.reply_admin_cmd(&req, db)
                    } else {
                        //failwith "not implemented"
                        Err(Error::Misc("TODO"))
                    }
                } else {
                    if coll == "$cmd" {
                        if pair.len() == 4 && pair[2]=="sys" && pair[3]=="inprog" {
                            //reply_cmd_sys_inprog req db
                            Err(Error::Misc("TODO"))
                        } else {
                            self.reply_cmd(&req, db)
                        }
                    } else if pair.len()==3 && pair[1]=="system" && pair[2]=="indexes" {
                        //reply_system_indexes req db
                        Err(Error::Misc("TODO"))
                    } else if pair.len()==3 && pair[1]=="system" && pair[2]=="namespaces" {
                        //reply_system_namespaces req db
                        Err(Error::Misc("TODO"))
                    } else {
                        //reply_Query req
                        Err(Error::Misc("TODO"))
                    }
                }
            };
        println!("reply: {:?}", r);
        match r {
            Ok(r) => r,
            Err(e) => reply_err(req.req_id, e),
        }
    }

    fn reply_2005(&mut self, req: MsgGetMore) -> Reply {
        match self.cursors.remove(&req.cursor_id) {
            Some((ns,seq)) => {
                let (docs, cursor_id) = self.do_limit(&ns, seq, req.number_to_return);
                match cursor_id {
                    Some(cursor_id) => create_reply(req.req_id, docs, cursor_id),
                    None => create_reply(req.req_id, docs, 0),
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

