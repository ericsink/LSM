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

#![feature(box_syntax)]
#![feature(convert)]
#![feature(associated_consts)]
#![feature(vec_push_all)]
#![feature(result_expect)]

// TODO turn the following warnings back on later
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

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

    Bson(bson::Error),
    Io(std::io::Error),
    Utf8(std::str::Utf8Error),
    Whatever(Box<std::error::Error>),
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

// TODO why is 'static needed here?  Doesn't this take ownership?
fn wrap_err<E: std::error::Error + 'static>(err: E) -> Error {
    Error::Whatever(box err)
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
struct BsonMsgReply {
    r_requestID : i32,
    r_responseTo : i32,
    r_responseFlags : i32,
    r_cursorID : i64,
    r_startingFrom : i32,
    r_documents : Vec<BsonValue>,
}

#[derive(Debug)]
struct BsonMsgQuery {
    q_requestID : i32,
    q_flags : i32,
    q_fullCollectionName : String,
    q_numberToSkip : i32,
    q_numberToReturn : i32,
    q_query : BsonValue,
    q_returnFieldsSelector : Option<BsonValue>,
}

#[derive(Debug)]
struct BsonMsgGetMore {
    m_requestID : i32,
    m_fullCollectionName : String,
    m_numberToReturn : i32,
    m_cursorID : i64,
}

#[derive(Debug)]
struct BsonMsgKillCursors {
    k_requestID : i32,
    k_cursorIDs : Vec<i64>,
}

#[derive(Debug)]
enum BsonClientMsg {
    BsonMsgQuery(BsonMsgQuery),
    BsonMsgGetMore(BsonMsgGetMore),
    BsonMsgKillCursors(BsonMsgKillCursors),
}

impl BsonMsgReply {
    fn encodeReply(&self) -> Box<[u8]> {
        let mut w = Vec::new();
        // length placeholder
        w.push_all(&[0u8; 4]);
        w.push_all(&endian::i32_to_bytes_le(self.r_requestID));
        w.push_all(&endian::i32_to_bytes_le(self.r_responseTo));
        w.push_all(&endian::u32_to_bytes_le(1u32)); 
        w.push_all(&endian::i32_to_bytes_le(self.r_responseFlags));
        w.push_all(&endian::i64_to_bytes_le(self.r_cursorID));
        w.push_all(&endian::i32_to_bytes_le(self.r_startingFrom));
        w.push_all(&endian::u32_to_bytes_le(self.r_documents.len() as u32));
        for doc in &self.r_documents {
            doc.to_bson(&mut w);
        }
        misc::bytes::copy_into(&endian::u32_to_bytes_le(w.len() as u32), &mut w[0 .. 4]);
        w.into_boxed_slice()
    }
}

fn parseMessageFromClient(ba: &[u8]) -> Result<BsonClientMsg> {
    let mut i = 0;
    let (messageLength,requestID,responseTo,opCode) = slurp_header(ba, &mut i);
    match opCode {
        2004 => {
            let flags = bufndx::slurp_i32_le(ba, &mut i);
            let fullCollectionName = try!(bufndx::slurp_cstring(ba, &mut i));
            let numberToSkip = bufndx::slurp_i32_le(ba, &mut i);
            let numberToReturn = bufndx::slurp_i32_le(ba, &mut i);
            let query = try!(bson::slurp_document(ba, &mut i));
            let returnFieldsSelector = if i < ba.len() { Some(try!(bson::slurp_document(ba, &mut i))) } else { None };

            let msg = BsonMsgQuery {
                q_requestID: requestID,
                q_flags: flags,
                q_fullCollectionName: fullCollectionName,
                q_numberToSkip: numberToSkip,
                q_numberToReturn: numberToReturn,
                q_query: query,
                q_returnFieldsSelector: returnFieldsSelector,
            };
            Ok(BsonClientMsg::BsonMsgQuery(msg))
        },

        2005 => {
            let flags = bufndx::slurp_i32_le(ba, &mut i);
            let fullCollectionName = try!(bufndx::slurp_cstring(ba, &mut i));
            let numberToReturn = bufndx::slurp_i32_le(ba, &mut i);
            let cursorID = bufndx::slurp_i64_le(ba, &mut i);

            let msg = BsonMsgGetMore {
                m_requestID: requestID,
                m_fullCollectionName: fullCollectionName,
                m_numberToReturn: numberToReturn,
                m_cursorID: cursorID,
            };
            Ok(BsonClientMsg::BsonMsgGetMore(msg))
        },

        2007 => {
            let flags = bufndx::slurp_i32_le(ba, &mut i);
            let numberOfCursorIDs = bufndx::slurp_i32_le(ba, &mut i);
            let mut cursorIDs = Vec::new();
            for _ in 0 .. numberOfCursorIDs {
                cursorIDs.push(bufndx::slurp_i64_le(ba, &mut i));
            }

            let msg = BsonMsgKillCursors {
                k_requestID: requestID,
                k_cursorIDs: cursorIDs,
            };
            Ok(BsonClientMsg::BsonMsgKillCursors(msg))
        },

        _ => {
            Err(Error::CorruptFile("unknown message opcode TODO"))
        },
    }
}

// TODO do these really need to be signed?
fn slurp_header(ba: &[u8], i: &mut usize) -> (i32,i32,i32,i32) {
    let messageLength = bufndx::slurp_i32_le(ba, i);
    let requestID = bufndx::slurp_i32_le(ba, i);
    let responseTo = bufndx::slurp_i32_le(ba, i);
    let opCode = bufndx::slurp_i32_le(ba, i);
    let v = (messageLength,requestID,responseTo,opCode);
    v
}

fn read_message(stream: &mut Read) -> Result<Option<Box<[u8]>>> {
    let mut a = [0; 4];
    let got = try!(misc::io::read_fully(stream, &mut a));
    if got == 0 {
        return Ok(None);
    }
    let messageLength = endian::u32_from_bytes_le(a) as usize;
    let mut msg = vec![0; messageLength]; 
    misc::bytes::copy_into(&a, &mut msg[0 .. 4]);
    let got = try!(misc::io::read_fully(stream, &mut msg[4 .. messageLength]));
    if got != messageLength - 4 {
        return Err(Error::CorruptFile("end of file at the wrong time"));
    }
    Ok(Some(msg.into_boxed_slice()))
}

fn create_reply(reqID: i32, docs: Vec<BsonValue>, crsrID: i64) -> BsonMsgReply {
    let msg = BsonMsgReply {
        r_requestID: 0,
        r_responseTo: reqID,
        r_responseFlags: 0,
        r_cursorID: crsrID,
        r_startingFrom: 0,
        // TODO
        r_documents: docs,
    };
    msg
}

fn reply_err(requestID: i32, err: Error) -> BsonMsgReply {
    let mut pairs = Vec::new();
    // TODO stack trace was nice here
    //pairs.push(("errmsg", BString("exception: " + errmsg)));
    //pairs.push(("code", BInt32(code)));
    pairs.push((String::from("ok"), BsonValue::BInt32(0)));
    let doc = BsonValue::BDocument(pairs);
    create_reply(requestID, vec![doc], 0)
}

struct Server {
    conn: elmo::Connection,
}

impl Server {

    fn reply_whatsmyuri(&self, clientMsg: &BsonMsgQuery) -> Result<BsonMsgReply> {
        let mut doc = BsonValue::BDocument(vec![]);
        doc.add_pair_str("you", "127.0.0.1:65460");
        doc.add_pair_i32("ok", 1);
        Ok(create_reply(clientMsg.q_requestID, vec![doc], 0))
    }

    fn reply_getlog(&self, clientMsg: &BsonMsgQuery) -> Result<BsonMsgReply> {
        let mut doc = BsonValue::BDocument(vec![]);
        doc.add_pair_i32("totalLinesWritten", 1);
        doc.add_pair("log", BsonValue::BArray(vec![]));
        doc.add_pair_i32("ok", 1);
        Ok(create_reply(clientMsg.q_requestID, vec![doc], 0))
    }

    fn reply_replsetgetstatus(&self, clientMsg: &BsonMsgQuery) -> Result<BsonMsgReply> {
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

        Ok(create_reply(clientMsg.q_requestID, vec![doc], 0))
    }

    fn reply_ismaster(&self, clientMsg: &BsonMsgQuery) -> Result<BsonMsgReply> {
        let mut pairs = Vec::new();
        // TODO
        pairs.push((String::from("ismaster"), BsonValue::BBoolean(true)));
        pairs.push((String::from("secondary"), BsonValue::BBoolean(false)));
        pairs.push((String::from("maxWireVersion"), BsonValue::BInt32(3)));
        pairs.push((String::from("ok"), BsonValue::BDouble(1.0)));
        let doc = BsonValue::BDocument(pairs);
        Ok(create_reply(clientMsg.q_requestID, vec![doc], 0))
    }

    fn reply_admin_cmd(&self, clientMsg: &BsonMsgQuery, db: &str) -> Result<BsonMsgReply> {
        match clientMsg.q_query {
            BsonValue::BDocument(ref pairs) => {
                if pairs.is_empty() {
                    Err(Error::Misc("empty query"))
                } else {
                    // this code assumes that the first key is always the command
                    let cmd = pairs[0].0.as_str();
                    // TODO let cmd = cmd.ToLower();
                    let res =
                        match cmd {
                            "whatsmyuri" => self.reply_whatsmyuri(clientMsg),
                            "getLog" => self.reply_getlog(clientMsg),
                            "replSetGetStatus" => self.reply_replsetgetstatus(clientMsg),
                            "isMaster" => self.reply_ismaster(clientMsg),
                            _ => Err(Error::Misc("unknown cmd"))
                        };
                    res
                }
            },
            _ => Err(Error::Misc("query must be a document")),
        }
    }

    fn reply_insert(&self, clientMsg: &BsonMsgQuery, db: &str) -> Result<BsonMsgReply> {
        let q = &clientMsg.q_query;
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
        let mut pairs = Vec::new();
        pairs.push((String::from("n"), BsonValue::BInt32((results.len() - errors.len()) as i32)));
        //pairs.Add("lastOp", BTimeStamp 0L) // TODO
        if errors.len() > 0 {
            pairs.push((String::from("writeErrors"), BsonValue::BArray(errors)));
        }
        // TODO electionId?
        pairs.push((String::from("ok"), BsonValue::BDouble(1.0)));
        let doc = BsonValue::BDocument(pairs);
        Ok(create_reply(clientMsg.q_requestID, vec![doc], 0))
    }

    // TODO this layer of the code should not be using Error

    fn reply_cmd(&self, clientMsg: &BsonMsgQuery, db: &str) -> Result<BsonMsgReply> {
        match clientMsg.q_query {
            BsonValue::BDocument(ref pairs) => {
                if pairs.is_empty() {
                    Err(Error::Misc("empty query"))
                } else {
                    // this code assumes that the first key is always the command
                    let cmd = pairs[0].0.as_str();
                    // TODO let cmd = cmd.ToLower();
                    let res =
                        match cmd {
                            //"explain" => reply_explain clientMsg db
                            //"aggregate" => reply_aggregate clientMsg db
                            "insert" => self.reply_insert(clientMsg, db),
                            //"delete" => reply_Delete clientMsg db
                            //"distinct" => reply_distinct clientMsg db
                            //"update" => reply_Update clientMsg db
                            //"findandmodify" => reply_FindAndModify clientMsg db
                            //"count" => reply_Count clientMsg db
                            //"validate" => reply_Validate clientMsg db
                            //"createindexes" => reply_createIndexes clientMsg db
                            //"deleteindexes" => reply_deleteIndexes clientMsg db
                            //"drop" => reply_DropCollection clientMsg db
                            //"dropdatabase" => reply_DropDatabase clientMsg db
                            //"listcollections" => reply_listCollections clientMsg db
                            //"listindexes" => reply_listIndexes clientMsg db
                            //"create" => reply_CreateCollection clientMsg db
                            //"features" => reply_features clientMsg db
                            _ => Err(Error::Misc("unknown cmd"))
                        };
                    res
                }
            },
            _ => Err(Error::Misc("query must be a document")),
        }
    }

    fn reply2004(&self, qm: BsonMsgQuery) -> BsonMsgReply {
        let pair = qm.q_fullCollectionName.split('.').collect::<Vec<_>>();
        let r = 
            if pair.len() < 2 {
                // TODO failwith (sprintf "bad collection name: %s" (clientMsg.q_fullCollectionName))
                Err(Error::Misc("bad collection name"))
            } else {
                let db = pair[0];
                let coll = pair[1];
                if db == "admin" {
                    if coll == "$cmd" {
                        //reply_AdminCmd clientMsg
                        self.reply_admin_cmd(&qm, db)
                    } else {
                        //failwith "not implemented"
                        Err(Error::Misc("TODO"))
                    }
                } else {
                    if coll == "$cmd" {
                        if pair.len() == 4 && pair[2]=="sys" && pair[3]=="inprog" {
                            //reply_cmd_sys_inprog clientMsg db
                        Err(Error::Misc("TODO"))
                        } else {
                            self.reply_cmd(&qm, db)
                        }
                    } else if pair.len()==3 && pair[1]=="system" && pair[2]=="indexes" {
                        //reply_system_indexes clientMsg db
                        Err(Error::Misc("TODO"))
                    } else if pair.len()==3 && pair[1]=="system" && pair[2]=="namespaces" {
                        //reply_system_namespaces clientMsg db
                        Err(Error::Misc("TODO"))
                    } else {
                        //reply_Query clientMsg
                        Err(Error::Misc("TODO"))
                    }
                }
            };
        println!("reply: {:?}", r);
        match r {
            Ok(r) => r,
            Err(e) => reply_err(qm.q_requestID, e),
        }
    }

    fn handle_one_message(&self, stream: &mut std::net::TcpStream) -> Result<()> {
        let ba = try!(read_message(stream));
        match ba {
            None => Ok(()),
            Some(ba) => {
                //println!("{:?}", ba);
                let msg = try!(parseMessageFromClient(&ba));
                println!("request: {:?}", msg);
                match msg {
                    BsonClientMsg::BsonMsgKillCursors(km) => {
                    },
                    BsonClientMsg::BsonMsgQuery(qm) => {
                        let resp = self.reply2004(qm);
                        //println!("resp: {:?}", resp);
                        let ba = resp.encodeReply();
                        //println!("ba: {:?}", ba);
                        stream.write(&ba);
                    },
                    BsonClientMsg::BsonMsgGetMore(gm) => {
                    },
                }
                Ok(())
            }
        }
    }

    fn handle_client(&self, mut stream: std::net::TcpStream) -> Result<()> {
        loop {
            let r = self.handle_one_message(&mut stream);
            if r.is_err() {
                // TODO if this is just plain end of file, no need to error.
                return r;
            }
        }
    }

}

pub fn serve() {
    let listener = std::net::TcpListener::bind("127.0.0.1:27017").unwrap();

    // accept connections and process them, spawning a new thread for each one
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                std::thread::spawn(move|| {
                    // connection succeeded
                    let conn = elmo_sqlite3::connect("foo.db").expect("TODO");
                    let conn = elmo::Connection::new(conn);
                    let s = Server {
                        conn: conn,
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

