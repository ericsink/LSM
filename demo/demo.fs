(*
    Copyright 2014-2015 Zumero, LLC
    All Rights Reserved.

    This file is not open source.  

    I haven't decided what to do with this code yet.
*)

namespace Zumero

open System
open System.IO
open FSharp.Data
open Zumero.LSM

open ubjson

module fj =
    type PathElement =
        | Key of string
        | Index of int

    let to_utf8 (s:string) =
        System.Text.Encoding.UTF8.GetBytes (s)

    let from_utf8 (ba:byte[]) =
        System.Text.Encoding.UTF8.GetString (ba, 0, ba.Length)

    let gid() = 
        let g = "_" + Guid.NewGuid().ToString()
        let g = g.Replace ("{", "")
        let g = g.Replace ("}", "")
        let g = g.Replace ("-", "")
        g

    let rec flatten fn path jv =
        match jv with
        | ubJsonValue.Record a -> 
            for (k,v) in a do
                let newpath = (PathElement.Key k) :: path
                flatten fn newpath v
        | ubJsonValue.Array a -> 
            for i in 0 .. a.Length-1 do
                let newpath = (PathElement.Index i) :: path
                let v = a.[i]
                flatten fn newpath v
        | _ -> fn path jv
            
    let encodeJsonValue jv =
        match jv with
        | ubJsonValue.Boolean b -> 
            "b" + if b then "1" else "0"
        | ubJsonValue.Float f -> 
            // TODO should have index policy to specify how this should be indexed. float/decimal/integer.
            // TODO check to see if this is an integer?
            // TODO are we sure that JsonValue will only return float when decimal was not possible?
            "f" + f.ToString() // TODO how to deal with this?
        | ubJsonValue.Integer i -> 
            // TODO should have index policy to specify how this should be indexed. float/decimal/integer.
            // TODO check to see if this is an integer?
            // TODO are we sure that JsonValue will only return float when decimal was not possible?
            "i" + i.ToString() // TODO how to deal with this?
        | ubJsonValue.Null -> 
            "n"
        | ubJsonValue.Decimal n -> 
            // TODO consider just putting the decimal number in as binary, if we can figure out
            // how to order the bits so it sorts properly

            // TODO should have index policy to specify how this should be indexed. float/decimal/integer.
            "d" + n.ToString()
        | ubJsonValue.String s -> 
            "s" + s
        | _ -> failwith "no record or array here.  should have been flattened"

    let encode collId path jv rid =
        // TODO the only safe delimiter is a 0, building this as bytes, not as a string

        // or escape things.  ugly.

        // or just store an index number which is referenced elsewhere.  more efficient?
        // but prefix key compression should help a lot.

        let fldr cur acc =
            let s = match cur with
                    | PathElement.Key k ->
                        "r" + k
                    | PathElement.Index i ->
                        "a" + i.ToString()
            if acc = "" then s else acc + ":" + s

        let pathString = List.foldBack fldr path ""
        let vs = encodeJsonValue jv

        let s = sprintf "x:%s:%s:%s:%s" collId pathString vs rid
        //printfn "%s" s
        s |> to_utf8

    // TODO this function could move into LSM
    let query_key_range (db:IDatabase) (k1:byte[]) (k2:byte[]) = 
        seq {
            use csr = db.OpenCursor()
            csr.Seek(k1, SeekOp.SEEK_GE)
            // we assume the keys are like index encoded.  first the
            // actual key followed by the record number.  so if we are
            // querying on the actual key, we'll never find an exact match.
            // the matches will all be GT the query key.  and k2 must be
            // constructed such that all the matches we want are strictly LT.
            while csr.IsValid() && csr.KeyCompare(k2)<0 do 
                yield csr.Key()
                csr.Next()
            }

    let rec add_one (a:byte[]) ndx =
        let b = a.[ndx]
        if b < 255uy then
            a.[ndx] <- b + 1uy
        else
            a.[ndx] <- 0uy
            add_one a (ndx-1)

    // TODO this function could move into LSM
    let query_key_prefix (db:IDatabase) (k:byte[]) = 
        let k2:byte[] = Array.zeroCreate k.Length
        System.Array.Copy (k, 0, k2, 0, k.Length)
        // TODO should maybe add_one to the char before the colon,
        // not to the colon itself.  but this function shouldn't know
        // about the format of the key encoding.
        add_one k2 (k2.Length-1)
        query_key_range db k k2

    let extract_value_and_id_from_end_of_index_key (ba:byte[]) =
        // TODO this will break when index key encoding changes to binary
        let s = ba |> from_utf8
        let parts = s.Split(':')
        let num = parts.Length
        (parts.[num-2], parts.[num-1])

    let query_string_equal db collId k v =
        let kpref = sprintf "x:%s:%s:s%s:" collId k v
        //printfn "kpref: %s" kpref
        let kb = kpref |> to_utf8
        let s1 = query_key_prefix db kb
        let s2 = Seq.map (fun ba -> extract_value_and_id_from_end_of_index_key ba) s1
        s2

    let wrap k id =
        let a:ubJsonValue[] = Array.zeroCreate 2
        a.[0] <- k
        a.[1] <-ubJsonValue.String id
        ubJsonValue.Array a

    let writeIndexPreface (ms:MemoryStream) collId ndxId =
        // TODO or maybe the index preface should go inside the json
        // TODO 0 instead of colon
        let kpref = (sprintf "x:%s:%s:" collId ndxId) |> to_utf8
        ms.Write(kpref, 0, kpref.Length)

    let fullEmitIndexPair (pairBuf:PairBuffer) collId (ms:MemoryStream) id ndxId (k:ubJsonValue) (v:ubJsonValue option) =
        ms.SetLength(0L)
        ms.Position <- 0L
        writeIndexPreface ms collId ndxId
        k.ToCollatable(ms)
        // TODO delim
        (id |> ubJsonValue.String).ToCollatable(ms)
        let kba = ms.ToArray()
        match v with
        | Some uv -> 
            ms.SetLength(0L)
            ms.Position <- 0L
            uv.Encode(ms)
            let vb = ms.ToArray()
            pairBuf.AddPair(kba, Blob.Array vb)
        | None -> 
            pairBuf.AddEmptyKey(kba)

    let makeQueryKey collId (ms:MemoryStream) ndxId (k:ubJsonValue) =
        ms.SetLength(0L)
        ms.Position <- 0L
        writeIndexPreface ms collId ndxId
        k.ToCollatable(ms)
        ms.ToArray()

    let map_group (doc:ubJsonValue) emit = 
        match doc.TryGetProperty("group") with
        | Some ub ->
            match ub with
            | ubJsonValue.String s -> emit ub None
            // TODO should we fuss if group is not a string?
            // TODO should we index it anyway?  (relying on the query to care about the type)
            // TODO or should we coerce the type?
            | _ -> () 
        | None -> ()

    let addDocument (pairBuf:PairBuffer) (ms:MemoryStream) collId id (doc:ubJsonValue) =
        ms.SetLength(0L)
        ms.Position <- 0L
        doc.Encode (ms)
        let uba = ms.ToArray()
        // TODO 0 instead of colon
        pairBuf.AddPair((sprintf "j:%s:%s" collId id) |> to_utf8, Blob.Array uba)

        // TODO look up views.  for each, get a name and a map function.
        let myEmit = fullEmitIndexPair pairBuf collId ms id "group"
        map_group doc myEmit

        #if not
        // now all the index items
        // TODO hook index policy to decide whether to index this record at all
        let fn path jv =
            // TODO search list of indexes to find out if anything wants this
            // TODO hook index policy to decide whether to index this key
            // TODO index policy notion of precision?  index only part of the value?
            let k = encode collId path jv id
            pairBuf.AddEmptyKey(k)
        // TODO remove old index entries for this doc
        flatten fn [] ub
        #endif

    let slurp dbFile collId jsonFile =
        let json = File.ReadAllText(jsonFile)
        let parsed = JsonValue.Parse(json)
        let foo = json_parser.parseJsonString json
        let a =
            match parsed with
            | JsonValue.Array a -> a
            | _ -> failwith "wrong"

        let f = dbf(dbFile)
        use db = new Database(f) :> IDatabase

        let pairBuf = PairBuffer(db, 10000)

        let msub = new MemoryStream()
        for i in 0 .. a.Length-1 do
            let doc = a.[i]
            let id = gid()

            let ubParsed = doc.ToUbjson()
            addDocument pairBuf msub collId id ubParsed

        async {
            use! tx = db.RequestWriteLock()
            pairBuf.Commit(tx)
        } |> Async.RunSynchronously

    let parseLiteral s =
        // TODO this is dreadful.
        let s2 = "[\"" + s + "\"]"
        let j = JsonValue.Parse(s2)
        let a =
            match j with
            | JsonValue.Array a -> a
            | _ -> failwith "wrong"
        let lit = a.[0]
        let ub = lit.ToUbjson()
        ub

    [<EntryPoint>]
    let main argv = 
        let dbFile = argv.[0]
        let collId = argv.[1]
        let op = argv.[2]
        match op with
        | "slurp" -> 
            let jsonFile = argv.[3]
            slurp dbFile collId jsonFile
        | "query" ->
            let ndx = argv.[3]
            let k = argv.[4] |> parseLiteral
            printfn "k: %A" k
            let f = dbf(dbFile)
            use db = new Database(f) :> IDatabase
            let ms = new MemoryStream()
            let qk = makeQueryKey collId ms ndx k
            printfn "qk: %A" qk
            let s = query_key_prefix db qk
            Seq.iter (fun (ba) -> printfn "%A" ba) s
        | _ -> failwith "unknown op"

        0 // return an integer exit code

