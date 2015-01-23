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
        // not to the colon itself.
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

    let fullEmit (pairBuf:PairBuffer) collId (ms:MemoryStream) id ndxId (k:ubJsonValue) (v:ubJsonValue option) =
        ms.SetLength(0L)
        ms.Position <- 0L
        let kpref = (sprintf "x:%s:%s:" collId ndxId) |> to_utf8
        ms.Write(kpref, 0, kpref.Length)
        let fullKey = wrap k id
        fullKey.ToCollatable(ms)
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

    let map_description (doc:ubJsonValue) emit = 
        match doc.TryGetProperty("description") with
        | Some ub ->
            emit "description" ub (Some doc)
        | None -> ()

    let addDocument (pairBuf:PairBuffer) (msub:MemoryStream) collId id (ub:ubJsonValue) =
        msub.SetLength(0L)
        msub.Position <- 0L
        ub.Encode (msub)
        let uba = msub.ToArray()
        pairBuf.AddPair((sprintf "j:%s:%s" collId id) |> to_utf8, Blob.Array uba)

        let myEmit = fullEmit pairBuf collId msub id

        map_description ub myEmit

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

        pairBuf.Commit()

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
            let k = argv.[3]
            let v = argv.[4]
            let f = dbf(dbFile)
            use db = new Database(f) :> IDatabase
            let t1 = DateTime.Now
            let s = query_string_equal db collId k v
            let t2 = DateTime.Now
            let diff = t2 - t1
            Seq.iter (fun (v,id) -> printfn "%s" id) s
            printfn "%f ms" (diff.TotalMilliseconds)
        | _ -> failwith "unknown op"

        0 // return an integer exit code

