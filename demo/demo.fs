(*
    Copyright 2014-2015 Zumero, LLC
    All Rights Reserved.

    This file is not open source.  

    I haven't decided what to do with this code yet.
*)

open System.IO
open FSharp.Data
open Zumero.LSM

module fj =
    type PathElement =
        | Key of string
        | Index of int

    let to_utf8 (s:string) =
        System.Text.Encoding.UTF8.GetBytes (s)

    let from_utf8 (ba:byte[]) =
        System.Text.Encoding.UTF8.GetString (ba, 0, ba.Length)

    let rec flatten fn path jv =
        match jv with
        | JsonValue.Boolean b -> fn path jv
        | JsonValue.Float f -> fn path jv
        | JsonValue.Null -> fn path jv
        | JsonValue.Number n -> fn path jv
        | JsonValue.String s -> fn path jv
        | JsonValue.Record a -> 
            for (k,v) in a do
                let newpath = (PathElement.Key k) :: path
                flatten fn newpath v
        | JsonValue.Array a -> 
            for i in 0 .. a.Length-1 do
                let newpath = (PathElement.Index i) :: path
                let v = a.[i]
                flatten fn newpath v
            
    let encodeJsonValue jv =
        match jv with
        | JsonValue.Boolean b -> 
            "b" + if b then "1" else "0"
        | JsonValue.Float f -> 
            // TODO check to see if this is an integer?
            // TODO are we sure that JsonValue will only return float when decimal was not possible?
            "f" + f.ToString() // TODO how to deal with this?
        | JsonValue.Null -> 
            "n"
        | JsonValue.Number n -> 
            let optAsInt64 = try Some (System.Decimal.ToInt64(n)) with :? System.OverflowException -> None
            match optAsInt64 with
            | Some i64 ->
                let dec = decimal i64
                if n = dec then
                    "i" + i64.ToString()
                else
                    "d" + n.ToString()
            | None ->
                "d" + n.ToString()
        | JsonValue.String s -> 
            "s" + s
        | _ -> failwith "should have been flattened"

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
        printfn "%s" s
        s |> to_utf8

    let query_string_equal dbFile collId k v =
        let f = dbf(dbFile)
        use db = new Database(f) :> IDatabase
        let kpref = sprintf "x:%s:%s:s%s:" collId k v
        let kb = kpref |> to_utf8
        use csr = db.OpenCursor()
        csr.Seek(kb, SeekOp.SEEK_GE)
        while csr.IsValid() do
            let csrKey = csr.Key() |> from_utf8
            if csrKey.StartsWith(kpref) then
                // TODO should return a list of record ids
                printfn "%s" (csr.Key() |> from_utf8)
            else
                // this is just a way of forcing the loop to exit
                csr.Last()
            csr.Next()

    let slurp dbFile collId jsonFile =
        let json = File.ReadAllText(jsonFile)
        let parsed = JsonValue.Parse(json)
        let a =
            match parsed with
            | JsonValue.Array a -> a
            | _ -> failwith "wrong"

        let f = dbf(dbFile)
        use db = new Database(f) :> IDatabase

        let d = System.Collections.Generic.Dictionary<byte[],Stream>()

        let flush count =
            if d.Count >= count then
                let g = db.WriteSegment(d)
                d.Clear()
                async {
                    use! tx = db.RequestWriteLock()
                    tx.CommitSegments [ g ]
                    printfn "%A" g
                } |> Async.RunSynchronously

        for i in 0 .. a.Length-1 do
            let doc = a.[i]
            let id = doc.Item("id").AsString()

            // TODO we don't generally want to get the id from the record

            printfn "%A" id

            // store the doc itself
            // TODO compress this.  or ubjson.
            d.[(sprintf "j:%s:%s" collId id) |> to_utf8] <- new MemoryStream((sprintf "%A" doc) |> to_utf8) :> Stream

            // now all the index items
            // TODO hook index policy to decide whether to index this record at all
            let fn path jv =
                // TODO hook index policy to decide whether to index this key
                // TODO index policy notion of precision?  index only part of the value?
                let k = encode collId path jv id
                let v = new MemoryStream(to_utf8 "") // TODO slow
                d.[k] <- v
            flatten fn [] doc

            // when the dictionary gets too large, flush it to a segment

            flush 10000

        // flush anything left in the dictionary

        flush 0

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
            query_string_equal dbFile collId k v
        | _ -> failwith "unknown op"

        0 // return an integer exit code

