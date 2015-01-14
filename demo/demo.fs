(*
    Copyright 2014-2015 Zumero, LLC
    All Rights Reserved.

    This file is not open source.  

    I haven't decided what to do with this code yet.
*)

open System
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

    let write_decimal (ms:MemoryStream) (d:decimal) =
        ms.WriteByte('D'B)
        let a = System.Decimal.GetBits(d)
        for i in a do
            let ba = BitConverter.GetBytes(i)
            ms.Write(ba, 0, ba.Length)

    let write_integer (ms:MemoryStream) (i64:int64) =
        if i64 >= -128L && i64 <= 127L then
            let v = int8 i64
            ms.WriteByte('i'B)
            ms.WriteByte(byte v)
        else if i64 >= 0L && i64 <= 255L then
            let v = byte i64
            ms.WriteByte('U'B)
            ms.WriteByte(v)
        else if i64 >= (int64 System.Int16.MinValue) && i64 <= (int64 System.Int16.MaxValue) then
            let v = int16 i64
            ms.WriteByte('I'B)
            let ba = BitConverter.GetBytes(v)
            if BitConverter.IsLittleEndian then
                Array.Reverse ba
            ms.Write(ba, 0, ba.Length)
        else if i64 >= (int64 System.Int32.MinValue) && i64 <= (int64 System.Int32.MaxValue) then
            let v = int32 i64
            ms.WriteByte('l'B)
            let ba = BitConverter.GetBytes(v)
            if BitConverter.IsLittleEndian then
                Array.Reverse ba
            ms.Write(ba, 0, ba.Length)
        else
            ms.WriteByte('L'B)
            let ba = BitConverter.GetBytes(i64)
            if BitConverter.IsLittleEndian then
                Array.Reverse ba
            ms.Write(ba, 0, ba.Length)

    let write_string (ms:MemoryStream) s =
        let ba = to_utf8 s
        write_integer ms (ba.Length |> int64)
        ms.Write(ba, 0, ba.Length)

    let rec to_ubjson (ms:MemoryStream) jv =
        match jv with
        | JsonValue.Boolean b -> if b then ms.WriteByte('T'B) else ms.WriteByte('F'B)
        | JsonValue.Null -> ms.WriteByte('Z'B)
        | JsonValue.Float f ->
            ms.WriteByte('D'B)
            let ba = BitConverter.GetBytes(f)
            if BitConverter.IsLittleEndian then
                Array.Reverse ba
            ms.Write(ba, 0, ba.Length)
        | JsonValue.Number n ->
            let optAsInt64 = try Some (System.Decimal.ToInt64(n)) with :? System.OverflowException -> None
            match optAsInt64 with
            | Some i64 ->
                let dec = decimal i64
                if n = dec then
                    write_integer ms i64
                else
                    #if not
                    // TODO try float?
                    ms.WriteByte('H'B)
                    let s = jv.AsString()
                    write_string ms s
                    #endif
                    write_decimal ms n
            | None ->
                #if not
                // TODO try float?
                ms.WriteByte('H'B)
                let s = jv.AsString()
                write_string ms s
                #endif
                write_decimal ms n
        | JsonValue.String s ->
                ms.WriteByte('S'B)
                let s = jv.AsString()
                write_string ms s
        | JsonValue.Record a -> 
            ms.WriteByte('{'B)
            for (k,v) in a do
                write_string ms k
                to_ubjson ms v
            ms.WriteByte('}'B)
        | JsonValue.Array a -> 
            ms.WriteByte('['B)
            for i in 0 .. a.Length-1 do
                let v = a.[i]
                to_ubjson ms v
            ms.WriteByte(']'B)
            
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
            // TODO should have index policy to specify how this should be indexed. float/decimal/integer.
            // TODO check to see if this is an integer?
            // TODO are we sure that JsonValue will only return float when decimal was not possible?
            "f" + f.ToString() // TODO how to deal with this?
        | JsonValue.Null -> 
            "n"
        | JsonValue.Number n -> 
            // TODO consider just putting the decimal number in as binary, if we can figure out
            // how to order the bits so it sorts properly

            // TODO should have index policy to specify how this should be indexed. float/decimal/integer.
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

    let slurp dbFile collId jsonFile =
        let json = File.ReadAllText(jsonFile)
        let parsed = JsonValue.Parse(json)
        let a =
            match parsed with
            | JsonValue.Array a -> a
            | _ -> failwith "wrong"

        let f = dbf(dbFile)
        use db = new Database(f) :> IDatabase

        let d = System.Collections.Generic.Dictionary<byte[],Blob>()

        let flush count =
            if d.Count >= count then
                let g = db.WriteSegment(d)
                d.Clear()
                async {
                    use! tx = db.RequestWriteLock()
                    tx.CommitSegments [ g ]
                    printfn "%A" g
                } |> Async.RunSynchronously

        let emptyByteArray:byte[] = Array.empty
        let emptyBlobValue = Blob.Array emptyByteArray
        let msub = new MemoryStream()
        for i in 0 .. a.Length-1 do
            let doc = a.[i]
            let id = doc.Item("id").AsString()

            // TODO we don't generally want to get the id from the record

            //printfn "%A" id

            // store the doc itself
            // TODO compress this.  or ubjson.
            msub.SetLength(0L)
            msub.Position <- 0L
            to_ubjson msub doc
            let ub = msub.ToArray()
            //d.[(sprintf "j:%s:%s" collId id) |> to_utf8] <- new MemoryStream((sprintf "%A" doc) |> to_utf8) :> Stream
            d.[(sprintf "j:%s:%s" collId id) |> to_utf8] <- Blob.Array ub

            // now all the index items
            // TODO hook index policy to decide whether to index this record at all
            let fn path jv =
                // TODO search list of indexes to find out if anything wants this
                // TODO hook index policy to decide whether to index this key
                // TODO index policy notion of precision?  index only part of the value?
                let k = encode collId path jv id
                d.[k] <- emptyBlobValue
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

