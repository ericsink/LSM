(*
    Copyright 2014-2015 Zumero, LLC
    All Rights Reserved.

    This file is not open source.  

    I haven't decided what to do with this code yet.
*)

namespace Zumero

open System
open System.IO
open FParsec

open Zumero.LSM
open JsonStuff

module fj =
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

    let writeIndexPreface (ms:MemoryStream) collId ndxId =
        // TODO or maybe the index preface should go inside the json
        // TODO 0 instead of colon
        let kpref = (sprintf "x:%s:%s:" collId ndxId) |> to_utf8
        ms.Write(kpref, 0, kpref.Length)

    let fullEmitIndexPair (pairBuf:PairBuffer) collId (ms:MemoryStream) id ndxId (k:JsonValue) (v:JsonValue option) =
        ms.SetLength(0L)
        ms.Position <- 0L
        writeIndexPreface ms collId ndxId
        k.ToCollatable(ms)
        // TODO delim
        (id |> JsonValue.JString).ToCollatable(ms)
        let kba = ms.ToArray()
        match v with
        | Some uv -> 
            ms.SetLength(0L)
            ms.Position <- 0L
            uv.ToBinary(ms)
            let vb = ms.ToArray()
            pairBuf.AddPair(kba, Blob.Array vb)
        | None -> 
            pairBuf.AddEmptyKey(kba)

    let makeQueryKey collId (ms:MemoryStream) ndxId (k:JsonValue) =
        ms.SetLength(0L)
        ms.Position <- 0L
        writeIndexPreface ms collId ndxId
        k.ToCollatable(ms)
        ms.ToArray()

    let map_group (doc:JsonValue) emit = 
        match doc.TryGetProperty("group") with
        | Some ub ->
            match ub with
            | JsonValue.JString s -> emit ub None
            // TODO should we fuss if group is not a string?
            // TODO should we index it anyway?  (relying on the query to care about the type)
            // TODO or should we coerce the type?
            | _ -> () 
        | None -> ()

    let addDocument (pairBuf:PairBuffer) (ms:MemoryStream) collId id (doc:JsonValue) =
        ms.SetLength(0L)
        ms.Position <- 0L
        doc.ToBinary (ms)
        let uba = ms.ToArray()
        // TODO 0 instead of colon
        pairBuf.AddPair((sprintf "j:%s:%s" collId id) |> to_utf8, Blob.Array uba)

        // TODO look up views.  for each, get a name and a map function.
        let myEmit = fullEmitIndexPair pairBuf collId ms id "group"
        map_group doc myEmit

    let slurp dbFile collId jsonFile =
        let json = File.ReadAllText(jsonFile)
        let parsed = parseJsonString json
        let a = 
            match parsed with
            | Success (result, userState, endPos) ->
                match result with
                | JsonValue.JArray a -> a
                | _ -> failwith "wrong"
            | Failure (errorAsString, error, userState) -> 
                failwith errorAsString

        let f = dbf(dbFile)
        use db = new Database(f) :> IDatabase

        let pairBuf = PairBuffer(db, 10000)

        let msub = new MemoryStream()
        for i in 0 .. a.Length-1 do
            let doc = a.[i]
            let id = gid()
            addDocument pairBuf msub collId id doc

        async {
            use! tx = db.RequestWriteLock()
            pairBuf.Commit(tx)
        } |> Async.RunSynchronously

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
            let k = argv.[4] |> JsonValue.JString
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

