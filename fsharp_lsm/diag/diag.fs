(*
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
*)

open Zumero.LSM

module diag =

    let from_utf8 (ba:byte[]) =
        System.Text.Encoding.UTF8.GetString (ba, 0, ba.Length)

    let list_segments dbFile =
        let f = dbf(dbFile)
        use db = new Database(f) :> IDatabase
        let (state,segs) = db.ListSegments()
        //printfn "%A" state
        //printfn "%A" segs
        List.iter (fun g ->
            let info = segs.[g]
            let blocks = info.blocks
            let pages = List.sumBy (fun (pb:PageBlock) -> pb.CountPages) blocks
            printfn "%A : age=%d  pages=%d  root=%d" g (info.age) pages (info.root)
            ) state

    let list_all_keys dbFile =
        let f = dbf(dbFile)
        use db = new Database(f) :> IDatabase
        use csr = db.OpenCursor()
        csr.First()
        while csr.IsValid() do
            let k = csr.Key()
            // TODO need function to decode a key
            printfn "%A -- %d" k (csr.ValueLength())
            csr.Next()

    let list_free_blocks dbFile =
        let f = dbf(dbFile)
        use db = new Database(f) :> IDatabase
        let fb = db.GetFreeBlocks()
        let pageSize = db.PageSize()
        let total = List.sumBy (fun (pb:PageBlock) -> pb.CountPages) fb
        printfn "%A" fb
        printfn "Total pages: %d" total
        printfn "Page Size: %d" pageSize
        printfn "Total bytes: %d" (total * pageSize)

    let list_segment_keys dbFile g =
        let f = dbf(dbFile)
        use db = new Database(f) :> IDatabase
        use csr = db.OpenSegmentCursor(g)
        csr.First()
        while csr.IsValid() do
            let k = csr.Key() |> from_utf8
            printfn "%s" k
            csr.Next()

    let gen dbFile =
        let settings = {
            Database.DefaultSettings with
                AutoMergeEnabled = false
            }
        let f = dbf(dbFile)
        use db = new Database(f, settings) :> IDatabase

        let a = ResizeArray<_>()
        for i in 0 .. 10-1 do
            let sortedSeq = seq { for n in (i * 100000) .. (i+1) .. (i+1) * 100000 do yield kvp(System.Text.Encoding.UTF8.GetBytes(n.ToString("00000000")),Blob.Array(System.Text.Encoding.UTF8.GetBytes((n*2).ToString()))) done }
            let g = db.WriteSegmentFromSortedSequence(sortedSeq);
            a.Add(g)
            //printfn "%A" g

        async {
            use! tx = db.RequestWriteLock()
            tx.CommitSegments (a.ToArray())
        } |> Async.RunSynchronously

        let mrg = db.Merge(0, 4, true)
        //printfn "%A" mrg
        async {
            let! res = mrg.Value
            ignore res
        } |> Async.RunSynchronously

    [<EntryPoint>]
    let main argv = 
        let dbFile = argv.[0]
        let op = argv.[1]
        match op with
        | "gen" -> 
            gen dbFile
        | "list_segments" -> 
            list_segments dbFile
        | "list_all_keys" -> 
            list_all_keys dbFile
        | "list_free_blocks" -> 
            list_free_blocks dbFile
        | "list_segment_keys" -> 
            let seg = argv.[2]
            let g = System.Int64.Parse(seg) |> uint64
            list_segment_keys dbFile g
        | _ -> failwith "Unknown op"

        0 // return an integer exit code

