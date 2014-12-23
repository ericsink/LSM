(*
    Copyright 2014 Zumero, LLC

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

module fsTapp

open System
open System.Collections.Generic
open System.IO

open Zumero.LSM

let tid() = 
    let g = "_" + Guid.NewGuid().ToString()
    let g2 = g.Replace ("{", "")
    let g3 = g2.Replace ("}", "")
    let g4 = g3.Replace ("-", "")
    g4 + "_tmptest"

type dseg = Dictionary<byte[],Stream>

let insert (ds:dseg) (sk:string) (sv:string) =
    let k = System.Text.Encoding.UTF8.GetBytes (sk)
    let v = new MemoryStream(System.Text.Encoding.UTF8.GetBytes (sv))
    ds.[k] <- v

let createMemorySegment (rand:Random) count =
    let d = Dictionary<byte[],Stream>()
    for q in 1 .. count do
        let sk = rand.Next().ToString()
        let sv = rand.Next().ToString()
        insert d sk sv
    d

[<EntryPoint>]
let main argv = 
    printfn "prefix_compression"
    fsTests.prefix_compression() 
    #if not
    printfn "simple_write"
    fsTests.simple_write() 
    printfn "empty_cursor"
    fsTests.empty_cursor() 
    printfn "simple_write"
    fsTests.simple_write() 
    printfn "multiple"
    fsTests.multiple() 
    printfn "lexographic"
    fsTests.lexographic() 
    printfn "weird"
    fsTests.weird() 
    printfn "blobs"
    fsTests.blobs() 
    printfn "hundredk"
    fsTests.hundredk() 
    printfn "no_le_ge_multicursor"
    fsTests.no_le_ge_multicursor() 
    printfn "no_le_ge"
    fsTests.no_le_ge() 
    printfn "seek_ge_le_bigger"
    fsTests.seek_ge_le_bigger() 
    printfn "seek_ge_le"
    fsTests.seek_ge_le() 
    printfn "tombstone"
    fsTests.tombstone() 
    printfn "overwrite"
    fsTests.overwrite() 
    printfn "empty_val"
    fsTests.empty_val() 
    printfn "delete_not_there"
    fsTests.delete_not_there() 
    printfn "delete_nothing_there"
    fsTests.delete_nothing_there() 
    printfn "write_then_read"
    fsTests.write_then_read() 
    printfn "seek_ge_le_bigger_multicursor"
    fsTests.seek_ge_le_bigger_multicursor()
    printfn "race"
    fsTests.race()
    printfn "many_segments"
    fsTests.many_segments() 
    #endif

    #if not
    let f = dbf("test1" + tid())
    use db = new Database(f) :> IDatabase
    let NUM = 50
    let rand = Random()

    let start i = async {
        let commit g = async {
            let q3 = DateTime.Now
            use! tx = db.RequestWriteLock()
            let q4 = DateTime.Now
            printfn "lock: %f" ((q4-q3).TotalMilliseconds)
            tx.CommitSegments (g :: List.empty)
            let q5 = DateTime.Now
            printfn "commit: %f" ((q5-q4).TotalMilliseconds)
            }

        let q1 = DateTime.Now
        let count = rand.Next(10000)
        let d = createMemorySegment rand count
        let q2 = DateTime.Now
        printfn "dict: %f" ((q2-q1).TotalMilliseconds)
        let g = db.WriteSegment(d)
        let q3 = DateTime.Now
        printfn "segment: %f" ((q3-q2).TotalMilliseconds)
        do! commit g
        printfn "lock released"
    }

    let c = seq { for i in 0 .. NUM-1 do yield i; done }
    let workers = Seq.fold (fun acc i -> (start i) :: acc) List.empty c
    let go = Async.Parallel workers
    Async.RunSynchronously go |> ignore

    printfn "waiting for background jobs"
    let jobs = db.BackgroundMergeJobs()
    jobs |> Async.Parallel |> Async.RunSynchronously |> printfn "%A"
    printfn "waiting for the same jobs list again"
    jobs |> Async.Parallel |> Async.RunSynchronously |> printfn "%A"
    printfn "waiting for a new jobs list, which should be empty"
    db.BackgroundMergeJobs() |> Async.Parallel |> Async.RunSynchronously |> printfn "%A"

    printfn "merging"
    let qm1 = DateTime.Now
    match db.Merge(0, 4, true, true) with
    | Some f -> f |> Async.RunSynchronously |> ignore
    | None -> ()
    let qm2 = DateTime.Now;
    printfn "merge: %f" ((qm2-qm1).TotalMilliseconds)

    let loop() = 
        use csr = db.OpenCursor()
        csr.First()
        let mutable count = 0
        let q1 = DateTime.Now
        while csr.IsValid() do
            count <- count + 1
            csr.Next()
        let q2 = DateTime.Now
        printfn "count items: %d" count
        printfn "cursor: %f" ((q2-q1).TotalMilliseconds)

    printfn "iterating over all items"
    loop()
    #endif

    #if not
    let f = dbf("many_segments" + tid())
    use db = new Database(f) :> IDatabase
    let rand = Random()

    let one count = 
        let d = createMemorySegment rand count
        let g = db.WriteSegment(d)
        async {
            use! tx = db.RequestWriteLock()
            tx.CommitSegments [ g ]
            printfn "%A" g
        }

    let bunch n = async {
        for i in 0 .. n-1 do
            let count = 1+rand.Next(1000)
            do! one count
        }

    let pile = Seq.map (fun _ -> bunch 500) [1..50]
    Async.Parallel pile |> Async.RunSynchronously |> ignore
    #endif

    #if not
    let mrg = db.Merge(0, 4, false, false)
    let p1 = async {
        let! res = mrg.Value
        ignore res
    }
    let p2 = async {
        let csr = db.OpenCursor()
        csr.First()
        while csr.IsValid() do
            let k = csr.Key()
            csr.Next()
    }

    Async.Parallel [p1;p2] |> Async.RunSynchronously |> ignore
    #endif

    0 // return an integer exit code

