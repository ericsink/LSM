
module fsTapp

open System
open System.Collections.Generic
open System.IO

open Zumero.LSM
open Zumero.LSM.fs

let tid() = 
    let g = Guid.NewGuid().ToString()
    let g2 = g.Replace ("{", "")
    let g3 = g2.Replace ("}", "")
    let g4 = g3.Replace ("-", "")
    g4

type dseg = Dictionary<byte[],Stream>

let insert (ds:dseg) (sk:string) (sv:string) =
    let k = System.Text.Encoding.UTF8.GetBytes (sk)
    let v = new MemoryStream(System.Text.Encoding.UTF8.GetBytes (sv))
    ds.[k] <- v

[<EntryPoint>]
let main argv = 
    let f = dbf("test1_" + tid())
    use db = new Database(f) :> IDatabase
    let NUM = 50
    let rand = Random()

    let createMemorySegment (rand:Random) count =
        let d = Dictionary<byte[],Stream>()
        for q in 1 .. count do
            let sk = rand.Next().ToString()
            let sv = rand.Next().ToString()
            insert d sk sv
        d

    let start i = async {
        let commit g = async {
            let q3 = DateTime.Now
            use! tx = db.RequestWriteLock()
            let q4 = DateTime.Now
            printfn "lock: %f" ((q4-q3).TotalMilliseconds)
            tx.CommitSegments (g :: List.empty)
            let q5 = DateTime.Now
            printfn "commit: %f" ((q5-q4).TotalMilliseconds)
            if i%4=0 then
                match db.MergeAll() with
                | Some f ->
                    let blk = async {
                        let! g = f
                        tx.CommitMerge g
                    }
                    do! blk
                | None -> ()
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

    let qm1 = DateTime.Now
    match db.MergeAll() with
    | Some f ->
        async {
            let! g = f
            use! tx = db.RequestWriteLock()
            tx.CommitMerge g
        } |> Async.RunSynchronously
    | None -> ()
    let qm2 = DateTime.Now;
    printfn "mergeall: %f" ((qm2-qm1).TotalMilliseconds)

    let loop() = 
        use csr = db.OpenCursor()
        csr.First()
        let mutable count = 0
        let q1 = DateTime.Now
        while csr.IsValid() do
            count <- count + 1
            csr.Next()
        let q2 = DateTime.Now
        printfn "cursor: %f" ((q2-q1).TotalMilliseconds)

    loop()

    0 // return an integer exit code

