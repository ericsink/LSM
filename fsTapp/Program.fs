
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

let createMemorySegment (rand:Random) count =
    let d = Dictionary<byte[],Stream>()
    for q in 1 .. count do
        let sk = rand.Next().ToString()
        let k = System.Text.Encoding.UTF8.GetBytes (sk)
        let sv = rand.Next().ToString()
        let v = new MemoryStream(System.Text.Encoding.UTF8.GetBytes (sv))
        d.[k] <- v
    d

[<EntryPoint>]
let main argv = 
    let f = dbf("test1_" + tid())
    use db = new Database(f) :> IDatabase
    let NUM = 50
    let rand = Random()
    let start i = async {
        let count = rand.Next(10000)
        let d = createMemorySegment rand count
        let g = db.WriteSegment(d)
        use! tx = db.RequestWriteLock2()
        tx.CommitSegments (g :: List.empty)
    }

    let c = seq { for i in 0 .. NUM-1 do yield i; done }
    let workers = Seq.fold (fun acc i -> (start i) :: acc) List.empty c
    let go = Async.Parallel workers
    Async.RunSynchronously go |> ignore

    0 // return an integer exit code

