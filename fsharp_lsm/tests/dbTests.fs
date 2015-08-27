﻿(*
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

module fsTests

open System
open System.Collections.Generic
open System.IO
open Xunit

open Zumero.LSM

let tid() = 
    let g = "_" + Guid.NewGuid().ToString()
    let g2 = g.Replace ("{", "")
    let g3 = g2.Replace ("}", "")
    let g4 = g3.Replace ("-", "")
    g4 + "_tmptest"

type dseg = Dictionary<byte[],Stream>

let to_utf8 (s:string) =
    System.Text.Encoding.UTF8.GetBytes (s)

let from_utf8 (ba:byte[]) =
    System.Text.Encoding.UTF8.GetString (ba, 0, ba.Length)

let insert (ds:dseg) (sk:string) (sv:string) =
    let k = to_utf8 sk
    let v = new MemoryStream(System.Text.Encoding.UTF8.GetBytes (sv))
    ds.[k] <- v

let createMemorySegment (rand:Random) count =
    let d = Dictionary<byte[],Stream>()
    for q in 1 .. count do
        let sk = rand.Next().ToString()
        let sv = rand.Next().ToString()
        insert d sk sv
    d

let count_keys_forward (csr:ICursor) =
    let mutable count = 0
    csr.First()
    while csr.IsValid() do
        count <- count + 1
        csr.Next()
    count

let count_keys_backward (csr:ICursor) =
    let mutable count = 0
    csr.Last()
    while csr.IsValid() do
        count <- count + 1
        csr.Prev()
    count

[<Fact>]
let empty_cursor() = 
    let f = dbf("empty_cursor" + tid())
    use db = new Database(f) :> IDatabase
    use csr = db.OpenCursor()
    csr.First ()
    Assert.False (csr.IsValid ())
    csr.Last ()
    Assert.False (csr.IsValid ())

[<Fact>]
let simple_write() = 
    let f = dbf("simple_write" + tid())
    use db = new Database(f) :> IDatabase
    let d = dseg()
    for i in 1 .. 100 do
        let s = i.ToString()
        insert d s s
    let seg = db.WriteSegment d
    async {
        use! tx = db.RequestWriteLock()
        tx.CommitSegments [ seg ]
    } |> Async.RunSynchronously
    use csr = db.OpenCursor()
    csr.First()
    Assert.True (csr.IsValid())
    csr.Seek ((42).ToString() |> to_utf8, SeekOp.SEEK_EQ)
    Assert.True (csr.IsValid())
    csr.Next()
    let k = csr.Key() |> from_utf8
    Assert.Equal<string> ("43", k)

[<Fact>]
let first_prev() = 
    let f = dbf("first_prev" + tid())
    use db = new Database(f) :> IDatabase
    let d = dseg()
    for i in 1 .. 100 do
        let s = i.ToString()
        insert d s s
    let seg = db.WriteSegment d
    async {
        use! tx = db.RequestWriteLock()
        tx.CommitSegments [ seg ]
    } |> Async.RunSynchronously
    use csr = db.OpenCursor()
    csr.First()
    Assert.True (csr.IsValid())
    csr.Prev()
    Assert.False (csr.IsValid())

[<Fact>]
let last_next() = 
    let f = dbf("last_next" + tid())
    use db = new Database(f) :> IDatabase
    let d = dseg()
    for q in 0 .. 3 do
        for i in q*100 .. q*100+100 do
            let s = i.ToString()
            insert d s s
        let seg = db.WriteSegment d
        async {
            use! tx = db.RequestWriteLock()
            tx.CommitSegments [ seg ]
        } |> Async.RunSynchronously
    use csr = db.OpenCursor()
    csr.Seek((33).ToString() |> to_utf8, SeekOp.SEEK_GE)
    Assert.True (csr.IsValid())
    csr.Next()
    Assert.True (csr.IsValid())
    csr.Last()
    Assert.True (csr.IsValid())
    csr.Next()
    Assert.False (csr.IsValid())

[<Fact>]
let prefix_compression() = 
    let f = dbf("prefix_compression" + tid())
    use db = new Database(f) :> IDatabase
    let d = dseg()
    for i in 1 .. 10000 do
        let s = i.ToString()
        insert d ("prefix_compression"+s) s
    let seg = db.WriteSegment d
    async {
        use! tx = db.RequestWriteLock()
        tx.CommitSegments [ seg ]
    } |> Async.RunSynchronously
    use csr = db.OpenCursor()
    csr.First()
    Assert.True (csr.IsValid())

[<Fact>]
let multiple() = 
    let f = dbf("multiple" + tid())
    use db = new Database(f) :> IDatabase
    let NUM = 10

    let start i = async {
        let commit g = async {
            use! tx = db.RequestWriteLock()
            tx.CommitSegments (g :: List.empty)
        }

        let rand = Random(i)
        let count = 1+rand.Next(10000)
        let d = createMemorySegment rand count
        let g = db.WriteSegment(d)
        do! commit g
    }

    let c = seq { for i in 0 .. NUM-1 do yield i; done }
    let workers = Seq.fold (fun acc i -> (start i) :: acc) List.empty c
    let go = Async.Parallel workers
    Async.RunSynchronously go |> ignore

    // TODO could do some merges here?

    let loop() = 
        use csr = db.OpenCursor()
        csr.First()
        let mutable count = 0
        while csr.IsValid() do
            count <- count + 1
            csr.Next()

    loop()

[<Fact>]
let lexographic() = 
    let f = dbf("lexographic" + tid())
    use db = new Database(f) :> IDatabase
    let d = dseg()
    insert d "8" ""
    insert d "10" ""
    insert d "20" ""
    let g = db.WriteSegment(d)
    async {
        use! tx = db.RequestWriteLock()
        tx.CommitSegments [ g ]
    } |> Async.RunSynchronously

    use csr = db.OpenCursor()
    csr.First()
    Assert.True(csr.IsValid())
    Assert.Equal<string> ("10", csr.Key () |> from_utf8)

    csr.Next()
    Assert.True(csr.IsValid())
    Assert.Equal<string> ("20", csr.Key () |> from_utf8)

    csr.Next()
    Assert.True(csr.IsValid())
    Assert.Equal<string> ("8", csr.Key () |> from_utf8)

    csr.Next()
    Assert.False(csr.IsValid())

    // --------
    csr.Last()
    Assert.True(csr.IsValid())
    Assert.Equal<string> ("8", csr.Key () |> from_utf8)

    csr.Prev()
    Assert.True(csr.IsValid())
    Assert.Equal<string> ("20", csr.Key () |> from_utf8)

    csr.Prev()
    Assert.True(csr.IsValid())
    Assert.Equal<string> ("10", csr.Key () |> from_utf8)

    csr.Prev()
    Assert.False(csr.IsValid())

[<Fact>]
let weird() = 
    let f = dbf("weird" + tid())
    use db = new Database(f) :> IDatabase
    let t1 = dseg()
    for i in 0 .. 100-1 do
        let sk = i.ToString("000")
        let sv = i.ToString()
        insert t1 sk sv
    let t2 = dseg()
    for i in 0 .. 1000-1 do
        let sk = i.ToString("00000")
        let sv = i.ToString()
        insert t2 sk sv
    let g1 = db.WriteSegment t1
    let g2 = db.WriteSegment t2
    async {
        use! tx = db.RequestWriteLock()
        tx.CommitSegments [ g1 ]
    } |> Async.RunSynchronously
    async {
        use! tx = db.RequestWriteLock()
        tx.CommitSegments [ g2 ]
    } |> Async.RunSynchronously
    use csr = db.OpenCursor()
    csr.First()
    for i in 0 .. 100-1 do
        csr.Next()
        Assert.True(csr.IsValid())
    for i in 0 .. 50-1 do
        csr.Prev()
        Assert.True(csr.IsValid())
    for i in 0 .. 100-1 do
        csr.Next()
        Assert.True(csr.IsValid())
        csr.Next()
        Assert.True(csr.IsValid())
        csr.Prev()
        Assert.True(csr.IsValid())
    for i in 0 .. 50-1 do
        csr.Seek(csr.Key(), SeekOp.SEEK_EQ);
        Assert.True(csr.IsValid())
        csr.Next()
        Assert.True(csr.IsValid())
    for i in 0 .. 50-1 do
        csr.Seek(csr.Key(), SeekOp.SEEK_EQ);
        Assert.True(csr.IsValid())
        csr.Prev()
        Assert.True(csr.IsValid())
    for i in 0 .. 50-1 do
        csr.Seek(csr.Key(), SeekOp.SEEK_LE);
        Assert.True(csr.IsValid())
        csr.Prev()
        Assert.True(csr.IsValid())
    for i in 0 .. 50-1 do
        csr.Seek(csr.Key(), SeekOp.SEEK_GE);
        Assert.True(csr.IsValid())
        csr.Next()
        Assert.True(csr.IsValid())
    let s = csr.Key() |> from_utf8
    // got the following value from the debugger.
    // just want to make sure that it doesn't change
    // and all combos give the same answer.
    Assert.Equal<string>("00148", s); 

[<Fact>]
let blobs() = 
    let r = Random(501)
    let f = dbf("blobs" + tid())
    use db = new Database(f) :> IDatabase
    let t1 = dseg()
    for i in 1 .. 1000 do
        let k:byte[] = Array.zeroCreate (r.Next(10000))
        let v:byte[] = Array.zeroCreate (r.Next(10000))
        for q in 0 .. k.Length-1 do
            k.[q] <- r.Next(255) |> byte
        for q in 0 .. v.Length-1 do
            v.[q] <- r.Next(255) |> byte
        t1.[k] <- new MemoryStream(v)
    let g = db.WriteSegment(t1)
    async {
        use! tx = db.RequestWriteLock()
        tx.CommitSegments [ g ]
    } |> Async.RunSynchronously
    use csr = db.OpenCursor()
    for k in t1.Keys do
        let tvstrm = t1.[k]
        tvstrm.Seek(0L, SeekOrigin.Begin) |> ignore
        let tv = utils.ReadAll(tvstrm)
        csr.Seek(k, SeekOp.SEEK_EQ)
        Assert.True(csr.IsValid())
        Assert.Equal(tv.Length, csr.ValueLength())
        let tb1 = utils.ReadValue(csr.Value())
        Assert.Equal(0, bcmp.Compare tv tb1)
        // TODO ReadAll_SmallChunks

[<Fact>]
let blobs_of_many_sizes() = 
    let r = Random(501)
    let f = dbf("blobs_of_many_sizes" + tid())
    let settings = {
        Database.DefaultSettings with
            DefaultPageSize = 256
            PagesPerBlock = 4
        }
    use db = new Database(f, settings) :> IDatabase
    let t1 = dseg()
    for i in 200 .. 1500 do
        let k:byte[] = i.ToString() |> to_utf8
        let v:byte[] = Array.zeroCreate i
        for q in 0 .. v.Length-1 do
            v.[q] <- r.Next(255) |> byte
        t1.[k] <- new MemoryStream(v)
    let g = db.WriteSegment(t1)
    async {
        use! tx = db.RequestWriteLock()
        tx.CommitSegments [ g ]
    } |> Async.RunSynchronously
    use csr = db.OpenCursor()
    for k in t1.Keys do
        let tvstrm = t1.[k]
        tvstrm.Seek(0L, SeekOrigin.Begin) |> ignore
        let tv = utils.ReadAll(tvstrm)
        csr.Seek(k, SeekOp.SEEK_EQ)
        Assert.True(csr.IsValid())
        Assert.Equal(tv.Length, csr.ValueLength())
        let tb1 = utils.ReadValue(csr.Value())
        Assert.Equal(0, bcmp.Compare tv tb1)
        // TODO ReadAll_SmallChunks

[<Fact>]
let hundredk() = 
    let f = dbf("hundredk" + tid())
    use db = new Database(f) :> IDatabase
    let t1 = dseg()
    for i in 1 .. 100000 do
        let sk = (i*2).ToString()
        let sv = i.ToString()
        insert t1 sk sv
    let g = db.WriteSegment(t1)
    async {
        use! tx = db.RequestWriteLock()
        tx.CommitSegments [ g ]
    } |> Async.RunSynchronously

[<Fact>]
let no_le_ge_multicursor() = 
    let f = dbf("no_le_ge_multicursor" + tid())
    use db = new Database(f) :> IDatabase
    let t1 = dseg()
    insert t1 "c" "3"
    insert t1 "g" "7"
    let g1 = db.WriteSegment(t1)
    async {
        use! tx = db.RequestWriteLock()
        tx.CommitSegments [ g1 ]
    } |> Async.RunSynchronously
    let t2 = dseg()
    insert t2 "e" "5"
    let g2 = db.WriteSegment(t2)
    async {
        use! tx = db.RequestWriteLock()
        tx.CommitSegments [ g2 ]
    } |> Async.RunSynchronously
    use csr = db.OpenCursor()
    csr.Seek ("a" |> to_utf8, SeekOp.SEEK_LE)
    Assert.False (csr.IsValid ())

    csr.Seek ("d" |> to_utf8, SeekOp.SEEK_LE)
    Assert.True (csr.IsValid ())

    csr.Seek ("f" |> to_utf8, SeekOp.SEEK_GE)
    Assert.True (csr.IsValid ())

    csr.Seek ("h" |> to_utf8, SeekOp.SEEK_GE)
    Assert.False (csr.IsValid ())

[<Fact>]
let no_le_ge() = 
    let f = dbf("no_le_ge" + tid())
    use db = new Database(f) :> IDatabase
    let t1 = dseg()
    insert t1 "c" "3"
    insert t1 "g" "7"
    insert t1 "e" "5"
    let g1 = db.WriteSegment(t1)
    async {
        use! tx = db.RequestWriteLock()
        tx.CommitSegments [ g1 ]
    } |> Async.RunSynchronously
    use csr = db.OpenCursor()
    csr.Seek ("a" |> to_utf8, SeekOp.SEEK_LE)
    Assert.False (csr.IsValid ())

    csr.Seek ("d" |> to_utf8, SeekOp.SEEK_LE)
    Assert.True (csr.IsValid ())

    csr.Seek ("f" |> to_utf8, SeekOp.SEEK_GE)
    Assert.True (csr.IsValid ())

    csr.Seek ("h" |> to_utf8, SeekOp.SEEK_GE)
    Assert.False (csr.IsValid ())

[<Fact>]
let seek_ge_le_bigger() = 
    let f = dbf("seek_ge_le_bigger" + tid())
    use db = new Database(f) :> IDatabase
    let t1 = dseg()
    for i in 0 .. 10000-1 do
        let sk = (i*2).ToString()
        let sv = i.ToString()
        insert t1 sk sv
    let g = db.WriteSegment(t1)
    async {
        use! tx = db.RequestWriteLock()
        tx.CommitSegments [ g ]
    } |> Async.RunSynchronously
    use csr = db.OpenCursor()
    csr.Seek (to_utf8 "8088", SeekOp.SEEK_EQ);
    Assert.True (csr.IsValid ());

    csr.Seek (to_utf8 "8087", SeekOp.SEEK_EQ);
    Assert.False (csr.IsValid ());

    csr.Seek (to_utf8 "8087", SeekOp.SEEK_LE);
    Assert.True (csr.IsValid ());
    Assert.Equal<string> ("8086", csr.Key () |> from_utf8);

    csr.Seek (to_utf8 "8087", SeekOp.SEEK_GE);
    Assert.True (csr.IsValid ());
    Assert.Equal<string> ("8088", csr.Key () |> from_utf8);

[<Fact>]
let seek_ge_le() = 
    let f = dbf("seek_ge_le" + tid())
    use db = new Database(f) :> IDatabase
    let t1 = dseg()
    insert t1 "a" "1"
    insert t1 "c" "3"
    insert t1 "e" "5"
    insert t1 "g" "7"
    insert t1 "i" "9"
    insert t1 "k" "11"
    insert t1 "m" "13"
    insert t1 "o" "15"
    insert t1 "q" "17"
    insert t1 "s" "19"
    insert t1 "u" "21"
    insert t1 "w" "23"
    insert t1 "y" "25"
    let g = db.WriteSegment(t1)
    async {
        use! tx = db.RequestWriteLock()
        tx.CommitSegments [ g ]
    } |> Async.RunSynchronously
    use csr = db.OpenCursor()
    Assert.Equal (13, count_keys_forward (csr));
    Assert.Equal (13, count_keys_backward (csr));

    csr.Seek (to_utf8 "n", SeekOp.SEEK_EQ);
    Assert.False (csr.IsValid ());

    csr.Seek (to_utf8 "n", SeekOp.SEEK_LE);
    Assert.True (csr.IsValid ());
    Assert.Equal<string> ("m", csr.Key () |> from_utf8);

    csr.Seek (to_utf8 "n", SeekOp.SEEK_GE);
    Assert.True (csr.IsValid ());
    Assert.Equal<string> ("o", csr.Key () |> from_utf8);

[<Fact>]
let tombstone() = 
    let f = dbf("tombstone" + tid())
    use db = new Database(f) :> IDatabase
    let t1 = dseg()
    insert t1 "a" "1"
    insert t1 "b" "2"
    insert t1 "c" "3"
    insert t1 "d" "4"
    let g1 = db.WriteSegment(t1)
    async {
        use! tx = db.RequestWriteLock()
        tx.CommitSegments [ g1 ]
    } |> Async.RunSynchronously
    let t2 = dseg()
    t2.[to_utf8 "b"] <- null
    let g2 = db.WriteSegment(t2)
    async {
        use! tx = db.RequestWriteLock()
        tx.CommitSegments [ g2 ]
    } |> Async.RunSynchronously
    // TODO it would be nice to check the multicursor without the living wrapper
    use lc = db.OpenCursor()
    lc.First ();
    Assert.True (lc.IsValid ());
    Assert.Equal<string> ("a", lc.Key () |> from_utf8);
    Assert.Equal<string> ("1", lc.Value () |> utils.ReadValue |> from_utf8);

    lc.Next ();
    Assert.True (lc.IsValid ());
    Assert.Equal<string> ("c", lc.Key () |> from_utf8);
    Assert.Equal<string> ("3", lc.Value () |> utils.ReadValue |> from_utf8);

    lc.Next ();
    Assert.True (lc.IsValid ());
    Assert.Equal<string> ("d", lc.Key () |> from_utf8);
    Assert.Equal<string> ("4", lc.Value () |> utils.ReadValue |> from_utf8);

    lc.Next ();
    Assert.False (lc.IsValid ());

    Assert.Equal (3, count_keys_forward (lc));
    Assert.Equal (3, count_keys_backward (lc));

    lc.Seek (to_utf8 "b", SeekOp.SEEK_EQ);
    Assert.False (lc.IsValid ());

    lc.Seek (to_utf8 "b", SeekOp.SEEK_LE);
    Assert.True (lc.IsValid ());
    Assert.Equal<string> ("a", lc.Key () |> from_utf8);
    lc.Next ();
    Assert.True (lc.IsValid ());
    Assert.Equal<string> ("c", lc.Key () |> from_utf8);

    lc.Seek (to_utf8 "b", SeekOp.SEEK_GE);
    Assert.True (lc.IsValid ());
    Assert.Equal<string> ("c", lc.Key () |> from_utf8);
    lc.Prev ();
    Assert.Equal<string> ("a", lc.Key () |> from_utf8);

[<Fact>]
let overwrite() = 
    let f = dbf("overwrite" + tid())
    use db = new Database(f) :> IDatabase
    let t1 = dseg()
    insert t1 "a" "1"
    insert t1 "b" "2"
    insert t1 "c" "3"
    insert t1 "d" "4"
    let g1 = db.WriteSegment(t1)
    async {
        use! tx = db.RequestWriteLock()
        tx.CommitSegments [ g1 ]
    } |> Async.RunSynchronously
    let getb() =
        use csr = db.OpenCursor()
        csr.Seek(to_utf8 "b", SeekOp.SEEK_EQ)
        csr.Value() |> utils.ReadValue |> from_utf8
    Assert.Equal<string>("2", getb())
    let t2 = dseg()
    insert t2 "b" "5"
    let g2 = db.WriteSegment(t2)
    async {
        use! tx = db.RequestWriteLock()
        tx.CommitSegments [ g2 ]
    } |> Async.RunSynchronously
    Assert.Equal<string>("5", getb())

[<Fact>]
let empty_val() = 
    let f = dbf("empty_val" + tid())
    use db = new Database(f) :> IDatabase
    let t1 = dseg()
    insert t1 "_" ""
    let g1 = db.WriteSegment(t1)
    async {
        use! tx = db.RequestWriteLock()
        tx.CommitSegments [ g1 ]
    } |> Async.RunSynchronously
    use csr = db.OpenCursor()
    csr.Seek(to_utf8 "_", SeekOp.SEEK_EQ)
    Assert.True (csr.IsValid ());
    Assert.Equal (0, csr.ValueLength ());

[<Fact>]
let delete_not_there() = 
    let f = dbf("delete_not_there" + tid())
    use db = new Database(f) :> IDatabase
    let t1 = dseg()
    insert t1 "a" "1"
    insert t1 "b" "2"
    insert t1 "c" "3"
    insert t1 "d" "4"
    let g1 = db.WriteSegment(t1)
    async {
        use! tx = db.RequestWriteLock()
        tx.CommitSegments [ g1 ]
    } |> Async.RunSynchronously
    let t2 = dseg()
    t2.[to_utf8 "e"] <- null
    let g2 = db.WriteSegment(t2)
    async {
        use! tx = db.RequestWriteLock()
        tx.CommitSegments [ g2 ]
    } |> Async.RunSynchronously
    use csr = db.OpenCursor()
    Assert.Equal (4, count_keys_forward (csr));
    Assert.Equal (4, count_keys_backward (csr));

[<Fact>]
let delete_nothing_there() = 
    let f = dbf("delete_nothing_there" + tid())
    use db = new Database(f) :> IDatabase
    let t2 = dseg()
    t2.[to_utf8 "e"] <- null
    let g2 = db.WriteSegment(t2)
    async {
        use! tx = db.RequestWriteLock()
        tx.CommitSegments [ g2 ]
    } |> Async.RunSynchronously
    use csr = db.OpenCursor()
    Assert.Equal (0, count_keys_forward (csr));
    Assert.Equal (0, count_keys_backward (csr));

[<Fact>]
let write_then_read() = 
    let f = dbf("write_then_read" + tid())
    let write() =
        use db = new Database(f) :> IDatabase
        let d = dseg()
        for i in 1 .. 100 do
            let s = i.ToString()
            insert d s s
        let seg = db.WriteSegment d
        async {
            use! tx = db.RequestWriteLock()
            tx.CommitSegments [ seg ]
        } |> Async.RunSynchronously
        let d2 = dseg()
        d2.[to_utf8 "73"] <- null
        let g2 = db.WriteSegment(d2)
        async {
            use! tx = db.RequestWriteLock()
            tx.CommitSegments [ g2 ]
        } |> Async.RunSynchronously
    write()
    use db = new Database(f) :> IDatabase
    use csr = db.OpenCursor()
    csr.Seek ((42).ToString() |> to_utf8, SeekOp.SEEK_EQ)
    Assert.True (csr.IsValid())
    csr.Next()
    let k = csr.Key() |> from_utf8
    Assert.Equal<string> ("43", k)
    csr.Seek ((73).ToString() |> to_utf8, SeekOp.SEEK_EQ)
    Assert.False (csr.IsValid())
    csr.Seek ((73).ToString() |> to_utf8, SeekOp.SEEK_LE)
    Assert.True (csr.IsValid())
    Assert.Equal<string> ("72", (csr.Key() |> from_utf8))
    csr.Next()
    Assert.True (csr.IsValid())
    Assert.Equal<string> ("74", (csr.Key() |> from_utf8))

[<Fact>]
let many_segments() = 
    let f = dbf("many_segments" + tid())
    use db = new Database(f) :> IDatabase
    let NUM = 300
    let rand = Random(501)

    for i in 0 .. NUM-1 do
        let count = 1+rand.Next(100)
        let d = createMemorySegment rand count
        let g = db.WriteSegment(d)
        async {
            use! tx = db.RequestWriteLock()
            tx.CommitSegments [ g ]
        } |> Async.RunSynchronously

[<Fact>]
let seek_ge_le_bigger_multicursor() =
    let fname = "seek_ge_le_bigger_multicursor" + tid()
    let PAGE_SIZE = 1024
    let f n =
        let t1 = dseg()
        for i in 0 .. 10000-1 do
            insert t1 ((i * n).ToString ("0000000000")) (i.ToString())
        use fs = new FileStream(n.ToString() + fname, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite)
        let pm = SimplePageManager(PAGE_SIZE)
        let (g,root) = BTreeSegment.SortAndCreate(fs, pm, t1)
        root

    let root4 = f 4
    let root7 = f 7

    use fs_4 = new FileStream((4).ToString() + fname, FileMode.Open, FileAccess.Read)
    let csr_4 = BTreeSegment.OpenCursor(fs_4, PAGE_SIZE, root4, null)
    use fs_7 = new FileStream((7).ToString() + fname, FileMode.Open, FileAccess.Read)
    let csr_7 = BTreeSegment.OpenCursor(fs_7, PAGE_SIZE, root7, null)
    let csr = MultiCursor.Create(csr_7, csr_4)
    csr.Seek (to_utf8 "0000002330", SeekOp.SEEK_EQ)
    Assert.False (csr.IsValid ())

    csr.Seek (to_utf8 "0000002330", SeekOp.SEEK_LE)
    Assert.True (csr.IsValid ())
    Assert.Equal<string> ("0000002328", csr.Key () |> from_utf8)

    csr.Seek (to_utf8 "0000002330", SeekOp.SEEK_GE)
    Assert.True (csr.IsValid ())
    Assert.Equal<string> ("0000002331", csr.Key () |> from_utf8)

[<Fact>]
let race() =
    let settings = {
        Database.DefaultSettings with
            AutoMergeEnabled = false
        }
    let f = dbf("race" + tid())
    use db = new Database(f, settings) :> IDatabase
    let NUM = 100

    let one count = 
        let r = Random(count)
        let d = createMemorySegment r count
        let g = db.WriteSegment(d)
        async {
            use! tx = db.RequestWriteLock()
            tx.CommitSegments [ g ]
        } |> Async.RunSynchronously

    let rand = Random(501)
    for i in 0 .. NUM-1 do
        let count = 1+rand.Next(100)
        one count

    let mrg = db.Merge(0, 4, false)
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


// TODO long_vals

