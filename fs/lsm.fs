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

namespace Zumero.LSM

open System
open System.IO
open System.Linq // used for OrderBy

open FSharpx.Collections

type kvp = System.Collections.Generic.KeyValuePair<byte[],Stream>

type IPendingSegment = interface end

// PageBlock, struct vs. record
// struct is a little faster and more compatible with C#
// record is more F# idiomatic
type PageBlock = 
    struct
        val firstPage: int32
        val lastPage: int32
        new (f,l) = { firstPage = f; lastPage = l; }
        override this.ToString() = sprintf "(%d,%d)" this.firstPage this.lastPage
        member this.CountPages with get() = this.lastPage - this.firstPage + 1
    end


type IPages =
    abstract member PageSize : int with get
    abstract member Begin : unit->IPendingSegment
    abstract member GetBlock : IPendingSegment->PageBlock
    abstract member End : IPendingSegment*int->Guid

type SeekOp = SEEK_EQ=0 | SEEK_LE=1 | SEEK_GE=2

type ICursor =
    inherit IDisposable
    abstract member Seek : k:byte[] * sop:SeekOp -> unit
    abstract member First : unit -> unit
    abstract member Last : unit -> unit
    abstract member Next : unit -> unit
    abstract member Prev : unit -> unit
    // the following are methods instead of properties because
    // ICursor doesn't know how expensive they will be to implement.
    abstract member IsValid : unit -> bool
    abstract member Key : unit -> byte[]
    abstract member Value : unit -> Stream
    abstract member ValueLength : unit -> int

    abstract member KeyCompare : k:byte[] -> int

type IWriteLock =
    inherit IDisposable
    abstract member CommitSegments : seq<Guid> -> unit
    abstract member CommitMerge : Guid -> unit

type IDatabaseFile =
    abstract member OpenForReading : unit -> Stream
    abstract member OpenForWriting : unit -> Stream

type DbSettings =
    {
        AutoMergeEnabled : bool
        AutoMergeMinimumPages : int
        DefaultPageSize : int
        PagesPerBlock : int
    }

type SegmentInfo =
    {
        root: int
        age: int
        blocks: PageBlock list
    } // with override this.ToString() = sprintf "(%d,%A)" this.age this.blocks

type IDatabase = 
    inherit IDisposable
    abstract member WriteSegmentFromSortedSequence : seq<kvp> -> Guid
    abstract member WriteSegment : System.Collections.Generic.IDictionary<byte[],Stream> -> Guid
    abstract member ForgetWaitingSegments : seq<Guid> -> unit

    abstract member OpenCursor : unit->ICursor 
    abstract member OpenSegmentCursor : Guid->ICursor 
    // TODO consider name such as OpenLivingCursorOnCurrentState()
    // TODO consider OpenCursorOnSegmentsInWaiting(seq<Guid>)
    // TODO consider ListSegmentsInCurrentState()
    // TODO consider OpenCursorOnSpecificSegment(seq<Guid>)

    abstract member ListSegments : unit -> (Guid list)*Map<Guid,SegmentInfo>

    abstract member RequestWriteLock : int->Async<IWriteLock>
    abstract member RequestWriteLock : unit->Async<IWriteLock>

    abstract member Merge : int*int*bool -> Async<Guid list> option
    abstract member BackgroundMergeJobs : unit->Async<Guid list> list // TODO have Auto in the name of this?

module CursorUtils =
    let ToSortedSequenceOfKeyValuePairs (csr:ICursor) = 
        seq { csr.First(); while csr.IsValid() do yield new kvp(csr.Key(), csr.Value()); csr.Next(); done }
    let ToSortedSequenceOfTuples (csr:ICursor) = 
        seq { csr.First(); while csr.IsValid() do yield (csr.Key(), csr.Value()); csr.Next(); done }
    let CountKeysForward (csr:ICursor) =
        let mutable i = 0
        csr.First()
        while csr.IsValid() do
            i <- i + 1
            csr.Next()
        i
    let CountKeysBackward (csr:ICursor) =
        let mutable i = 0
        csr.Last()
        while csr.IsValid() do
            i <- i + 1
            csr.Prev()
        i

type pgitem = 
    struct
        val page: int32
        val key: byte[]
        new (p,k) = { page = p; key = k; }
    end


module utils =
    let to_utf8 (s:string) =
        System.Text.Encoding.UTF8.GetBytes (s)

    let from_utf8 (ba:byte[]) =
        System.Text.Encoding.UTF8.GetString (ba, 0, ba.Length)

    let SeekPage(strm:Stream, pageSize, pageNumber) =
        if 0 = pageNumber then failwith "invalid page number"
        let pos = ((int64 pageNumber) - 1L) * int64 pageSize
        let newpos = strm.Seek(pos, SeekOrigin.Begin)
        if pos <> newpos then failwith "Stream.Seek failed"

    let ReadFully(strm:Stream, buf, off, len) :unit =
        // TODO consider memory alloc issue on this closure
        let rec fn sofar =
            if sofar < len then
                let got = strm.Read(buf, off + sofar, len - sofar)
                if 0 = got then failwith "end of stream in ReadFully"
                fn (sofar + got)
        fn 0

    let ReadFully2(strm:Stream, buf, off, len) =
        // TODO consider memory alloc issue on this closure
        let rec fn sofar =
            if sofar < len then
                let got = strm.Read(buf, off + sofar, len - sofar)
                if 0 = got then sofar
                else fn (sofar + got)
            else
                sofar
        fn 0

    let ReadAll(strm:Stream) =
        let len = int (strm.Length - strm.Position)
        let buf:byte[] = Array.zeroCreate len
        ReadFully(strm, buf, 0, len)
        buf

module bcmp = 
    // this code is very non-F#-ish.  but it's much faster than the
    // idiomatic version which preceded it.

    let Compare (x:byte[]) (y:byte[]) =
        let xlen = x.Length
        let ylen = y.Length
        let len = if xlen<ylen then xlen else ylen
        let mutable i = 0
        let mutable result = 0
        while i<len do
            let c = (int (x.[i])) - int (y.[i])
            if c <> 0 then
                i <- len+1 // breaks out of the loop, and signals that result is valid
                result <- c
            else
                i <- i + 1
        if i>len then result else (xlen - ylen)

    let CompareWithin (x:byte[]) off xlen (y:byte[]) =
        let ylen = y.Length
        let len = if xlen<ylen then xlen else ylen
        let mutable i = 0
        let mutable result = 0
        while i<len do
            let c = (int (x.[i + off])) - int (y.[i])
            if c <> 0 then
                i <- len+1 // breaks out of the loop, and signals that result is valid
                result <- c
            else
                i <- i + 1
        if i>len then result else (xlen - ylen)

    let CompareWithinWithPrefix (prefix:byte[]) (x:byte[]) off xlen (y:byte[]) =
        let ylen = y.Length
        let len = if xlen<ylen then xlen else ylen
        let mutable i = 0
        let mutable result = 0
        let plen = prefix.Length
        while i<len do
            let xval = if i<plen then int (prefix.[i]) else int (x.[i + off - plen])
            let c = xval - int (y.[i])
            if c <> 0 then
                i <- len+1 // breaks out of the loop, and signals that result is valid
                result <- c
            else
                i <- i + 1
        if i>len then result else (xlen - ylen)

    let PrefixMatch (x:byte[]) (y:byte[]) max =
        let len = if x.Length<y.Length then x.Length else y.Length
        let lim = if len<max then len else max
        let mutable i = 0
        while i<lim && x.[i]=y.[i] do
            i <- i + 1
        i

    let StartsWith (x:byte[]) (y:byte[]) =
        if x.Length < y.Length then
            false
        else
            let len = y.Length
            let mutable i = 0
            while i<len && x.[i]=y.[i] do
                i <- i + 1
            i=len

type private ByteComparer() =
    interface System.Collections.Generic.IComparer<byte[]> with
        member this.Compare(x,y) = bcmp.Compare x y

type private PageBuilder(pgsz:int) =
    let mutable cur = 0
    let buf:byte[] = Array.zeroCreate pgsz

    member this.Reset() = cur <- 0
    member this.Write(s:Stream) = s.Write(buf, 0, buf.Length)
    member this.PageSize = buf.Length
    member this.Buffer = buf
    member this.Position = cur
    member this.Available = buf.Length - cur
    member this.SetPageFlag(x:byte) = buf.[1] <- buf.[1] ||| x

    member this.PutByte(x:byte) =
        buf.[cur] <- byte x
        cur <- cur+1

    member this.PutStream(s:Stream, len:int) =
        utils.ReadFully(s, buf, cur, len)
        cur <- cur+len

    member this.PutStream2(s:Stream, len:int) =
        let got = utils.ReadFully2(s, buf, cur, len)
        cur <- cur+got
        got

    member this.PutArray(ba:byte[]) =
        System.Array.Copy (ba, 0, buf, cur, ba.Length)
        cur <- cur + ba.Length

    member this.PutArray(ba:byte[], off, len) =
        System.Array.Copy (ba, off, buf, cur, len)
        cur <- cur + len

    member this.PutInt32(ov:int) =
        // assert ov >= 0
        let v:uint32 = uint32 ov
        buf.[cur+0] <- byte (v >>>  24)
        buf.[cur+1] <- byte (v >>>  16)
        buf.[cur+2] <- byte (v >>>  8)
        buf.[cur+3] <- byte (v >>>  0)
        cur <- cur+sizeof<int32>

    member private this.PutInt32At(at:int, ov:int) =
        // assert ov >= 0
        let v:uint32 = uint32 ov
        buf.[at+0] <- byte (v >>>  24)
        buf.[at+1] <- byte (v >>>  16)
        buf.[at+2] <- byte (v >>>  8)
        buf.[at+3] <- byte (v >>>  0)

    member this.SetSecondToLastInt32(page:int) = 
        if cur > buf.Length - 8 then failwith "SetSecondToLastInt32 is squashing data"
        this.PutInt32At(buf.Length - 2*sizeof<int32>, page)

    member this.SetLastInt32(page:int) = 
        if cur > buf.Length - 4 then failwith "SetLastInt32 is squashing data"
        this.PutInt32At(buf.Length - sizeof<int32>, page)

    member this.PutInt16(ov:int) =
        // assert ov >= 0
        let v:uint32 = uint32 ov
        buf.[cur+0] <- byte (v >>>  8)
        buf.[cur+1] <- byte (v >>>  0)
        cur <- cur+2

    member this.PutInt16At(at:int, ov:int) =
        // assert ov >= 0
        let v:uint32 = uint32 ov
        buf.[at+0] <- byte (v >>>  8)
        buf.[at+1] <- byte (v >>>  0)

    member this.PutVarint(ov:int64) =
        // assert ov >= 0
        let v:uint64 = uint64 ov
        if v<=240UL then 
            buf.[cur] <- byte v
            cur <- cur + 1
        else if v<=2287UL then 
            buf.[cur] <- byte ((v - 240UL) / 256UL + 241UL)
            buf.[cur+1] <- byte ((v - 240UL) % 256UL)
            cur <- cur + 2
        else if v<=67823UL then 
            buf.[cur] <- 249uy
            buf.[cur+1] <- byte ((v - 2288UL) / 256UL)
            buf.[cur+2] <- byte ((v - 2288UL) % 256UL)
            cur <- cur + 3
        else if v<=16777215UL then 
            buf.[cur] <- 250uy
            buf.[cur+1] <- byte (v >>> 16)
            buf.[cur+2] <- byte (v >>>  8)
            buf.[cur+3] <- byte (v >>>  0)
            cur <- cur + 4
        else if v<=4294967295UL then 
            buf.[cur] <- 251uy
            buf.[cur+1] <- byte (v >>> 24)
            buf.[cur+2] <- byte (v >>> 16)
            buf.[cur+3] <- byte (v >>>  8)
            buf.[cur+4] <- byte (v >>>  0)
            cur <- cur + 5
        else if v<=1099511627775UL then 
            buf.[cur] <- 252uy
            buf.[cur+1] <- byte (v >>> 32)
            buf.[cur+2] <- byte (v >>> 24)
            buf.[cur+3] <- byte (v >>> 16)
            buf.[cur+4] <- byte (v >>>  8)
            buf.[cur+5] <- byte (v >>>  0)
            cur <- cur + 6
        else if v<=281474976710655UL then 
            buf.[cur] <- 253uy
            buf.[cur+1] <- byte (v >>> 40)
            buf.[cur+2] <- byte (v >>> 32)
            buf.[cur+3] <- byte (v >>> 24)
            buf.[cur+4] <- byte (v >>> 16)
            buf.[cur+5] <- byte (v >>>  8)
            buf.[cur+6] <- byte (v >>>  0)
            cur <- cur + 7
        else if v<=72057594037927935UL then 
            buf.[cur] <- 254uy
            buf.[cur+1] <- byte (v >>> 48)
            buf.[cur+2] <- byte (v >>> 40)
            buf.[cur+3] <- byte (v >>> 32)
            buf.[cur+4] <- byte (v >>> 24)
            buf.[cur+5] <- byte (v >>> 16)
            buf.[cur+6] <- byte (v >>>  8)
            buf.[cur+7] <- byte (v >>>  0)
            cur <- cur + 8
        else
            buf.[cur] <- 255uy
            buf.[cur+1] <- byte (v >>> 56)
            buf.[cur+2] <- byte (v >>> 48)
            buf.[cur+3] <- byte (v >>> 40)
            buf.[cur+4] <- byte (v >>> 32)
            buf.[cur+5] <- byte (v >>> 24)
            buf.[cur+6] <- byte (v >>> 16)
            buf.[cur+7] <- byte (v >>>  8)
            buf.[cur+8] <- byte (v >>>  0)
            cur <- cur + 9

type private PageReader(pgsz:int) =
    let mutable cur = 0
    let buf:byte[] = Array.zeroCreate pgsz

    member this.Position = cur
    member this.PageSize = buf.Length
    member this.SetPosition(x) = cur <- x
    member this.Read(s:Stream) = utils.ReadFully(s, buf, 0, buf.Length)
    member this.Read(s:Stream, off, len) = utils.ReadFully(s, buf, off, len)
    member this.Reset() = cur <- 0
    member this.Compare(len, other) = bcmp.CompareWithin buf cur len other
    member this.CompareWithPrefix(prefix, len, other) = bcmp.CompareWithinWithPrefix prefix buf cur len other
    member this.PageType = buf.[0]
    member this.Skip(len) = cur <- cur + len

    member this.GetByte() =
        let r = buf.[cur]
        cur <- cur + 1
        r

    member this.GetInt32() :int =
        let a0 = uint64 buf.[cur+0]
        let a1 = uint64 buf.[cur+1]
        let a2 = uint64 buf.[cur+2]
        let a3 = uint64 buf.[cur+3]
        let r = (a0 <<< 24) ||| (a1 <<< 16) ||| (a2 <<< 8) ||| (a3 <<< 0)
        cur <- cur + sizeof<int32>
        // assert r fits in a 32 bit signed int
        int r

    member this.GetInt32At(at) :int =
        let a0 = uint64 buf.[at+0]
        let a1 = uint64 buf.[at+1]
        let a2 = uint64 buf.[at+2]
        let a3 = uint64 buf.[at+3]
        let r = (a0 <<< 24) ||| (a1 <<< 16) ||| (a2 <<< 8) ||| (a3 <<< 0)
        // assert r fits in a 32 bit signed int
        int r

    member this.CheckPageFlag(f) = 0uy <> (buf.[1] &&& f)
    member this.GetSecondToLastInt32() = this.GetInt32At(buf.Length - 2*sizeof<int32>)
    member this.GetLastInt32() = this.GetInt32At(buf.Length - sizeof<int32>)

    member this.GetIntoArray(ba:byte[], off, len) =
        System.Array.Copy(buf, cur, ba, off, len)
        cur <- cur + len

    member this.GetArray(len) =
        let ba:byte[] = Array.zeroCreate len
        System.Array.Copy(buf, cur, ba, 0, len)
        cur <- cur + len
        ba

    member this.GetInt16() :int =
        let a0 = uint64 buf.[cur+0]
        let a1 = uint64 buf.[cur+1]
        let r = (a0 <<< 8) ||| (a1 <<< 0)
        cur <- cur + 2
        // assert r fits in a 16 bit signed int
        let r2 = int16 r
        int r2

    member this.GetVarint() :int64 =
        let a0 = uint64 buf.[cur]
        if a0 <= 240UL then 
            cur <- cur + 1
            int64 a0
        else if a0 <= 248UL then
            let a1 = uint64 buf.[cur+1]
            cur <- cur + 2
            let r = (240UL + 256UL * (a0 - 241UL) + a1)
            int64 r
        else if a0 = 249UL then
            let a1 = uint64 buf.[cur+1]
            let a2 = uint64 buf.[cur+2]
            cur <- cur + 3
            let r = (2288UL + 256UL * a1 + a2)
            int64 r
        else if a0 = 250UL then
            let a1 = uint64 buf.[cur+1]
            let a2 = uint64 buf.[cur+2]
            let a3 = uint64 buf.[cur+3]
            cur <- cur + 4
            let r = (a1<<<16) ||| (a2<<<8) ||| a3
            int64 r
        else if a0 = 251UL then
            let a1 = uint64 buf.[cur+1]
            let a2 = uint64 buf.[cur+2]
            let a3 = uint64 buf.[cur+3]
            let a4 = uint64 buf.[cur+4]
            cur <- cur + 5
            let r = (a1<<<24) ||| (a2<<<16) ||| (a3<<<8) ||| a4
            int64 r
        else if a0 = 252UL then
            let a1 = uint64 buf.[cur+1]
            let a2 = uint64 buf.[cur+2]
            let a3 = uint64 buf.[cur+3]
            let a4 = uint64 buf.[cur+4]
            let a5 = uint64 buf.[cur+5]
            cur <- cur + 6
            let r = (a1<<<32) ||| (a2<<<24) ||| (a3<<<16) ||| (a4<<<8) ||| a5
            int64 r
        else if a0 = 253UL then
            let a1 = uint64 buf.[cur+1]
            let a2 = uint64 buf.[cur+2]
            let a3 = uint64 buf.[cur+3]
            let a4 = uint64 buf.[cur+4]
            let a5 = uint64 buf.[cur+5]
            let a6 = uint64 buf.[cur+6]
            cur <- cur + 7
            let r = (a1<<<40) ||| (a2<<<32) ||| (a3<<<24) ||| (a4<<<16) ||| (a5<<<8) ||| a6
            int64 r
        else if a0 = 254UL then
            let a1 = uint64 buf.[cur+1]
            let a2 = uint64 buf.[cur+2]
            let a3 = uint64 buf.[cur+3]
            let a4 = uint64 buf.[cur+4]
            let a5 = uint64 buf.[cur+5]
            let a6 = uint64 buf.[cur+6]
            let a7 = uint64 buf.[cur+7]
            cur <- cur + 8
            let r = (a1<<<48) ||| (a2<<<40) ||| (a3<<<32) ||| (a4<<<24) ||| (a5<<<16) ||| (a6<<<8) ||| a7
            int64 r
        else
            let a1 = uint64 buf.[cur+1]
            let a2 = uint64 buf.[cur+2]
            let a3 = uint64 buf.[cur+3]
            let a4 = uint64 buf.[cur+4]
            let a5 = uint64 buf.[cur+5]
            let a6 = uint64 buf.[cur+6]
            let a7 = uint64 buf.[cur+7]
            let a8 = uint64 buf.[cur+8]
            cur <- cur + 9
            let r = (a1<<<56) ||| (a2<<<48) ||| (a3<<<40) ||| (a4<<<32) ||| (a5<<<24) ||| (a6<<<16) ||| (a7<<<8) ||| a8
            int64 r

module misc =
    // TODO not sure this is useful.  it opens an ICursor for a dictionary,
    // using an object expression.  this code is trusting the dictionary not
    // to get modified.
    let openCursor (pairs:System.Collections.Generic.Dictionary<byte[],Stream>) = 
        // The following is a ref cell because mutable variables
        // cannot be captured by a closure.
        let cur = ref -1
        let keys:byte[][] = (Array.ofSeq pairs.Keys)
        let sortfunc x y = bcmp.Compare x y
        Array.sortInPlaceWith sortfunc keys

        let rec search k min max sop le ge = 
            if max < min then
                match sop with
                | SeekOp.SEEK_EQ -> -1
                | SeekOp.SEEK_LE -> le
                | _ -> ge
            else
                let mid = (max + min) / 2
                let kmid = keys.[mid]
                let cmp = bcmp.Compare kmid k
                if 0 = cmp then mid
                else if cmp<0  then search k (mid+1) max sop mid ge
                else search k min (mid-1) sop le mid

        { new ICursor with
            member this.Dispose() =
                ()

            member this.IsValid() =
                (!cur >= 0) && (!cur < keys.Length)

            member this.First() =
                cur := 0

            member this.Last() =
                cur := keys.Length - 1
            
            member this.Next() =
                cur := !cur + 1

            member this.Prev() =
                cur := !cur - 1

            member this.Key() =
                keys.[!cur]
            
            member this.KeyCompare(k) =
                bcmp.Compare (keys.[!cur]) k

            member this.Value() =
                let v = pairs.[keys.[!cur]]
                if v <> null then ignore (v.Seek(0L, SeekOrigin.Begin))
                v

            member this.ValueLength() =
                let v = pairs.[keys.[!cur]]
                if v <> null then (int v.Length) else -1

            member this.Seek (k, sop) =
                cur := search k 0 (keys.Length-1) sop -1 -1
        }

type private Direction = FORWARD=0 | BACKWARD=1 | WANDERING=2

type MultiCursor private (_subcursors:seq<ICursor>) =
    let subcursors = List.ofSeq _subcursors
    let mutable cur:ICursor option = None
    let mutable dir = Direction.WANDERING

    let find sortfunc = 
        if List.isEmpty subcursors then None
        else
            let first =
                let hd = List.head subcursors
                if hd.IsValid() then Some hd else None

            let tail = List.tail subcursors

            if List.isEmpty tail then
                first
            else
                let fldr (cur:ICursor option) (csr:ICursor) =
                    if csr.IsValid() then 
                        match cur with
                        | Some winning -> 
                            let cmp = sortfunc csr winning
                            if cmp < 0 then Some csr else cur
                        | None -> Some csr
                    else
                        cur
                let result = List.fold fldr first tail
                result

    let findMin() = 
        let sortfunc (a:ICursor) (b:ICursor) = a.KeyCompare(b.Key())
        find sortfunc
    
    let findMax() = 
        let sortfunc (a:ICursor) (b:ICursor) = b.KeyCompare(a.Key())
        find sortfunc

    let dispose itIsSafeToAlsoFreeManagedObjects =
        if itIsSafeToAlsoFreeManagedObjects then
            List.iter (fun (x:ICursor) -> x.Dispose();) subcursors

    override this.Finalize() =
        dispose false

    static member Create(_subcursors:seq<ICursor>) :ICursor =
        let mc = new MultiCursor(_subcursors)
        mc :> ICursor
               
    static member Create([<ParamArray>] _subcursors: ICursor[]) :ICursor =
        let mc = new MultiCursor(_subcursors)
        mc :> ICursor
               
    interface ICursor with
        member this.Dispose() =
            dispose true
            GC.SuppressFinalize(this)

        member this.IsValid() = 
            match cur with
            | Some csr -> csr.IsValid()
            | None -> false

        member this.First() =
            let f (x:ICursor) = x.First()
            List.iter f subcursors
            cur <- findMin()
            dir <- Direction.WANDERING // TODO why?

        member this.Last() =
            let f (x:ICursor) = x.Last()
            List.iter f subcursors
            cur <- findMax()
            dir <- Direction.WANDERING // TODO why?

        // the following members are asking for the value of cur (an option)
        // without checking or matching on it.  they'll crash if cur is None.
        // this matches the C# behavior and the expected behavior of ICursor.
        // don't call these methods without checking IsValid() first.
        member this.Key() = cur.Value.Key()
        member this.KeyCompare(k) = cur.Value.KeyCompare(k)
        member this.Value() = cur.Value.Value()
        member this.ValueLength() = cur.Value.ValueLength()

        member this.Next() =
            let k = cur.Value.Key()
            let f (csr:ICursor) :unit = 
                if (dir <> Direction.FORWARD) && (csr <> cur.Value) then csr.Seek (k, SeekOp.SEEK_GE)
                if csr.IsValid() && (0 = csr.KeyCompare(k)) then csr.Next()
            List.iter f subcursors
            cur <- findMin()
            dir <- Direction.FORWARD

        member this.Prev() =
            let k = cur.Value.Key()
            let f (csr:ICursor) :unit = 
                if (dir <> Direction.BACKWARD) && (csr <> cur.Value) then csr.Seek (k, SeekOp.SEEK_LE)
                if csr.IsValid() && (0 = csr.KeyCompare(k)) then csr.Prev()
            List.iter f subcursors
            cur <- findMax()
            dir <- Direction.BACKWARD

        member this.Seek (k, sop) =
            cur <- None
            dir <- Direction.WANDERING
            let f (csr:ICursor) :bool =
                csr.Seek (k, sop)
                if cur.IsNone && csr.IsValid() && ( (SeekOp.SEEK_EQ = sop) || (0 = csr.KeyCompare (k)) ) then 
                    cur <- Some csr
                    true
                else
                    false
            if not (List.exists f subcursors) then
                match sop with
                | SeekOp.SEEK_GE ->
                    cur <- findMin()
                    if cur.IsSome then dir <- Direction.FORWARD
                | SeekOp.SEEK_LE ->
                    cur <- findMax()
                    if cur.IsSome then dir <- Direction.BACKWARD
                | _ -> ()

type LivingCursor private (ch:ICursor) =
    let chain = ch

    let skipTombstonesForward() =
        while (chain.IsValid() && (chain.ValueLength() < 0)) do
            chain.Next()

    let skipTombstonesBackward() =
        while (chain.IsValid() && (chain.ValueLength() < 0)) do
            chain.Prev()

    let dispose itIsSafeToAlsoFreeManagedObjects =
        if itIsSafeToAlsoFreeManagedObjects then
            chain.Dispose()

    static member Create(_chain:ICursor) :ICursor =
        let csr = new LivingCursor(_chain)
        csr :> ICursor
               
    override this.Finalize() =
        dispose false

    interface ICursor with
        member this.Dispose() =
            dispose true
            GC.SuppressFinalize(this)

        member this.First() = 
            chain.First()
            skipTombstonesForward()

        member this.Last() = 
            chain.Last()
            skipTombstonesBackward()

        member this.Key() = chain.Key()
        member this.Value() = chain.Value()
        member this.ValueLength() = chain.ValueLength()
        member this.IsValid() = chain.IsValid() && (chain.ValueLength() >= 0)
        member this.KeyCompare k = chain.KeyCompare k

        member this.Next() =
            chain.Next()
            skipTombstonesForward()

        member this.Prev() =
            chain.Prev()
            skipTombstonesBackward()
        
        member this.Seek (k, sop) =
            chain.Seek (k, sop)
            match sop with
            | SeekOp.SEEK_GE -> skipTombstonesForward()
            | SeekOp.SEEK_LE -> skipTombstonesBackward()
            | _ -> ()

module Varint =
    let SpaceNeededFor v :int = 
        if v<=240L then 1
        else if v<=2287L then 2
        else if v<=67823L then 3
        else if v<=16777215L then 4
        else if v<=4294967295L then 5
        else if v<=1099511627775L then 6
        else if v<=281474976710655L then 7
        else if v<=72057594037927935L then 8
        else 9

module bt =
    // page types
    // TODO enum?
    let private LEAF_NODE:byte = 1uy
    let private PARENT_NODE:byte = 2uy
    let private OVERFLOW_NODE:byte = 3uy

    // flags on values
    // TODO enum?
    let private FLAG_OVERFLOW:byte = 1uy
    let private FLAG_TOMBSTONE:byte = 2uy

    // flags on pages
    // TODO enum?
    let private FLAG_ROOT_NODE:byte = 1uy
    let private FLAG_BOUNDARY_NODE:byte = 2uy
    let private FLAG_ENDS_ON_BOUNDARY:byte = 4uy


    type private ParentState = 
        { 
            sofar:int 
            items:pgitem list 
            overflows:Map<byte[],int32>
            nextGeneration:pgitem list 
            blk:PageBlock
        }

    // TODO gratuitously different names of the items in these
    // two unions

    type private KeyLocation =
        | Inline
        | Overflow of page:int

    type private ValueLocation =
        | Tombstone
        | Buffer of vbuf:byte[]
        | Overflowed of len:int*page:int

    type private LeafPair =
        {
            key:byte[]
            kLoc:KeyLocation
            vLoc:ValueLocation
        }

    type private LeafState =
        // TODO considered making this a struct, but it doesn't make much/any perf difference
        {
            sofarLeaf:int
            keys:(LeafPair list)
            prevLeaf:int
            prefixLen:int
            firstLeaf:int32
            leaves:pgitem list 
            blk:PageBlock
        }

    let findPrefix keys =
        let fldr acc b = 
            let (first,cur) = acc
            let n = bcmp.PrefixMatch first b cur
            (first,n)
        let k = List.head keys
        let b = List.fold fldr (k, k.Length) (List.tail keys)
        let len = snd b
        len

    type private myConcatStream(_a:byte[], _alen:int, _s:Stream) =
        inherit Stream()
        let a = _a
        let alen = _alen
        let s = _s
        let mutable sofar = 0L

        override this.CanRead = true

        override this.Read(ba,offset,wanted) =
            if (int sofar) < alen then
                let available = alen - int sofar
                let num = Math.Min (available, wanted)
                System.Array.Copy (a, int sofar, ba, offset, num)
                sofar <- sofar + int64 num
                num
            else  
                let num = s.Read(ba,offset,wanted)
                sofar <- sofar + int64 num
                num

        override this.CanSeek = false
        override this.CanWrite = false
        override this.Length = raise (NotSupportedException())
        override this.SetLength(_) = raise (NotSupportedException())
        override this.Flush() = raise (NotSupportedException())
        override this.Seek(_,_) = raise (NotSupportedException())
        override this.Write(_,_,_) = raise (NotSupportedException())
        override this.Position
            with get() = raise (NotSupportedException())
            and set(_) = raise (NotSupportedException())

    let CreateFromSortedSequenceOfKeyValuePairs(fs:Stream, pageManager:IPages, source:seq<kvp>) = 
        let pageSize = pageManager.PageSize
        let token = pageManager.Begin()
        let pbFirstOverflow = PageBuilder(pageSize)
        let pbOverflow = PageBuilder(pageSize)

        let writeOverflow (startingBlk:PageBlock) (ba:Stream) =
            printfn "BEGIN OVERFLOW: %A" startingBlk
            let pageSize = pbOverflow.PageSize

            let buildFirstPage () =
                pbFirstOverflow.Reset()
                pbFirstOverflow.PutByte(OVERFLOW_NODE)
                pbFirstOverflow.PutByte(0uy) // starts 0, may be changed later
                let room = (pageSize - (2 + sizeof<int32>))
                let put = pbFirstOverflow.PutStream2(ba, room)
                // something will be put in lastInt32 later
                printfn "buildFirstPage put: %d" put
                (put, put<room)

            let buildRegularPage () = 
                pbOverflow.Reset()
                let room = pageSize
                let put = pbOverflow.PutStream2(ba, room)
                printfn "buildRegularPage put: %d" put
                (put, put<room)

            let buildBoundaryPage () =
                pbOverflow.Reset()
                let room = (pageSize - sizeof<int32>)
                let put = pbOverflow.PutStream2(ba, room)
                // something will be put in lastInt32 before the page is written
                printfn "buildBoundaryPage put: %d" put
                (put, put<room)

            let writeRegularPages _max _sofar =
                let rec fn i sofar =
                    if i < _max then
                        let (put, finished) = buildRegularPage()
                        if put=0 then
                            (i, sofar, true)
                        else
                            let sofar = sofar + put
                            pbOverflow.Write(fs)
                            if not finished then                            
                                fn (i+1) sofar
                            else 
                                (i+1, sofar, true)
                    else
                        (i, sofar, false)

                fn 0 _sofar

            let rec writeOneBlock sofar (firstBlk:PageBlock) :int*PageBlock =
                // each trip through this loop will write out one
                // block, starting with the overflow first page,
                // followed by zero-or-more "regular" overflow pages,
                // which have no header.  we'll stop at the block boundary,
                // either because we land there or because the whole overflow
                // won't fit and we have to continue into the next block.
                // the boundary page will be like a regular overflow page,
                // headerless, but it is four bytes smaller.
                printfn "writeOneBlock: sofar=%d  blk=%A" sofar firstBlk
                let (putFirst,finished) = buildFirstPage ()
                if putFirst=0 then 
                    (sofar, firstBlk)
                else
                    // note that we haven't written the first page yet.  we may have to fix
                    // a couple of things before it gets written out.
                    let sofar = sofar + putFirst
                    if firstBlk.firstPage = firstBlk.lastPage then
                        printfn "firstPage is last in block"
                        // the first page landed on a boundary.
                        // we can just set the flag and write it now.
                        pbFirstOverflow.SetPageFlag(FLAG_BOUNDARY_NODE)
                        let blk = pageManager.GetBlock(token)
                        pbFirstOverflow.SetLastInt32(blk.firstPage)
                        printfn "    firstPage in next block: %d" (blk.firstPage)
                        pbFirstOverflow.Write(fs)
                        utils.SeekPage(fs, pageSize, blk.firstPage)
                        if not finished then
                            writeOneBlock sofar blk
                        else
                            (sofar, blk)
                    else 
                        let firstRegularPageNumber = firstBlk.firstPage + 1
                        if finished then
                            printfn "firstPage is only page"
                            // the first page is also the last one
                            pbFirstOverflow.SetLastInt32(0) // offset to last used page in this block, which is this one
                            pbFirstOverflow.Write(fs)
                            (sofar, PageBlock(firstRegularPageNumber,firstBlk.lastPage))
                        else
                            // we need to write more pages,
                            // until the end of the block,
                            // or the end of the stream, 
                            // whichever comes first

                            utils.SeekPage(fs, pageSize, firstRegularPageNumber)

                            // availableBeforeBoundary is the number of pages until the boundary,
                            // NOT counting the boundary page, and the first page in the block
                            // has already been accounted for, so we're just talking about data pages.
                            let availableBeforeBoundary = 
                                if firstBlk.lastPage > 0 
                                then (firstBlk.lastPage - firstRegularPageNumber) 
                                else System.Int32.MaxValue

                            printfn "availableBeforeBoundary: %d" availableBeforeBoundary

                            let (numRegularPages, sofar, finished) = 
                                writeRegularPages availableBeforeBoundary sofar

                            printfn "wrote %d regular pages" numRegularPages

                            if finished then
                                printfn "done after regular"
                                // go back and fix the first page
                                pbFirstOverflow.SetLastInt32(numRegularPages)
                                utils.SeekPage(fs, pageSize, firstBlk.firstPage)
                                pbFirstOverflow.Write(fs)
                                // now reset to the next page in the block
                                let blk = PageBlock(firstRegularPageNumber + numRegularPages, firstBlk.lastPage)
                                utils.SeekPage(fs, pageSize, blk.firstPage)
                                (sofar,blk)
                            else
                                // we need to write out a regular page except with a
                                // boundary pointer in it.  and we need to set
                                // FLAG_ENDS_ON_BOUNDARY on the first
                                // overflow page in this block.

                                let (putBoundary,finished) = buildBoundaryPage ()
                                if putBoundary=0 then
                                    printfn "actually, no"
                                    // go back and fix the first page
                                    pbFirstOverflow.SetLastInt32(numRegularPages)
                                    utils.SeekPage(fs, pageSize, firstBlk.firstPage)
                                    pbFirstOverflow.Write(fs)

                                    // now reset to the next page in the block
                                    let blk = PageBlock(firstRegularPageNumber + numRegularPages, firstBlk.lastPage)
                                    utils.SeekPage(fs, pageSize, firstBlk.lastPage)
                                    (sofar,blk)
                                else
                                    printfn "boundary page"
                                    // write the boundary page
                                    let sofar = sofar + putBoundary
                                    let blk = pageManager.GetBlock(token)
                                    pbOverflow.SetLastInt32(blk.firstPage)
                                    pbOverflow.Write(fs)

                                    // go back and fix the first page
                                    pbFirstOverflow.SetPageFlag(FLAG_ENDS_ON_BOUNDARY)
                                    pbFirstOverflow.SetLastInt32(numRegularPages + 1)
                                    utils.SeekPage(fs, pageSize, firstBlk.firstPage)
                                    pbFirstOverflow.Write(fs)

                                    // now reset to the first page in the next block
                                    utils.SeekPage(fs, pageSize, blk.firstPage)
                                    if not finished then
                                        writeOneBlock sofar blk
                                    else
                                        (sofar,blk)

            let res = writeOneBlock 0 startingBlk
            printfn "DONE OVERFLOW: %A" res
            res

        let pb = PageBuilder(pageSize)

        let writeLeaves (leavesBlk:PageBlock) :PageBlock*(pgitem list)*int =
            // 2 for the page type and flags
            // 4 for the prev page
            // 2 for the stored count
            // 4 for lastInt32 (which isn't in pb.Available)
            let LEAF_PAGE_OVERHEAD = 2 + 4 + 2 + 4

            let buildLeaf st =
                pb.Reset()
                pb.PutByte(LEAF_NODE)
                pb.PutByte(0uy) // flags
                pb.PutInt32 (st.prevLeaf) // prev page num.
                // TODO prefixLen is one byte.  should it be two?
                pb.PutByte(byte st.prefixLen)
                if st.prefixLen > 0 then
                    let k = (List.head st.keys).key
                    pb.PutArray(k, 0, st.prefixLen)
                pb.PutInt16 (st.keys.Length)
                List.iter (fun lp ->
                    let k = lp.key
                    match lp.kLoc with
                    | Inline ->
                        pb.PutByte(0uy) // flags
                        pb.PutVarint(int64 k.Length)
                        pb.PutArray(k, st.prefixLen, k.Length - st.prefixLen)
                    | Overflow kpage ->
                        pb.PutByte(FLAG_OVERFLOW)
                        pb.PutVarint(int64 k.Length)
                        pb.PutInt32(kpage)
                    match lp.vLoc with
                    | Tombstone ->
                        pb.PutByte(FLAG_TOMBSTONE)
                    | Buffer (vbuf) ->
                        pb.PutByte(0uy)
                        pb.PutVarint(int64 vbuf.Length)
                        pb.PutArray(vbuf)
                    | Overflowed (vlen,vpage) ->
                        pb.PutByte(FLAG_OVERFLOW)
                        pb.PutVarint(int64 vlen)
                        pb.PutInt32(vpage)
                   ) (List.rev st.keys) // TODO rev is unfortunate

            let writeLeaf st isRootPage = 
                buildLeaf st
                let thisPageNumber = st.blk.firstPage
                let firstLeaf = if List.isEmpty st.leaves then thisPageNumber else st.firstLeaf
                let nextBlk = 
                    if isRootPage then
                        PageBlock(thisPageNumber + 1, st.blk.lastPage)
                    else if thisPageNumber = st.blk.lastPage then
                        pb.SetPageFlag FLAG_BOUNDARY_NODE
                        let newBlk = pageManager.GetBlock(token)
                        pb.SetLastInt32(newBlk.firstPage)
                        newBlk
                    else
                        PageBlock(thisPageNumber + 1, st.blk.lastPage)
                pb.Write(fs)
                if nextBlk.firstPage <> (thisPageNumber+1) then utils.SeekPage(fs, pageSize, nextBlk.firstPage)
                {
                    sofarLeaf=0
                    keys=[]
                    prevLeaf=thisPageNumber
                    prefixLen=(-1)
                    firstLeaf=firstLeaf
                    blk=nextBlk
                    leaves=pgitem(thisPageNumber,(List.head st.keys).key)::st.leaves
                    }

            let vbuf:byte[] = Array.zeroCreate pageSize

            // TODO can the overflow page number become a varint?
            let neededForOverflowPageNumber = sizeof<int32>

            // the max limit of an inline key is when that key is the only
            // one in the leaf, and its value is overflowed.

            let maxKeyInline = 
                pageSize 
                - LEAF_PAGE_OVERHEAD 
                - 1 // prefixLen
                - 1 // key flags
                - Varint.SpaceNeededFor(int64 pageSize) // approx worst case inline key len
                - 1 // value flags
                - 9 // worst case varint value len
                - neededForOverflowPageNumber // overflowed value page

            let kLocNeed (k:byte[]) kloc prefixLen =
                let klen = k.Length
                match kloc with
                | Inline ->
                    1 + Varint.SpaceNeededFor(int64 klen) + klen - prefixLen
                | Overflow _ ->
                    1 + Varint.SpaceNeededFor(int64 klen) + neededForOverflowPageNumber

            let vLocNeed vloc =
                match vloc with
                | Tombstone -> 
                    1
                | Buffer vbuf ->
                    let vlen = vbuf.Length
                    1 + Varint.SpaceNeededFor(int64 vlen) + vlen
                | Overflowed (vlen,_) ->
                    1 + Varint.SpaceNeededFor(int64 vlen) + neededForOverflowPageNumber

            let leafPairSize prefixLen lp =
                kLocNeed lp.key lp.kLoc prefixLen
                +
                vLocNeed lp.vLoc

            let defaultPrefixLen (k:byte[]) =
                // TODO max prefix.  relative to page size?  must fit in byte.
                if k.Length > 255 then 255 else k.Length

            let foldLeaf st (pair:kvp) = 
                let k = pair.Key
                //printfn "writing key: %s" (k|>utils.from_utf8)
                // assert k <> null
                // but pair.Value might be null (a tombstone)

                // TODO is it possible for this to conclude that the key must be overflowed
                // when it would actually fit because of prefixing?

                let blkAfterKey,kloc = 
                    if k.Length <= maxKeyInline then
                        st.blk, Inline
                    else
                        let vPage = st.blk.firstPage
                        let _,newBlk = writeOverflow st.blk (new MemoryStream(k))
                        newBlk, (Overflow (vPage))

                // the max limit of an inline value is when the key is inline
                // on a new page.

                let availableOnNewPageAfterKey = 
                    pageSize 
                    - LEAF_PAGE_OVERHEAD 
                    - 1 // prefixLen
                    - 1 // key flags
                    - Varint.SpaceNeededFor(int64 k.Length)
                    - k.Length 
                    - 1 // value flags

                // availableOnNewPageAfterKey needs to accomodate the value and its length as a varint.
                // it might already be <=0 because of the key length

                let maxValueInline = 
                    if availableOnNewPageAfterKey > 0 then                    
                        let neededForVarintLen = Varint.SpaceNeededFor(int64 availableOnNewPageAfterKey)
                        let avail2 = availableOnNewPageAfterKey - neededForVarintLen
                        if avail2 > 0 then avail2 else 0
                    else 0

                let blkAfterValue, vloc = 
                    if pair.Value = null then
                        blkAfterKey, Tombstone
                    else match kloc with
                         | Inline ->
                            if maxValueInline = 0 then
                                let valuePage = blkAfterKey.firstPage
                                let len,newBlk = writeOverflow blkAfterKey pair.Value
                                newBlk, (Overflowed (len,valuePage))
                            else
                                let vread = utils.ReadFully2(pair.Value, vbuf, 0, maxValueInline+1)
                                if vread < maxValueInline then
                                    // TODO this alloc+copy is unfortunate
                                    let va:byte[] = Array.zeroCreate vread
                                    System.Array.Copy (vbuf, 0, va, 0, vread)
                                    blkAfterKey, Buffer va
                                else
                                    let valuePage = blkAfterKey.firstPage
                                    let len,newBlk = writeOverflow blkAfterKey (new myConcatStream(vbuf, vread, pair.Value))
                                    newBlk, (Overflowed (len,valuePage))
                         | Overflow _ ->
                            let valuePage = blkAfterKey.firstPage
                            let len,newBlk = writeOverflow blkAfterKey pair.Value
                            newBlk, (Overflowed (len,valuePage))


                let lp = {
                            key=k
                            kLoc=kloc
                            vLoc=vloc
                            }

                // whether/not the key/value are to be overflowed is now already decided.
                // now all we have to do is decide if this key/value are going into this leaf
                // or not.  note that it is possible to overflow these and then have them not
                // fit into the current leaf and end up landing in the next leaf.

                let st = {st with blk=blkAfterValue}

                let st = 
                    // TODO ignore prefixLen for overflowed keys?
                    let newPrefixLen = 
                        if List.isEmpty st.keys then
                            defaultPrefixLen k
                        else
                            bcmp.PrefixMatch ((List.head st.keys).key) k st.prefixLen
                    let sofar = 
                        if newPrefixLen < st.prefixLen then
                            // the prefixLen would change with the addition of this key,
                            // so we need to recalc sofar
                            // TODO is it a problem that we're doing this without List.rev ?
                            List.sumBy (fun lp -> leafPairSize newPrefixLen lp) st.keys
                        else
                            st.sofarLeaf
                    let available = pageSize - (sofar + LEAF_PAGE_OVERHEAD + 1 + newPrefixLen)
                    let needed = kLocNeed k kloc newPrefixLen + vLocNeed vloc
                    let fit = (available >= needed)
                    let writeThisPage = (not (List.isEmpty st.keys)) && (not fit)

                    if writeThisPage then
                        writeLeaf st false
                    else
                        st

                let st =
                    // TODO ignore prefixLen for overflowed keys?
                    let newPrefixLen = 
                        if List.isEmpty st.keys then
                            defaultPrefixLen k
                        else
                            bcmp.PrefixMatch ((List.head st.keys).key) k st.prefixLen
                    let sofar = 
                        if newPrefixLen < st.prefixLen then
                            // the prefixLen will change with the addition of this key,
                            // so we need to recalc sofar
                            // TODO is it a problem that we're doing this without List.rev ?
                            List.sumBy (fun lp -> leafPairSize newPrefixLen lp) st.keys
                        else
                            st.sofarLeaf
                    {st with 
                        sofarLeaf=sofar + leafPairSize newPrefixLen lp
                        keys=lp::st.keys
                        prefixLen=newPrefixLen
                        }
                        
                st

            // this is the body of writeLeaves
            //let source = seq { csr.First(); while csr.IsValid() do yield (csr.Key(), csr.Value()); csr.Next(); done }
            let initialState = {
                sofarLeaf=0
                firstLeaf=0
                prevLeaf=0
                keys=[]
                prefixLen=(-1)
                leaves=[]
                blk=leavesBlk
                }
            let middleState = Seq.fold foldLeaf initialState source
            let finalState = 
                if not (List.isEmpty middleState.keys) then
                    let isRootNode = List.isEmpty middleState.leaves
                    writeLeaf middleState isRootNode
                else
                    middleState
            (finalState.blk,finalState.leaves,finalState.firstLeaf)

        // this is the body of Create
        let startingBlk = pageManager.GetBlock(token)
        utils.SeekPage(fs, pageSize, startingBlk.firstPage)

        let (blkAfterLeaves, leaves, firstLeaf) = writeLeaves startingBlk

        // all the leaves are written.
        // now write the parent pages.
        // maybe more than one level of them.
        // keep writing until we have written a level which has only one node,
        // which is the root node.

        let lastLeaf = leaves.[0].page

        let writeParentNodes (startingBlk:PageBlock) children =
            // 2 for the page type and flags
            // 2 for the stored count
            // 5 for the extra ptr we will add at the end, a varint, 5 is worst case (page num < 4294967295L)
            // 4 for lastInt32
            let PARENT_PAGE_OVERHEAD = 2 + 2 + 5 + 4

            let calcAvailable currentSize couldBeRoot =
                let basicSize = pageSize - currentSize
                let allowanceForRootNode = if couldBeRoot then (sizeof<int32>) else 0 // first/last Leaf, lastInt32 already
                basicSize - allowanceForRootNode

            let buildParentPage (items:pgitem list) lastPtr (overflows:Map<byte[],int32>) =
                pb.Reset ()
                pb.PutByte (PARENT_NODE)
                pb.PutByte (0uy)
                pb.PutInt16 (items.Length)
                // store all the ptrs, n+1 of them
                List.iter (fun (x:pgitem) -> pb.PutVarint(int64 (x.page))) items
                pb.PutVarint(int64 lastPtr)
                // store all the keys, n of them
                let fn (x:pgitem) = 
                    let k = x.key
                    match overflows.TryFind(k) with
                    | Some pg ->
                        pb.PutByte(FLAG_OVERFLOW)
                        pb.PutVarint(int64 k.Length)
                        pb.PutInt32(pg)
                    | None ->
                        pb.PutByte(0uy)
                        pb.PutVarint(int64 k.Length)
                        pb.PutArray(k)
                List.iter fn items

            let writeParentPage st (pair:pgitem) isRootNode =
                let pagenum = pair.page
                let k = pair.key
                let {items=items; blk=blk; overflows=overflows; nextGeneration=nextGeneration} = st
                // assert st.sofar > 0
                let thisPageNumber = blk.firstPage
                // TODO needing to reverse the items list here is rather unfortunate
                buildParentPage (List.rev items) pagenum overflows
                let nextBlk =
                    if isRootNode then
                        pb.SetPageFlag(FLAG_ROOT_NODE)
                        pb.SetSecondToLastInt32(firstLeaf)
                        pb.SetLastInt32(lastLeaf)
                        PageBlock(thisPageNumber+1,blk.lastPage)
                    else
                        if (blk.firstPage = blk.lastPage) then
                            pb.SetPageFlag(FLAG_BOUNDARY_NODE)
                            let newBlk = pageManager.GetBlock(token)
                            pb.SetLastInt32(newBlk.firstPage)
                            newBlk
                        else
                            PageBlock(thisPageNumber+1,blk.lastPage)
                pb.Write(fs)
                if nextBlk.firstPage <> (thisPageNumber+1) then utils.SeekPage(fs, pageSize, nextBlk.firstPage)
                {sofar=0; items=[]; blk=nextBlk; overflows=Map.empty; nextGeneration=pgitem(thisPageNumber,k)::nextGeneration}

            let foldParent (pair:pgitem) st =
                let pagenum = pair.page
                let k = pair.key

                let neededEitherWay = 1 + Varint.SpaceNeededFor (int64 k.Length) + Varint.SpaceNeededFor (int64 pagenum)
                let neededForInline = neededEitherWay + k.Length
                let neededForOverflow = neededEitherWay + sizeof<int32>
                let couldBeRoot = (List.isEmpty st.nextGeneration)

                let maybeWriteParent st = 
                    let available = calcAvailable (st.sofar) couldBeRoot
                    let fitsInline = (available >= neededForInline)
                    let wouldFitInlineOnNextPage = ((pageSize - PARENT_PAGE_OVERHEAD) >= neededForInline)
                    let fitsOverflow = (available >= neededForOverflow)
                    let writeThisPage = (not fitsInline) && (wouldFitInlineOnNextPage || (not fitsOverflow))

                    if writeThisPage then
                        // assert sofar > 0
                        writeParentPage st pair false
                    else
                        st

                let initParent st = 
                    if st.sofar = 0 then
                        {st with sofar=PARENT_PAGE_OVERHEAD; items=[]; }
                    else
                        st

                let addKeyToParent st = 
                    let {sofar=sofar; items=items; blk=blk; overflows=overflows} = st
                    let stateWithK = {st with items=pair :: items}
                    if calcAvailable sofar (List.isEmpty st.nextGeneration) >= neededForInline then
                        {stateWithK with sofar=sofar + neededForInline}
                    else
                        let keyOverflowFirstPage = blk.firstPage
                        let _,newBlk = writeOverflow blk (new MemoryStream(k))
                        {stateWithK with sofar=sofar + neededForOverflow; blk=newBlk; overflows=overflows.Add(k,keyOverflowFirstPage)}


                // this is the body of the foldParent function
                maybeWriteParent st |> initParent |> addKeyToParent

            // this is the body of writeParentNodes
            // children is in reverse order.  so List.head children is actually the very last child.
            let lastChild = List.head children
            let initialState = {nextGeneration=[];sofar=0;items=[];blk=startingBlk;overflows=Map.empty}
            let middleState = List.foldBack foldParent (List.tail children) initialState 
            let isRootNode = (List.isEmpty middleState.nextGeneration)
            let finalState = writeParentPage middleState lastChild isRootNode
            let {blk=blk;nextGeneration=ng} = finalState
            (blk,ng)

        let rec writeOneLayerOfParentPages (blk:PageBlock) (children:pgitem list) :int32 =
            if children.Length > 1 then
                let (newBlk,newChildren) = writeParentNodes blk children
                writeOneLayerOfParentPages newBlk newChildren
            else
                children.[0].page

        let rootPage = writeOneLayerOfParentPages blkAfterLeaves leaves

        let g = pageManager.End(token, rootPage)
        printfn "wrote segment: %A" g
        (g,rootPage)

    type private myOverflowReadStream(_fs:Stream, pageSize:int, _firstPage:int, _len:int) =
        inherit Stream()
        let fs = _fs
        let len = _len
        let firstPage = _firstPage
        let buf:byte[] = Array.zeroCreate pageSize
        let mutable currentPage = firstPage
        let mutable sofarOverall = 0
        let mutable sofarThisPage = 0
        let mutable firstPageInBlock = 0
        let mutable offsetToLastPageInThisBlock = 0 // add to firstPageInBlock to get the last one
        let mutable countRegularDataPagesInBlock = 0       
        let mutable boundaryPageNumber = 0
        let mutable bytesOnThisPage = 0
        let mutable offsetOnThisPage = 0

        // TODO consider supporting seek

        let ReadPage() = 
            utils.SeekPage(fs, buf.Length, currentPage)
            utils.ReadFully(fs, buf, 0, buf.Length)
            // assert PageType is OVERFLOW
            sofarThisPage <- 0
            if currentPage = firstPageInBlock then
                bytesOnThisPage <- buf.Length - (2 + sizeof<int32>)
                offsetOnThisPage <- 2
            else if currentPage = boundaryPageNumber then
                bytesOnThisPage <- buf.Length - sizeof<int32>
                offsetOnThisPage <- 0
            else
                // assert currentPage > firstPageInBlock
                // assert currentPage < boundaryPageNumber OR boundaryPageNumber = 0
                bytesOnThisPage <- buf.Length
                offsetOnThisPage <- 0

        let GetInt32At(at) :int =
            let a0 = uint64 buf.[at+0]
            let a1 = uint64 buf.[at+1]
            let a2 = uint64 buf.[at+2]
            let a3 = uint64 buf.[at+3]
            let r = (a0 <<< 24) ||| (a1 <<< 16) ||| (a2 <<< 8) ||| (a3 <<< 0)
            // assert r fits in a 32 bit signed int
            int r

        let GetLastInt32() = GetInt32At(buf.Length - sizeof<int32>)
        let PageType() = (buf.[0])
        let CheckPageFlag(f) = 0uy <> (buf.[1] &&& f)

        let ReadFirstPage() =
            printfn "ReadFirstPage of overflow at currentPage: %d" currentPage
            firstPageInBlock <- currentPage
            ReadPage()
            if PageType() <> OVERFLOW_NODE then failwith "first overflow page has invalid page type"
            if CheckPageFlag(FLAG_BOUNDARY_NODE) then
                // first page landed on a boundary node
                // lastInt32 is the next page number, which we'll fetch later
                boundaryPageNumber <- currentPage
                offsetToLastPageInThisBlock <- 0
                countRegularDataPagesInBlock <- 0
            else 
                offsetToLastPageInThisBlock <- GetLastInt32()
                if CheckPageFlag(FLAG_ENDS_ON_BOUNDARY) then
                    boundaryPageNumber <- currentPage + offsetToLastPageInThisBlock
                    countRegularDataPagesInBlock <- offsetToLastPageInThisBlock - 1
                else
                    boundaryPageNumber <- 0
                    countRegularDataPagesInBlock <- offsetToLastPageInThisBlock

        do ReadFirstPage()

        override this.Length = int64 len
        override this.CanRead = sofarOverall < len // TODO always return true?

        override this.Read(ba,offset,wanted) =
            if sofarOverall >= len then
                0
            else    
                let mutable direct = false
                if (sofarThisPage >= bytesOnThisPage) then
                    if currentPage = boundaryPageNumber then
                        currentPage <- GetLastInt32()
                        ReadFirstPage()
                    else
                        // we need a new page.  and if it's a full data page,
                        // and if wanted is big enough to take all of it, then
                        // we want to read (at least) it directly into the
                        // buffer provided by the caller.  we already know
                        // this candidate page cannot be the first page in a
                        // block.
                        let maybeDataPage = currentPage + 1
                        let isDataPage = 
                            if boundaryPageNumber > 0 then
                                ((len - sofarOverall) >= buf.Length) && (countRegularDataPagesInBlock > 0) && (maybeDataPage > firstPageInBlock) && (maybeDataPage < boundaryPageNumber)
                            else
                                ((len - sofarOverall) >= buf.Length) && (countRegularDataPagesInBlock > 0) && (maybeDataPage > firstPageInBlock) && (maybeDataPage <= (firstPageInBlock + countRegularDataPagesInBlock))

                        if isDataPage && (wanted >= buf.Length) then
                            // assert (currentPage + 1) > firstPageInBlock
                            //
                            // don't increment currentPage here because below, we will
                            // calculate how many pages we actually want to do.
                            direct <- true
                            bytesOnThisPage <- buf.Length
                            sofarThisPage <- 0
                            offsetOnThisPage <- 0
                        else
                            currentPage <- currentPage + 1
                            ReadPage()

                if direct then
                    // currentPage has not been incremented yet
                    //
                    // skip the buffer.  note, therefore, that the contents of the
                    // buffer are "invalid" in that they do not correspond to currentPage
                    //
                    let numPagesWanted = wanted / buf.Length
                    // assert countRegularDataPagesInBlock > 0
                    let lastDataPageInThisBlock = firstPageInBlock + countRegularDataPagesInBlock 
                    let theDataPage = currentPage + 1
                    let numPagesAvailable = 
                        if boundaryPageNumber>0 then 
                            boundaryPageNumber - theDataPage 
                        else 
                            lastDataPageInThisBlock - theDataPage + 1
                    let numPagesToFetch = Math.Min(numPagesWanted, numPagesAvailable)
                    let bytesToFetch = numPagesToFetch * buf.Length
                    // assert bytesToFetch <= wanted

                    utils.SeekPage(fs, buf.Length, theDataPage)
                    utils.ReadFully(fs, ba, offset, bytesToFetch)
                    sofarOverall <- sofarOverall + bytesToFetch
                    currentPage <- currentPage + numPagesToFetch
                    sofarThisPage <- buf.Length
                    bytesToFetch
                else
                    let available = Math.Min (bytesOnThisPage - sofarThisPage, len - sofarOverall)
                    let num = Math.Min (available, wanted)
                    System.Array.Copy (buf, offsetOnThisPage + sofarThisPage, ba, offset, num)
                    sofarOverall <- sofarOverall + num
                    sofarThisPage <- sofarThisPage + num
                    num

        override this.CanSeek = false
        override this.Seek(_,_) = raise (NotSupportedException())
        override this.Position
            with get() = int64 sofarOverall
            and set(v) = this.Seek(v, SeekOrigin.Begin) |> ignore

        override this.CanWrite = false
        override this.SetLength(_) = raise (NotSupportedException())
        override this.Flush() = raise (NotSupportedException())
        override this.Write(_,_,_) = raise (NotSupportedException())

    let private readOverflow len fs pageSize (firstPage:int) =
        let ostrm = new myOverflowReadStream(fs, pageSize, firstPage, len)
        utils.ReadAll(ostrm)

    type private myCursor(_fs:Stream, pageSize:int, _rootPage:int, _hook:Action<ICursor>) =
        let fs = _fs
        let rootPage = _rootPage
        let pr = PageReader(pageSize)
        let hook = _hook

        let mutable currentPage:int = 0
        let mutable leafKeys:int[] = null
        let mutable countLeafKeys = 0 // only realloc leafKeys when it's too small
        let mutable previousLeaf:int = 0
        let mutable currentKey = -1
        let mutable prefix:byte[] = null

        let resetLeaf() =
            countLeafKeys <- 0
            previousLeaf <- 0
            currentKey <- -1
            prefix <- null

        let setCurrentPage (pagenum:int) = 
            // TODO consider passing a block list for the segment into this
            // cursor so that the code here can detect if it tries to stray
            // out of bounds.

            // TODO if currentPage = pagenum already...
            currentPage <- pagenum
            resetLeaf()
            if 0 = currentPage then 
                false
            else
                // refuse to go to a page beyond the end of the stream
                // TODO is this the right place for this check?    
                let pos = ((int64 currentPage) - 1L) * int64 pr.PageSize
                if pos + int64 pr.PageSize <= fs.Length then
                    utils.SeekPage(fs, pr.PageSize, currentPage)
                    pr.Read(fs)
                    true
                else
                    false

        let getFirstAndLastLeaf() = 
            if not (setCurrentPage rootPage) then failwith "failed to read root page"
            if pr.PageType = LEAF_NODE then
                (rootPage, rootPage)
            else if pr.PageType = PARENT_NODE then
                if not (pr.CheckPageFlag(FLAG_ROOT_NODE)) then failwith "root page lacks flag"
                let first = pr.GetSecondToLastInt32()
                let last = pr.GetLastInt32()
                (first, last)
            else failwith "root page has invalid page type"
              
        let (firstLeaf, lastLeaf) = getFirstAndLastLeaf()

        let nextInLeaf() =
            if (currentKey+1) < countLeafKeys then
                currentKey <- currentKey + 1
                true
            else
                false

        let prevInLeaf() =
            if (currentKey > 0) then
                currentKey <- currentKey - 1
                true
            else
                false

        let skipKey() =
            let kflag = pr.GetByte()
            let klen = pr.GetVarint()
            if 0uy = (kflag &&& FLAG_OVERFLOW) then
                pr.Skip(int klen - if null=prefix then 0 else prefix.Length)
            else
                pr.Skip(sizeof<int32>)

        let skipValue() =
            let vflag = pr.GetByte()
            if 0uy <> (vflag &&& FLAG_TOMBSTONE) then ()
            else 
                let vlen = pr.GetVarint()
                if 0uy <> (vflag &&& FLAG_OVERFLOW) then pr.Skip(sizeof<int32>)
                else pr.Skip(int vlen)

        let readLeaf() =
            resetLeaf()
            pr.Reset()
            if pr.GetByte() <> LEAF_NODE then failwith "leaf has invalid page type"
            pr.GetByte() |> ignore
            previousLeaf <- pr.GetInt32()
            let prefixLen = pr.GetByte() |> int
            if prefixLen > 0 then
                prefix <- pr.GetArray(prefixLen)
            else
                prefix <- null
            countLeafKeys <- pr.GetInt16() |> int
            // only realloc leafKeys if it's too small
            if leafKeys=null || leafKeys.Length<countLeafKeys then
                leafKeys <- Array.zeroCreate countLeafKeys
            for i in 0 .. (countLeafKeys-1) do
                leafKeys.[i] <- pr.Position
                skipKey()
                skipValue()

        let keyInLeaf n = 
            pr.SetPosition(leafKeys.[n])
            let kflag = pr.GetByte()
            let klen = pr.GetVarint() |> int
            if 0uy = (kflag &&& FLAG_OVERFLOW) then
                if prefix <> null then
                    let prefixLen = prefix.Length
                    let res:byte[] = Array.zeroCreate klen
                    System.Array.Copy(prefix, 0, res, 0, prefixLen)
                    pr.GetIntoArray(res, prefixLen, klen - prefixLen)
                    res
                else
                    pr.GetArray(klen) // TODO consider alloc here and use GetIntoArray
            else
                let pagenum = pr.GetInt32()
                let k = readOverflow klen fs pr.PageSize pagenum
                k

        let compareKeyInLeaf n other = 
            pr.SetPosition(leafKeys.[n])
            let kflag = pr.GetByte()
            let klen = pr.GetVarint() |> int
            if 0uy = (kflag &&& FLAG_OVERFLOW) then
                if prefix <> null then
                    pr.CompareWithPrefix(prefix, klen, other)
                else
                    pr.Compare(klen, other)
            else
                // TODO this could be more efficient. we could compare the key
                // in place in the overflow without fetching the entire thing.

                // TODO overflowed keys are not prefixed.  should they be?
                let pagenum = pr.GetInt32()
                let k = readOverflow klen fs pr.PageSize pagenum
                bcmp.Compare k other
           
        let rec searchLeaf k min max sop le ge = 
            if max < min then
                match sop with
                | SeekOp.SEEK_EQ -> -1
                | SeekOp.SEEK_LE -> le
                | _ -> ge
            else
                let mid = (max + min) / 2
                let cmp = compareKeyInLeaf mid k
                if 0 = cmp then mid
                else if cmp<0  then searchLeaf k (mid+1) max sop mid ge
                else searchLeaf k min (mid-1) sop le mid

        let readParentPage() =
            pr.Reset()
            if pr.GetByte() <> PARENT_NODE then failwith "parent page has invalid page type"
            pr.Skip(1) // page flags
            let count = pr.GetInt16()
            let ptrs:int[] = Array.zeroCreate (int (count+1))
            let keys:byte[][] = Array.zeroCreate (int count)
            for i in 0 .. int count do
                ptrs.[i] <-  pr.GetVarint() |> int
            for i in 0 .. int (count-1) do
                let kflag = pr.GetByte()
                let klen = pr.GetVarint() |> int
                if 0uy = (kflag &&& FLAG_OVERFLOW) then
                    keys.[i] <- pr.GetArray(klen)
                else
                    let pagenum = pr.GetInt32()
                    keys.[i] <- readOverflow klen fs pr.PageSize pagenum
            (ptrs,keys)

        // this is used when moving forward through the leaf pages.
        // we need to skip any overflows.  when moving backward,
        // this is not necessary, because each leaf has a pointer to
        // the leaf before it.
        let rec searchForwardForLeaf() = 
            printfn "searchForwardForLeaf: currentPage: %d" currentPage
            let pt = pr.PageType
            if pt = LEAF_NODE then 
                printfn "found leaf"
                true
            else if pt = PARENT_NODE then 
                // if we bump into a parent node, that means there are
                // no more leaves.
                printfn "found parent"
                false
            else
                let lastInt32 = pr.GetLastInt32()
                //
                // an overflow page has a value in its LastInt32 which
                // is one of two things.
                //
                // if it's a boundary node, it's the page number of the
                // next page in the segment.
                //
                // otherwise, it's the number of pages to skip ahead.
                // this skip might take us to whatever follows this
                // overflow (which could be a leaf or a parent or
                // another overflow), or it might just take us to a
                // boundary page (in the case where the overflow didn't
                // fit).  it doesn't matter.  we just skip ahead.
                //
                if pr.CheckPageFlag(FLAG_BOUNDARY_NODE) then
                    printfn "searchForwardForLeaf: boundary, jump to: %d" lastInt32
                    if setCurrentPage (lastInt32) then
                        printfn "    ok"
                        searchForwardForLeaf()
                    else
                        printfn "    fail"
                        false
                else 
                    let lastPage = currentPage + lastInt32
                    let endsOnBoundary = pr.CheckPageFlag(FLAG_ENDS_ON_BOUNDARY)
                    printfn "searchForwardForLeaf: lastPage: %d  endsOnBoundary: %b" lastPage endsOnBoundary
                    if endsOnBoundary then
                        if setCurrentPage lastPage then
                            printfn "    ok"
                            let next = pr.GetLastInt32()
                            printfn "    endsOnBoundary: next: %d" next
                            if setCurrentPage (next) then
                                searchForwardForLeaf()
                            else
                                false
                        else
                            printfn "    fail"
                            false
                    else
                        if setCurrentPage (lastPage + 1) then
                            printfn "    ok"
                            searchForwardForLeaf()
                        else
                            printfn "    fail"
                            false

        let leafIsValid() =
            let ok = (leafKeys <> null) && (countLeafKeys > 0) && (currentKey >= 0) && (currentKey < countLeafKeys)
            ok

        let rec searchInParentPage k (ptrs:int[]) (keys:byte[][]) (i:int) :int =
            // TODO linear search?  really?
            if i < keys.Length then
                let cmp = bcmp.Compare k (keys.[int i])
                if cmp>0 then
                    searchInParentPage k ptrs keys (i+1)
                else
                    ptrs.[int i]
            else 0

        let rec search pg k sop =
            if setCurrentPage pg then
                if LEAF_NODE = pr.PageType then
                    readLeaf()
                    currentKey <- searchLeaf k 0 (countLeafKeys - 1) sop -1 -1
                    if SeekOp.SEEK_EQ <> sop then
                        if not (leafIsValid()) then
                            // if LE or GE failed on a given page, we might need
                            // to look at the next/prev leaf.
                            if SeekOp.SEEK_GE = sop then
                                let nextPage =
                                    if pr.CheckPageFlag(FLAG_BOUNDARY_NODE) then pr.GetLastInt32()
                                    else if currentPage = rootPage then 0
                                    else currentPage + 1
                                if (setCurrentPage (nextPage) && searchForwardForLeaf ()) then
                                    readLeaf()
                                    currentKey <- 0
                            else
                                if 0 = previousLeaf then
                                    resetLeaf()
                                else if setCurrentPage previousLeaf then
                                    readLeaf()
                                    currentKey <- countLeafKeys - 1
                else if PARENT_NODE = pr.PageType then
                    let (ptrs,keys) = readParentPage()
                    let found = searchInParentPage k ptrs keys 0
                    if 0 = found then
                        search ptrs.[ptrs.Length - 1] k sop
                    else
                        search found k sop

        let dispose itIsSafeToAlsoFreeManagedObjects this =
            if itIsSafeToAlsoFreeManagedObjects then
                if hook <> null then 
                    let fsHook = FuncConvert.ToFSharpFunc hook
                    fsHook(this)

        override this.Finalize() =
            dispose false this

        interface ICursor with
            member this.Dispose() =
                dispose true this
                GC.SuppressFinalize(this)

            member this.IsValid() =
                leafIsValid()

            member this.Seek(k,sop) =
                search rootPage k sop

            member this.Key() =
                keyInLeaf currentKey
            
            member this.Value() =
                pr.SetPosition(leafKeys.[currentKey])

                skipKey()

                let vflag = pr.GetByte()
                if 0uy <> (vflag &&& FLAG_TOMBSTONE) then null
                else 
                    let vlen = pr.GetVarint() |> int
                    if 0uy <> (vflag &&& FLAG_OVERFLOW) then 
                        let pagenum = pr.GetInt32()
                        new myOverflowReadStream(fs, pr.PageSize, pagenum, vlen) :> Stream
                    else 
                        new MemoryStream(pr.GetArray (vlen)) :> Stream

            member this.ValueLength() =
                pr.SetPosition(leafKeys.[currentKey])

                skipKey()

                let vflag = pr.GetByte()
                if 0uy <> (vflag &&& FLAG_TOMBSTONE) then -1
                else
                    let vlen = pr.GetVarint() |> int
                    vlen

            member this.KeyCompare(k) =
                compareKeyInLeaf currentKey k

            member this.First() =
                if setCurrentPage firstLeaf then
                    readLeaf()
                    currentKey <- 0

            member this.Last() =
                if setCurrentPage lastLeaf then
                    readLeaf()
                    currentKey <- countLeafKeys - 1

            member this.Next() =
                if not (nextInLeaf()) then
                    let nextPage =
                        if pr.CheckPageFlag(FLAG_BOUNDARY_NODE) then pr.GetLastInt32()
                        else if pr.PageType = LEAF_NODE then
                            if currentPage = rootPage then 0
                            else currentPage + 1
                        else 0
                    if setCurrentPage (nextPage) && searchForwardForLeaf() then
                        readLeaf()
                        currentKey <- 0

            member this.Prev() =
                if not (prevInLeaf()) then
                    if 0 = previousLeaf then
                        resetLeaf()
                    else if setCurrentPage previousLeaf then
                        readLeaf()
                        currentKey <- countLeafKeys - 1
    
    let OpenCursor(fs, pageSize:int, rootPage:int, hook:Action<ICursor>) :ICursor =
        new myCursor(fs, pageSize, rootPage, hook) :> ICursor

[<AbstractClass;Sealed>]
type BTreeSegment =
    static member CreateFromSortedSequence(fs:Stream, pageManager:IPages, source:seq<kvp>) = 
        bt.CreateFromSortedSequenceOfKeyValuePairs (fs, pageManager, source)

    #if not
    static member CreateFromSortedSequence(fs:Stream, pageManager:IPages, pairs:seq<byte[]*Stream>, mess:string) = 
        let source = seq { for t in pairs do yield kvp(fst t,snd t) done }
        bt.CreateFromSortedSequenceOfKeyValuePairs (fs, pageManager, source)
    #endif

    static member SortAndCreate(fs:Stream, pageManager:IPages, pairs:System.Collections.Generic.IDictionary<byte[],Stream>) =
#if not
        let keys:byte[][] = (Array.ofSeq pairs.Keys)
        let sortfunc x y = bcmp.Compare x y
        Array.sortInPlaceWith sortfunc keys
        let sortedSeq = seq { for k in keys do yield kvp(k,pairs.[k]) done }
#else
        // TODO which is faster?  how does linq OrderBy implement sorting
        // of a sequence?
        // http://code.logos.com/blog/2010/04/a_truly_lazy_orderby_in_linq.html
        let sortedSeq = pairs.AsEnumerable().OrderBy((fun (x:kvp) -> x.Key), ByteComparer())
#endif
        bt.CreateFromSortedSequenceOfKeyValuePairs (fs, pageManager, sortedSeq)

    #if not
    static member SortAndCreate(fs:Stream, pageManager:IPages, pairs:Map<byte[],Stream>) =
        let keys:byte[][] = pairs |> Map.toSeq |> Seq.map fst |> Array.ofSeq
        let sortfunc x y = bcmp.Compare x y
        Array.sortInPlaceWith sortfunc keys
        let sortedSeq = seq { for k in keys do yield kvp(k,pairs.[k]) done }
        bt.CreateFromSortedSequenceOfKeyValuePairs (fs, pageManager, sortedSeq)
    #endif

    static member OpenCursor(fs, pageSize:int, rootPage:int, hook:Action<ICursor>) :ICursor =
        bt.OpenCursor(fs,pageSize,rootPage,hook)


type dbf(_path) = 
    let path = _path

    // TODO this code should move elsewhere, since this file wants to be a PCL

    interface IDatabaseFile with
        member this.OpenForWriting() =
            new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite) :> Stream
        member this.OpenForReading() =
            new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite) :> Stream


type private HeaderData =
    {
        // TODO currentState is an ordered copy of segments.Keys.  eliminate duplication?
        // or add assertions and tests to make sure they never get out of sync?
        currentState: Guid list
        segments: Map<Guid,SegmentInfo>
        headerOverflow: PageBlock option
        changeCounter: int64
        mergeCounter: int64
    }

type private PendingSegment() =
    let mutable blockList:PageBlock list = []
    interface IPendingSegment
    member this.AddBlock((b:PageBlock)) =
        if (not (List.isEmpty blockList)) && (b.firstPage = (List.head blockList).lastPage+1) then
            // note that by consolidating blocks here, the segment info list will
            // not have information about the fact that the two blocks were
            // originally separate.  that's okay, since all we care about here is
            // keeping track of which pages are used.  but the btree code itself
            // is still treating the last page of the first block as a boundary
            // page, even though its pointer to the next block goes to the very
            // next page, because its page manager happened to give it a block
            // which immediately follows the one it had.
            blockList <- PageBlock((List.head blockList).firstPage, b.lastPage) :: blockList.Tail
        else
            blockList <- b :: blockList
    member this.End(lastPage) =
        let lastBlock = List.head blockList
        let unused = 
            if lastPage < lastBlock.lastPage then
                blockList <- PageBlock(lastBlock.firstPage, lastPage) :: (List.tail blockList)
                Some (PageBlock(lastPage+1, lastBlock.lastPage))
            else
                None
        (Guid.NewGuid(), blockList, unused)


// used for testing purposes
type SimplePageManager(_pageSize) =
    let pageSize = _pageSize

    let WASTE_PAGES_AFTER_EACH_BLOCK = 3
    let PAGES_PER_BLOCK = 10

    let critSectionNextPage = obj()
    let mutable nextPage = 1

    let getBlock num =
        lock critSectionNextPage (fun () -> 
            let blk = PageBlock(nextPage, nextPage+num-1) 
            nextPage <- nextPage + num + WASTE_PAGES_AFTER_EACH_BLOCK
            blk
            )

    interface IPages with
        member this.PageSize = pageSize

        member this.Begin() = PendingSegment() :> IPendingSegment

        // note that we assume that a single pending segment is going
        // to be written by a single thread.  the concrete PendingSegment
        // class above is not threadsafe.

        member this.GetBlock(token) =
            let ps = token :?> PendingSegment
            let blk = getBlock PAGES_PER_BLOCK
            ps.AddBlock(blk)
            blk

        member this.End(token, lastPage) =
            let ps = token :?> PendingSegment
            let (g,_,_) = ps.End(lastPage)
            g

type Database(_io:IDatabaseFile, _settings:DbSettings) =
    let io = _io
    let settings = _settings
    let fsMine = io.OpenForWriting()

    let HEADER_SIZE_IN_BYTES = 4096

    let readHeader() =
        let read() =
            if fsMine.Length >= (HEADER_SIZE_IN_BYTES |> int64) then
                let pr = PageReader(HEADER_SIZE_IN_BYTES)
                pr.Read(fsMine)
                Some pr
            else
                None

        let parse (pr:PageReader) =
            let readSegmentList (pr:PageReader) =
                let readBlockList (prBlocks:PageReader) =
                    let rec f more cur =
                        if more > 0 then
                            let firstPage = prBlocks.GetVarint() |> int
                            let countPages = prBlocks.GetVarint() |> int
                            // blocks are stored as firstPage/count rather than as
                            // firstPage/lastPage, because the count will always be
                            // smaller as a varint
                            f (more-1) (PageBlock(firstPage,firstPage + countPages - 1) :: cur)
                        else
                            cur

                    let count = prBlocks.GetVarint() |> int
                    f count []

                let count = pr.GetVarint() |> int
                let a:Guid[] = Array.zeroCreate count
                let fldr acc i = 
                    let g = Guid(pr.GetArray(16))
                    a.[i] <- g
                    let root = pr.GetVarint() |> int
                    let age = pr.GetVarint() |> int
                    let blocks = readBlockList(pr)
                    let info = {root=root;age=age;blocks=blocks}
                    Map.add g info acc
                let b = List.fold fldr Map.empty [0 .. count-1]
                (List.ofArray a,b)

            // --------

            let pageSize = pr.GetInt32()
            let changeCounter = pr.GetVarint()
            let mergeCounter = pr.GetVarint()
            let lenSegmentList = pr.GetVarint() |> int

            let overflowed = pr.GetByte()
            let (prSegmentList, blk) = 
                if overflowed <> 0uy then
                    let lenChunk1 = pr.GetInt32()
                    let lenChunk2 = lenSegmentList - lenChunk1
                    let firstPageChunk2 = pr.GetInt32()
                    let extraPages = lenChunk2 / pageSize + if (lenChunk2 % pageSize) <> 0 then 1 else 0
                    let lastPageChunk2 = firstPageChunk2 + extraPages - 1
                    let pr2 = PageReader(lenSegmentList)
                    // copy from chunk1 into pr2
                    pr2.Read(fsMine, 0, lenChunk1)
                    // now get chunk2 and copy it in as well
                    utils.SeekPage(fsMine, pageSize, firstPageChunk2)
                    pr2.Read(fsMine, lenChunk1, lenChunk2)
                    (pr2, Some (PageBlock(firstPageChunk2, lastPageChunk2)))
                else
                    (pr, None)

            let (state,segments) = readSegmentList(prSegmentList)

            let hd = 
                {
                    currentState=state 
                    segments=segments
                    headerOverflow=blk
                    changeCounter=changeCounter
                    mergeCounter=mergeCounter
                }

            (hd, pageSize)

        let calcNextPage pageSize (len:int64) =
            let numPagesSoFar = if (int64 pageSize) > len then 1 else (int (len / (int64 pageSize)))
            numPagesSoFar + 1

        // --------

        fsMine.Seek(0L, SeekOrigin.Begin) |> ignore
        let hdr = read()
        match hdr with
            | Some pr ->
                let (h, pageSize) = parse pr
                let nextAvailablePage = calcNextPage pageSize fsMine.Length
                (h, pageSize, nextAvailablePage)
            | None ->
                let defaultPageSize = settings.DefaultPageSize
                let h = 
                    {
                        segments = Map.empty
                        currentState = []
                        headerOverflow = None
                        changeCounter = 0L
                        mergeCounter = 0L
                    }
                let nextAvailablePage = calcNextPage defaultPageSize (int64 HEADER_SIZE_IN_BYTES)
                (h, defaultPageSize, nextAvailablePage)

    let (firstReadOfHeader,pageSize,firstAvailablePage) = readHeader()

    let mutable header = firstReadOfHeader
    let mutable nextPage = firstAvailablePage
    let mutable segmentsInWaiting: Map<Guid,SegmentInfo> = Map.empty

    let consolidateBlockList blocks =
        let sortedBlocks = List.sortBy (fun (x:PageBlock) -> x.firstPage) blocks
        let fldr acc (t:PageBlock) =
            let (blk:PageBlock, pile) = acc
            if blk.lastPage + 1 = t.firstPage then
                (PageBlock(blk.firstPage, t.lastPage), pile)
            else
                (PageBlock(t.firstPage, t.lastPage), blk :: pile)
        let folded = List.fold fldr (List.head sortedBlocks, []) (List.tail sortedBlocks)
        let consolidated = (fst folded) :: (snd folded)
        consolidated

    let invertBlockList blocks =
        let sortedBlocks = List.sortBy (fun (x:PageBlock) -> x.firstPage) blocks
        let fldr acc (t:PageBlock) =
            let (prev:PageBlock, result) = acc
            (t, PageBlock(prev.lastPage+1, t.firstPage-1) :: result)
        let folded = List.fold fldr (List.head sortedBlocks, []) (List.tail sortedBlocks)
        // if is being used to calculate free blocks, it won't find any free
        // blocks between the last used page and the end of the file
        snd folded

    let listAllBlocks h =
        let headerBlock = PageBlock(1, HEADER_SIZE_IN_BYTES / pageSize)
        let currentBlocks = Map.fold (fun acc _ info -> info.blocks @ acc) [] h.segments
        let inWaitingBlocks = Map.fold (fun acc _ info -> info.blocks @ acc) [] segmentsInWaiting
        let segmentBlocks = headerBlock :: currentBlocks @ inWaitingBlocks
        match h.headerOverflow with
            | Some blk ->
                blk :: segmentBlocks
            | None ->
                segmentBlocks

    let initialFreeBlocks = header |> listAllBlocks |> consolidateBlockList |> invertBlockList |> List.sortBy (fun (x:PageBlock) -> -(x.CountPages)) 
    //do printfn "initialFreeBlocks: %A" initialFreeBlocks

    let mutable freeBlocks:PageBlock list = initialFreeBlocks

    let critSectionNextPage = obj()
    let getBlock specificSize =
        lock critSectionNextPage (fun () -> 
            if specificSize > 0 then
                if List.isEmpty freeBlocks || specificSize > (List.head freeBlocks).CountPages then
                    let newBlk = PageBlock(nextPage, nextPage+specificSize-1) 
                    nextPage <- nextPage + specificSize
                    //printfn "newBlk: %A" newBlk
                    newBlk
                else
                    let headBlk = List.head freeBlocks
                    if headBlk.CountPages > specificSize then
                        // trim the block to size
                        let blk2 = PageBlock(headBlk.firstPage, headBlk.firstPage+specificSize-1) 
                        let remainder = PageBlock(headBlk.firstPage+specificSize, headBlk.lastPage)
                        freeBlocks <- remainder :: List.tail freeBlocks
                        // TODO problem: the list is probably no longer sorted.  is this okay?
                        // is a re-sort of the list really worth it?
                        //printfn "reusing blk prune: %A, specificSize:%d, freeBlocks now: %A" headBlk specificSize freeBlocks
                        blk2
                    else
                        freeBlocks <- List.tail freeBlocks
                        //printfn "reusing blk: %A, specificSize:%d, freeBlocks now: %A" headBlk specificSize freeBlocks
                        //printfn "blk.CountPages: %d" (headBlk.CountPages)
                        headBlk
            else
                let size = settings.PagesPerBlock
                let newBlk = PageBlock(nextPage, nextPage+size-1) 
                nextPage <- nextPage + size
                //printfn "newBlk: %A" newBlk
                newBlk
            )

    let addFreeBlocks blocks =
        // this code should not be called in a release build.  it helps
        // finds problems by zeroing out pages in blocks that
        // have been freed.
        let stomp() =
            let bad:byte[] = Array.zeroCreate pageSize
            use fs = io.OpenForWriting()
            List.iter (fun (b:PageBlock) ->
                //printfn "stomping on block: %A" b
                for x in b.firstPage .. b.lastPage do
                    utils.SeekPage(fs, pageSize, x)
                    fs.Write(bad,0,pageSize)
                ) blocks

        //stomp() // TODO remove.  or maybe a setting?  probably not.

        lock critSectionNextPage (fun () ->
            // all additions to the freeBlocks list should happen here
            // by calling this function.
            //
            // the list is kept consolidated and sorted by size descending.
            // unfortunately this requires two sorts, and they happen here
            // inside a critical section.  but the benefit is considered
            // worth the trouble.
            
            // TODO it is important that freeBlocks contains no overlaps.
            // add debug-only checks to verify?

            // TODO is there such a thing as a block that is so small we
            // don't want to bother with it?  what about a single-page block?
            // should this be a configurable setting?

            // TODO if the last block of the file is free, consider just
            // moving nextPage back.

            let newList = freeBlocks @ blocks |> consolidateBlockList
            let sorted = List.sortBy (fun (x:PageBlock) -> -(x.CountPages)) newList
            freeBlocks <- sorted
        )
        //printfn "freeBlocks: %A" freeBlocks
        #if not
        printfn "lastPage: %d" (nextPage-1)
        let c1 = header |> listAllBlocks |> consolidateBlockList
        printfn "usedBlocks: %A" c1
        let c2 = c1 |> invertBlockList
        printfn "inverted: %A" c2
        let c3 = List.sortBy (fun (x:PageBlock) -> -(x.CountPages)) c2
        printfn "calc: %A" c3
        printfn ""
        #endif

    let critSectionCursors = obj()
    let mutable cursors:Map<Guid,ICursor list> = Map.empty

    let getCursor segs g fnFree =
        let seg = Map.find g segs
        let rootPage = seg.root
        let fs = io.OpenForReading()
        let hook (csr:ICursor) =
            fs.Close()
            lock critSectionCursors (fun () -> 
                let cur = Map.find g cursors
                let removed = List.filter (fun x -> not (Object.ReferenceEquals(csr, x))) cur
                // if we are removing the last cursor for a segment, we do need to
                // remove that segment guid from the cursors map, not just leave
                // it there with an empty list.
                if List.isEmpty removed then
                    cursors <- Map.remove g cursors
                    match fnFree with
                    | Some f -> f g seg
                    | None -> ()
                else
                    cursors <- Map.add g removed cursors
            )
            //printfn "done with cursor %O" g 
        let csr = BTreeSegment.OpenCursor(fs, pageSize, rootPage, Action<ICursor>(hook))
        // note that getCursor is (and must be) only called from within
        // lock critSectionCursors
        let cur = match Map.tryFind g cursors with
                   | Some c -> c
                   | None -> []
        cursors <- Map.add g (csr :: cur) cursors
        //printfn "added cursor %O: %A" g seg
        csr

    let checkForGoneSegment g seg =
        if not (Map.containsKey g header.segments) then
            // this segment no longer exists
            //printfn "cursor done, segment %O is gone: %A" g seg
            addFreeBlocks seg.blocks

    let critSectionSegmentsInWaiting = obj()

    let pageManager = 
        { new IPages with
            member this.PageSize = pageSize

            member this.Begin() = PendingSegment() :> IPendingSegment

            // note that we assume that a single pending segment is going
            // to be written by a single thread.  the concrete PendingSegment
            // class above is not threadsafe.

            member this.GetBlock(token) =
                let ps = token :?> PendingSegment
                let blk = getBlock 0 // specificSize=0 means we don't care how big of a block we get
                ps.AddBlock(blk)
                blk

            member this.End(token, lastPage) =
                let ps = token :?> PendingSegment
                let (g,blocks,unused) = ps.End(lastPage)
                let info = {age=(-1);blocks=blocks;root=lastPage}
                lock critSectionSegmentsInWaiting (fun () -> 
                    segmentsInWaiting <- Map.add g info segmentsInWaiting
                )
                //printfn "wrote %A: %A" g blocks
                match unused with
                | Some b -> addFreeBlocks [ b ]
                | None -> ()
                g
        }

    let critSectionHeader = obj()

    // a stored segmentinfo for a segment is a single blob of bytes.
    // root page
    // age
    // number of pairs
    // each pair is startBlock,countBlocks
    // all in varints

    let writeHeader hdr =
        let spaceNeededForSegmentInfo (info:SegmentInfo) =
            let a = List.sumBy (fun (t:PageBlock) -> Varint.SpaceNeededFor(t.firstPage |> int64) + Varint.SpaceNeededFor(t.CountPages |> int64)) info.blocks
            let b = Varint.SpaceNeededFor(info.root |> int64)
            let c = Varint.SpaceNeededFor(info.age |> int64)
            let d = Varint.SpaceNeededFor(List.length info.blocks |> int64)
            a + b + c + d

        let spaceForHeader h =
            Varint.SpaceNeededFor(List.length h.currentState |> int64) 
                + List.sumBy (fun g -> (Map.find g h.segments |> spaceNeededForSegmentInfo) + 16) h.currentState

        let buildSegmentList h =
            let space = spaceForHeader h
            let pb = PageBuilder(space)
            // TODO format version number
            pb.PutVarint(List.length h.currentState |> int64)
            List.iter (fun (g:Guid) -> 
                pb.PutArray(g.ToByteArray())
                let info = Map.find g h.segments
                pb.PutVarint(info.root |> int64)
                pb.PutVarint(info.age |> int64)
                pb.PutVarint(List.length info.blocks |> int64)
                // we store PageBlock as first/count instead of first/last, since the
                // count will always compress better as a varint.
                List.iter (fun (t:PageBlock) -> pb.PutVarint(t.firstPage |> int64); pb.PutVarint(t.CountPages |> int64);) info.blocks
                ) h.currentState
            //if 0 <> pb.Available then failwith "not exactly full"
            pb.Buffer

        let pb = PageBuilder(HEADER_SIZE_IN_BYTES)
        pb.PutInt32(pageSize)

        pb.PutVarint(hdr.changeCounter)
        pb.PutVarint(hdr.mergeCounter)

        let buf = buildSegmentList hdr
        pb.PutVarint(buf.Length |> int64)

        let headerOverflow =
            if (pb.Available >= (buf.Length + 1)) then
                pb.PutByte(0uy)
                pb.PutArray(buf)
                None
            else
                pb.PutByte(1uy)
                let fits = pb.Available - 4 - 4
                let extra = buf.Length - fits
                let extraPages = extra / pageSize + if (extra % pageSize) <> 0 then 1 else 0
                //printfn "extra pages: %d" extraPages
                let blk = getBlock (extraPages)
                utils.SeekPage(fsMine, pageSize, blk.firstPage)
                fsMine.Write(buf, fits, extra)
                pb.PutInt32(fits)
                pb.PutInt32(blk.firstPage)
                pb.PutArray(buf, 0, fits)
                Some blk

        fsMine.Seek(0L, SeekOrigin.Begin) |> ignore
        pb.Write(fsMine)
        fsMine.Flush()
        {hdr with headerOverflow=headerOverflow}

    let critSectionMerging = obj()
    // this keeps track of which segments are currently involved in a merge.
    // a segment can only be in one merge at a time.  in effect, this is a list
    // of merge locks for segments.  segments should be removed from this set
    // after the merge has been committed.
    let mutable merging = Set.empty

    let critSectionPendingMerges = obj()
    // this keeps track of merges which have been written but not
    // yet committed.
    let mutable pendingMerges:Map<Guid,Guid list> = Map.empty

    let tryMerge segs =
        let requestMerge () =
            lock critSectionMerging (fun () ->
                let want = Set.ofSeq segs
                let already = Set.intersect want merging
                if Set.isEmpty already then
                    merging <- Set.union merging want
                    true
                else
                    false
            )

        let merge () = 
            // TODO this is silly if segs has only one item in it
            printfn "merge getting cursors: %A" segs
            let clist = lock critSectionCursors (fun () ->
                let h = header
                List.map (fun g -> getCursor h.segments g (Some checkForGoneSegment)) segs
            )
            List.iter (fun csr ->
                let count = CursorUtils.CountKeysForward(csr)
                printfn "    subcursor count: %d" count
                ) clist
            use mc = MultiCursor.Create clist
            let mcCount = CursorUtils.CountKeysForward(mc)
            let pairs = CursorUtils.ToSortedSequenceOfKeyValuePairs mc
            use fs = io.OpenForWriting()
            let (g,rootPage) = BTreeSegment.CreateFromSortedSequence(fs, pageManager, pairs)
            use csrNew = BTreeSegment.OpenCursor(fs, pageSize, rootPage, null)
            printfn "multicursor count: %d" mcCount
            printfn "csrNew count forwards: %d" (CursorUtils.CountKeysForward(csrNew))
            printfn "csrNew count backwards: %d" (CursorUtils.CountKeysBackward(csrNew))
            g

        let storePendingMerge g =
            lock critSectionPendingMerges (fun () ->
                pendingMerges <- Map.add g segs pendingMerges
                // TODO assert segs are in merging set?
            )

        if requestMerge () then
            //printfn "requestMerge Some"
            let later() = 
                //printfn "inside later"
                let g = merge ()
                storePendingMerge g
                g
            Some later
        else
            //printfn "requestMerge None"
            None

    let removePendingMerge g =
        let doneMerge segs =
            lock critSectionMerging (fun () ->
                let removing = Set.ofSeq segs
                // TODO assert is subset?
                merging <- Set.difference merging removing
            )

        let segs = Map.find g pendingMerges
        doneMerge segs
        lock critSectionPendingMerges (fun () ->
            pendingMerges <- Map.remove g pendingMerges
        )

    // only call this if you have the writeLock
    let commitMerge (newGuid:Guid) =
        // TODO we could check to see if this guid is already in the list.

        let lstOld = Map.find newGuid pendingMerges
        let countOld = List.length lstOld                                         
        let oldGuidsAsSet = List.fold (fun acc g -> Set.add g acc) Set.empty lstOld
        let lstAges = List.map (fun g -> (Map.find g header.segments).age) lstOld
        let age = 1 + List.max lstAges

        let segmentsBeingReplaced = Set.fold (fun acc g -> Map.add g (Map.find g header.segments) acc ) Map.empty oldGuidsAsSet

        let oldHeaderOverflow = lock critSectionHeader (fun () -> 
            let ndxFirstOld = List.findIndex (fun g -> g=List.head lstOld) header.currentState
            let subListOld = List.skip ndxFirstOld header.currentState |> List.take countOld
            // if the next line fails, it probably means that somebody tried to merge a set
            // of segments that are not contiguous in currentState.
            if lstOld <> subListOld then failwith (sprintf "segments not found: lstOld = %A  currentState = %A" lstOld header.currentState)
            let before = List.take ndxFirstOld header.currentState
            let after = List.skip (ndxFirstOld + countOld) header.currentState
            let newState = before @ (newGuid :: after)
            let segmentsWithoutOld = Map.filter (fun g _ -> not (Set.contains g oldGuidsAsSet)) header.segments
            let newSegmentInfo = Map.find newGuid segmentsInWaiting
            let newSegments = Map.add newGuid {newSegmentInfo with age=age} segmentsWithoutOld
            let newHeaderBeforeWriting = {
                changeCounter=header.changeCounter
                mergeCounter=header.mergeCounter + 1L
                currentState=newState 
                segments=newSegments
                headerOverflow=None
                }
            let newHeader = writeHeader newHeaderBeforeWriting
            let oldHeaderOverflow = header.headerOverflow
            header <- newHeader
            oldHeaderOverflow
        )
        removePendingMerge newGuid
        // the segment we just committed can now be removed from
        // the segments in waiting list
        lock critSectionSegmentsInWaiting (fun () ->
            segmentsInWaiting <- Map.remove newGuid segmentsInWaiting
        )
        //printfn "segmentsBeingReplaced: %A" segmentsBeingReplaced
        // don't free blocks from any segment which still has a cursor
        lock critSectionCursors (fun () -> 
            let segmentsToBeFreed = Map.filter (fun g _ -> not (Map.containsKey g cursors)) segmentsBeingReplaced
            //printfn "oldGuidsAsSet: %A" oldGuidsAsSet
            let blocksToBeFreed = Seq.fold (fun acc info -> info.blocks @ acc) List.empty (Map.values segmentsToBeFreed)
            match oldHeaderOverflow with
            | Some blk ->
                let blocksToBeFreed' = PageBlock(blk.firstPage, blk.lastPage) :: blocksToBeFreed
                addFreeBlocks blocksToBeFreed'
            | None ->
                addFreeBlocks blocksToBeFreed
        )
        // note that we intentionally do not release the writeLock here.
        // you can change the segment list more than once while holding
        // the writeLock.  the writeLock gets released when you Dispose() it.

    // only call this if you have the writeLock
    let commitSegments (newGuids:seq<Guid>) fnHook =
        // TODO we could check to see if this guid is already in the list.

        let newGuidsAsSet = Seq.fold (fun acc g -> Set.add g acc) Set.empty newGuids

        let mySegmentsInWaiting = Map.filter (fun g _ -> Set.contains g newGuidsAsSet) segmentsInWaiting
        //printfn "committing: %A" mySegmentsInWaiting
        let oldHeaderOverflow = lock critSectionHeader (fun () -> 
            let newState = (List.ofSeq newGuids) @ header.currentState
            let newSegments = Map.fold (fun acc g info -> Map.add g {info with age=0} acc) header.segments mySegmentsInWaiting
            let newHeaderBeforeWriting = {
                changeCounter=header.changeCounter + 1L
                mergeCounter=header.mergeCounter
                currentState=newState
                segments=newSegments
                headerOverflow=None
                }
            let newHeader = writeHeader newHeaderBeforeWriting
            let oldHeaderOverflow = header.headerOverflow
            header <- newHeader
            oldHeaderOverflow
        )
        //printfn "after commit, currentState: %A" header.currentState
        //printfn "after commit, segments: %A" header.segments
        // all the segments we just committed can now be removed from
        // the segments in waiting list
        lock critSectionSegmentsInWaiting (fun () ->
            let remainingSegmentsInWaiting = Map.filter (fun g _ -> Set.contains g newGuidsAsSet |> not) segmentsInWaiting
            segmentsInWaiting <- remainingSegmentsInWaiting
        )
        match oldHeaderOverflow with
        | Some blk -> addFreeBlocks [ PageBlock(blk.firstPage, blk.lastPage) ]
        | None -> ()
        // note that we intentionally do not release the writeLock here.
        // you can change the segment list more than once while holding
        // the writeLock.  the writeLock gets released when you Dispose() it.

        match fnHook with
        | Some f -> f()
        | None -> ()

    let critSectionInTransaction = obj()
    let mutable inTransaction = false 
    let mutable waiting = Deque.empty

    let getWriteLock front timeout fnCommitSegmentsHook =
        let whence = Environment.StackTrace // TODO remove this.  it was just for debugging.
        let createWriteLockObject () =
            let isReleased = ref false
            let release() =
                isReleased := true
                let next = lock critSectionInTransaction (fun () ->
                    if Deque.isEmpty waiting then
                        //printfn "nobody waiting. tx done"
                        inTransaction <- false
                        None
                    else
                        //printfn "queue has %d waiting.  next." (Queue.length waiting)
                        let f = Deque.head waiting
                        waiting <- Deque.tail waiting
                        //printfn "giving writeLock to next"
                        Some f
                )
                match next with
                | Some f ->
                    f()
                    //printfn "done giving writeLock to next"
                | None -> ()
            {
            new System.Object() with
                override this.Finalize() =
                    let already = !isReleased
                    if not already then failwith (sprintf "a writelock must be explicitly disposed: %s" whence)

            interface IWriteLock with
                member this.Dispose() =
                    let already = !isReleased
                    if already then failwith "only dispose a writelock once"
                    release()
                    GC.SuppressFinalize(this)

                member this.CommitMerge(g:Guid) =
                    let already = !isReleased
                    if already then failwith "don't use a writelock after you dispose it"
                    commitMerge g
                    // note that we intentionally do not release the writeLock here.
                    // you can change the segment list more than once while holding
                    // the writeLock.  the writeLock gets released when you Dispose() it.

                member this.CommitSegments(newGuids:seq<Guid>) =
                    let already = !isReleased
                    if already then failwith "don't use a writelock after you dispose it"
                    commitSegments newGuids fnCommitSegmentsHook
                    // note that we intentionally do not release the writeLock here.
                    // you can change the segment list more than once while holding
                    // the writeLock.  the writeLock gets released when you Dispose() it.
            }

        lock critSectionInTransaction (fun () -> 
            if inTransaction then 
                let ev = new System.Threading.ManualResetEventSlim()
                let cb () = ev.Set()
                if front then
                    waiting <- Deque.cons cb waiting
                else
                    waiting <- Deque.conj cb waiting
                //printfn "Add to wait list: %O" whence
                async {
                    let! b = Async.AwaitWaitHandle(ev.WaitHandle, timeout)
                    ev.Dispose()
                    if b then
                        let lck = createWriteLockObject () 
                        return lck
                    else
                        return failwith "timeout waiting for write lock"
                }
            else 
                //printfn "No waiting: %O" whence
                inTransaction <- true
                async { 
                    let lck = createWriteLockObject () 
                    return lck
                }
            )

    let getPossibleMerge level min all =
        let h = header
        let segmentsOfAge = List.filter (fun g -> (Map.find g h.segments).age=level) h.currentState
        // TODO it would be nice to be able to have more than one merge happening in a level

        // TODO we are trusting segmentsOfAge to be contiguous.  need test cases to
        // verify that currentState always ends up with monotonically increasing age.
        let count = List.length segmentsOfAge
        if count > min then 
            //printfn "NEED MERGE %d -- %d" level count
            // (List.skip) we always merge the stuff at the end of the level so things
            // don't get split up when more segments get prepended to the
            // beginning.
            // TODO if we only do partial here, we might want to schedule a job to do more.
            let grp = if all then segmentsOfAge else List.skip (count - min) segmentsOfAge
            tryMerge grp
        else
            //printfn "no merge needed %d -- %d" level count
            None

    let wrapMergeForLater f = async {
        let g = f()
        //printfn "now waiting for writeLock"
        // merges go to the front of the queue
        use! tx = getWriteLock true (-1) None
        tx.CommitMerge g
        return [ g ]
    }

    let critSectionBackgroundMergeJobs = obj()
    let mutable backgroundMergeJobs = List.empty

    let startBackgroundMergeJob f =
        //printfn "starting background job"
        // TODO this is starving.
        async {
            //printfn "inside start background job"
            let! completor = Async.StartChild f
            lock critSectionBackgroundMergeJobs (fun () -> 
                backgroundMergeJobs <- completor :: backgroundMergeJobs 
                )
            //printfn "inside start background job step 2"
            let! result = completor
            //printfn "inside start background job step 3"
            ignore result
            lock critSectionBackgroundMergeJobs (fun () -> 
                backgroundMergeJobs <- List.filter (fun x -> not (Object.ReferenceEquals(x,completor))) backgroundMergeJobs
                )
        } |> Async.Start

    let doAutoMerge() = 
        if settings.AutoMergeEnabled then
            for level in 0 .. 3 do // TODO max merge level immediate
                match getPossibleMerge level settings.AutoMergeMinimumPages false with
                | Some f -> 
                    let g = f()
                    commitMerge g
                | None -> 
                    () // printfn "cannot merge level %d" level
            for level in 4 .. 7 do // TODO max merge level
                match getPossibleMerge level settings.AutoMergeMinimumPages false with
                | Some f -> 
                    f |> wrapMergeForLater |> startBackgroundMergeJob
                | None -> 
                    () // printfn "cannot merge level %d" level

    let dispose itIsSafeToAlsoFreeManagedObjects =
        //let blocks = consolidateBlockList header
        //printfn "%A" blocks
        if itIsSafeToAlsoFreeManagedObjects then
            // we don't want to close fsMine until all background jobs
            // are completed.
            let bg = backgroundMergeJobs
            if not (List.isEmpty bg) then
                bg |> Async.Parallel |> Async.RunSynchronously |> ignore

            fsMine.Close()

    static member DefaultSettings = 
        {
            AutoMergeEnabled = true
            AutoMergeMinimumPages = 4
            DefaultPageSize = 256
            PagesPerBlock = 10
        }

    new(_io:IDatabaseFile) =
        new Database(_io, Database.DefaultSettings)

    override this.Finalize() =
        dispose false

    interface IDatabase with
        member this.Dispose() =
            dispose true
            // TODO what happens if there are open cursors?
            // we could throw.  but why?  maybe we should just
            // let them live until they're done.  does the db
            // object care?  this would be more tricky if we were
            // pooling and reusing read streams.  similar issues
            // for background writes as well.
            GC.SuppressFinalize(this)

        member this.WriteSegmentFromSortedSequence(pairs:seq<kvp>) =
            use fs = io.OpenForWriting()
            let (g,_) = BTreeSegment.CreateFromSortedSequence(fs, pageManager, pairs)
            g

        member this.WriteSegment(pairs:System.Collections.Generic.IDictionary<byte[],Stream>) =
            use fs = io.OpenForWriting()
            let (g,_) = BTreeSegment.SortAndCreate(fs, pageManager, pairs)
            g

        member this.Merge(level:int, howMany:int, all:bool) =
            let maybe = getPossibleMerge level howMany all
            match maybe with
            | Some f ->
                let blk = wrapMergeForLater f
                Some blk
            | None -> 
                None

        member this.BackgroundMergeJobs() = 
            backgroundMergeJobs

        member this.ForgetWaitingSegments(guids:seq<Guid>) =
            // TODO need a test case for this
            let guidsAsSet = Seq.fold (fun acc g -> Set.add g acc) Set.empty guids
            let mySegmentsInWaiting = Map.filter (fun g _ -> Set.contains g guidsAsSet) segmentsInWaiting
            lock critSectionSegmentsInWaiting (fun () ->
                let remainingSegmentsInWaiting = Map.filter (fun g _ -> Set.contains g guidsAsSet |> not) segmentsInWaiting
                segmentsInWaiting <- remainingSegmentsInWaiting
            )
            lock critSectionCursors (fun () -> 
                let segmentsToBeFreed = Map.filter (fun g _ -> not (Map.containsKey g cursors)) mySegmentsInWaiting
                let blocksToBeFreed = Seq.fold (fun acc info -> info.blocks @ acc) List.empty (Map.values segmentsToBeFreed)
                addFreeBlocks blocksToBeFreed
            )

        member this.OpenCursor() =
            // TODO this cursor needs to expose the changeCounter and segment list
            // on which it is based. for optimistic writes. caller can grab a cursor,
            // do their writes, then grab the writelock, and grab another cursor, then
            // compare the two cursors to see if anything important changed.  if not,
            // commit their writes.  if so, nevermind the written segments and start over.

            // TODO we also need a way to open a cursor on segments in waiting
            let clist = lock critSectionCursors (fun () ->
                let h = header
                List.map (fun g -> getCursor h.segments g (Some checkForGoneSegment)) h.currentState
            )
            let mc = MultiCursor.Create clist
            LivingCursor.Create mc

        member this.OpenSegmentCursor(g:Guid) =
            let csr = lock critSectionCursors (fun () ->
                let h = header
                getCursor h.segments g (Some checkForGoneSegment)
            )
            csr

        member this.ListSegments() =
            (header.currentState, header.segments)

        member this.RequestWriteLock(timeout:int) =
            // TODO need a test case for this
            getWriteLock false timeout (Some doAutoMerge)

        member this.RequestWriteLock() =
            getWriteLock false (-1) (Some doAutoMerge)

