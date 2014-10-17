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

namespace Zumero.LSM.fs

open System
open System.IO

open Zumero.LSM

module utils =
    // TODO why aren't the functions here in utils using curried params?

    let SeekPage(strm:Stream, pageSize, pageNumber) =
        if 0 = pageNumber then raise (new Exception())
        let pos = ((int64 pageNumber) - 1L) * int64 pageSize
        let newpos = strm.Seek(pos, SeekOrigin.Begin)
        if pos <> newpos then raise (new Exception())

    let ReadFully(strm:Stream, buf, off, len) :unit =
        let rec fn sofar =
            if sofar < len then
                let got = strm.Read(buf, off + sofar, len - sofar)
                if 0 = got then raise (new Exception())
                fn (sofar + got)
        fn 0

    let ReadAll(strm:Stream) =
        let len = int (strm.Length - strm.Position)
        let buf:byte[] = Array.zeroCreate len
        ReadFully(strm, buf, 0, len)
        buf

(*
type SeekOp = SEEK_EQ=0 | SEEK_LE=1 | SEEK_GE=2

type ICursor =
    abstract member Seek : k:byte[] * sop:SeekOp -> unit
    abstract member First : unit -> unit
    abstract member Last : unit -> unit
    abstract member Next : unit -> unit
    abstract member Prev : unit -> unit
    abstract member IsValid : unit -> bool
    abstract member Key : unit -> byte[]
    abstract member Value : unit -> Stream
    abstract member ValueLength : unit -> int
    abstract member KeyCompare : k:byte[] -> int

type IWrite = 
    abstract member Insert: k:byte[] * s:Stream -> unit
    abstract member Delete: k:byte[] -> unit
    abstract member OpenCursor: unit -> ICursor
*)

module ByteComparer = 
    // this code is very non-F#-ish.  but it's much faster than the
    // idiomatic version which precedded it.

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

// TODO this could move into BTreeSegment.Create.
// and it doesn't need to be a class.
type private PageBuilder(pgsz:int) =
    let mutable cur = 0
    let buf:byte[] = Array.zeroCreate pgsz

    member this.Reset() = cur <- 0
    member this.Flush(s:Stream) = s.Write(buf, 0, buf.Length)
    member this.PageSize = buf.Length
    member this.Position = cur
    member this.Available = buf.Length - cur
    member this.SetPageFlag(x:byte) = buf.[1] <- buf.[1] ||| x

    member this.PutByte(x:byte) =
        buf.[cur] <- byte x
        cur <- cur+1

    member this.PutStream(s:Stream, len:int) =
        utils.ReadFully(s, buf, cur, len)
        cur <- cur+len

    member this.PutArray(ba:byte[]) =
        System.Array.Copy (ba, 0, buf, cur, ba.Length)
        cur <- cur + ba.Length

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
        if cur > buf.Length - 8 then raise (new Exception())
        this.PutInt32At(buf.Length - 2*sizeof<int32>, page)

    member this.SetLastInt32(page:int) = 
        if cur > buf.Length - 4 then raise (new Exception())
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
    member this.Reset() = cur <- 0
    member this.Compare(len, other) = ByteComparer.CompareWithin buf cur len other
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

type MemorySegment() =
    // TODO this will need to be an immutable collection so that
    // openCursor will see a snapshot while it may continue
    // being modified further.

    // TODO without using the ByteArrayComparer class, this is broken.
    let pairs = new System.Collections.Generic.Dictionary<byte[],Stream>()

     // Here in the F# version, the cursor is implemented as an object
     // expression instead of a private inner class, which F# does
     // not support.  Actually, I prefer this because the object
     // expression can access private class instance fields directly
     // whereas the nested class in C# cannot.

    let openCursor() = 
        // The following is a ref cell because mutable variables
        // cannot be captured by a closure.
        let cur = ref -1
        // TODO could this be a list?  but we need prev, and fs list
        // is a linked list in only one direction.  but once we
        // construct it, it is immutable for the life of this cursor,
        // so array isn't ideal either.
        let keys:byte[][] = (Array.ofSeq pairs.Keys)
        let sortfunc x y = ByteComparer.Compare x y
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
                let cmp = ByteComparer.Compare kmid k
                if 0 = cmp then mid
                else if cmp<0  then search k (mid+1) max sop mid ge
                else search k min (mid-1) sop le mid

        { new ICursor with
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
                ByteComparer.Compare (keys.[!cur]) k

            member this.Value() =
                // TODO the pairs array in the IWrite may have changed
                let v = pairs.[keys.[!cur]]
                if v <> null then ignore (v.Seek(0L, SeekOrigin.Begin))
                v

            member this.ValueLength() =
                // TODO the pairs array in the IWrite may have changed
                let v = pairs.[keys.[!cur]]
                if v <> null then (int v.Length) else -1

            member this.Seek (k, sop) =
                cur := search k 0 (keys.Length-1) sop -1 -1
        }

    interface IWrite with
        member this.Insert (k:byte[], s:Stream) =
            pairs.[k] <- s

        member this.Delete (k:byte[]) =
            pairs.[k] <- null

        member this.OpenCursor() =
            // note that this opens a cursor just for this memory segment.
            openCursor()

    static member Create() :IWrite =
        upcast (new MemorySegment())

type private Direction = FORWARD=0 | BACKWARD=1 | WANDERING=2

type MultiCursor private (_subcursors:seq<ICursor>) =
    let subcursors = List.ofSeq _subcursors
    let mutable cur:ICursor option = None
    let mutable dir = Direction.WANDERING

    let validSorted sortfunc = 
        let valids = List.filter (fun (csr:ICursor) -> csr.IsValid()) subcursors
        let sorted = List.sortWith sortfunc valids
        sorted

    let find sortfunc = 
        let vs = validSorted sortfunc
        if vs.IsEmpty then None
        else Some vs.Head

    let findMin() = 
        let sortfunc (a:ICursor) (b:ICursor) = a.KeyCompare(b.Key())
        find sortfunc
    
    let findMax() = 
        let sortfunc (a:ICursor) (b:ICursor) = b.KeyCompare(a.Key())
        find sortfunc

    static member Create(_subcursors:seq<ICursor>) :ICursor =
        upcast (MultiCursor _subcursors)
               
    static member Create([<ParamArray>] _subcursors: ICursor[]) :ICursor =
        upcast (MultiCursor _subcursors)
               
    interface ICursor with
        member this.IsValid() = 
            match cur with
            | Some csr -> csr.IsValid()
            | None -> false

        member this.First() =
            let f (x:ICursor) = x.First()
            List.iter f subcursors
            cur <- findMin()

        member this.Last() =
            let f (x:ICursor) = x.Last()
            List.iter f subcursors
            cur <- findMax()

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

type LivingCursor(ch:ICursor) =
    let chain = ch

    let skipTombstonesForward() =
        while (chain.IsValid() && (chain.ValueLength() < 0)) do
            chain.Next()

    let skipTombstonesBackward() =
        while (chain.IsValid() && (chain.ValueLength() < 0)) do
            chain.Prev()

    interface ICursor with
        member this.First() = 
            chain.First()
            skipTombstonesForward()

        member this.Last() = 
            chain.Last()
            skipTombstonesBackward()

        member this.Key() = chain.Key()
        member this.Value() = chain.Value()
        member this.ValueLength() = chain.ValueLength()
        member this.IsValid() = chain.IsValid() && (chain.ValueLength() > 0)
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

module BTreeSegment =
    // page types
    let private LEAF_NODE:byte = 1uy
    let private PARENT_NODE:byte = 2uy
    let private OVERFLOW_NODE:byte = 3uy

    // flags on values
    // TODO enum
    let private FLAG_OVERFLOW:byte = 1uy
    let private FLAG_TOMBSTONE:byte = 2uy

    // flags on pages
    // TODO enum
    let private FLAG_ROOT_NODE:byte = 1uy
    let private FLAG_BOUNDARY_NODE:byte = 2uy
    let private FLAG_ENDS_ON_BOUNDARY:byte = 4uy


    type writeParentPagesState = 
        { 
            sofar:int; 
            items:(int32*byte[]) list; 
            nextPage:int; 
            boundaryPage:int; 
            //children:(int32*byte[]); 
            overflows:Map<byte[],int32>;
        }

    let Create(fs:Stream, pageManager:IPages, csr:ICursor) :int32 = 
        let pageSize = pageManager.PageSize
        let token = pageManager.Begin()
        let pbOverflow = new PageBuilder(pageSize)

        let writeOverflow startingNextPageNumber startingBoundaryPageNumber (ba:Stream) =
            let len = (int ba.Length)
            let pageSize = pbOverflow.PageSize

            let buildBoundaryPage sofar =
                pbOverflow.Reset()
                // check for the partial page at the end
                let num = Math.Min((pageSize - sizeof<int32>), (len - sofar))
                pbOverflow.PutStream(ba, num)
                // something will be put in lastInt32 before the page is written
                sofar + num

            let buildFirstPage sofar =
                pbOverflow.Reset()
                pbOverflow.PutByte(OVERFLOW_NODE)
                pbOverflow.PutByte(0uy) // starts 0, may be changed before the page is written
                // check for the partial page at the end
                let num = Math.Min((pageSize - (2 + sizeof<int32>)), (len - sofar))
                pbOverflow.PutStream(ba, num)
                // something will be put in lastInt32 before the page is written
                sofar + num

            let writeRegularPages numPagesToWrite _sofar =
                let rec fn i sofar =
                    if i < numPagesToWrite then
                        pbOverflow.Reset()
                        // check for the partial page at the end
                        let num = Math.Min(pageSize, (len - sofar))
                        pbOverflow.PutStream(ba, num)
                        pbOverflow.Flush(fs)
                        fn (i+1) (sofar + num)
                    else
                        sofar

                fn 0 _sofar

            let countRegularPagesNeeded siz = 
                let pages = siz / pageSize
                let extra = if (siz % pageSize) <> 0 then 1 else 0
                pages + extra

            let rec writeOneBlock sofarBeforeFirstPage firstPageNumber boundaryPageNumber :int*int =
                // each trip through this loop will write out one
                // block, starting with the overflow first page,
                // followed by zero-or-more "regular" overflow pages,
                // which have no header.  we'll stop at the block boundary,
                // either because we land there or because the whole overflow
                // won't fit and we have to continue into the next block.
                // the boundary page will be like a regular overflow page,
                // headerless, but it is four bytes smaller.
                if sofarBeforeFirstPage >= len then
                    (firstPageNumber, boundaryPageNumber)
                else
                    let sofarAfterFirstPage = buildFirstPage sofarBeforeFirstPage
                    // note that we haven't flushed this page yet.  we may have to fix
                    // a couple of things before it gets written out.
                    if firstPageNumber = boundaryPageNumber then
                        // the first page landed on a boundary
                        pbOverflow.SetPageFlag(FLAG_BOUNDARY_NODE)
                        let (nextPage,boundaryPage) = pageManager.GetRange(token)
                        pbOverflow.SetLastInt32(nextPage)
                        pbOverflow.Flush(fs)
                        utils.SeekPage(fs, pageSize, nextPage)
                        writeOneBlock sofarAfterFirstPage nextPage boundaryPage
                    else 
                        let firstRegularPageNumber = firstPageNumber + 1
                        // assert sofar <= len
                        if sofarAfterFirstPage = len then
                            // the first page is also the last one
                            pbOverflow.SetLastInt32(0) // number of regular pages following
                            pbOverflow.Flush(fs)
                            (firstRegularPageNumber,boundaryPageNumber)
                        else
                            // assert sofar < len

                            // needed gives us the number of pages, NOT including the first one
                            // which would be necessary to finish this overflow.
                            let needed = countRegularPagesNeeded (len - sofarAfterFirstPage)

                            // availableBeforeBoundary is the number of pages until the boundary,
                            // NOT counting the boundary page, and the first page in the block
                            // has already been accounted for, so we're just talking about data pages.
                            let availableBeforeBoundary = 
                                if boundaryPageNumber > 0 
                                then (boundaryPageNumber - firstRegularPageNumber) 
                                else needed

                            // if needed <= availableBeforeBoundary then this will fit

                            // if needed = (1 + availableBeforeBoundary) then this might fit, 
                            // depending on whether the loss of the 4 bytes on the boundary
                            // page makes a difference or not.  Either way, for this block,
                            // the overflow ends on the boundary

                            // if needed > (1 + availableBeforeBoundary), then this block will end 
                            // on the boundary, but it will continue

                            let numRegularPages = Math.Min(needed, availableBeforeBoundary)
                            pbOverflow.SetLastInt32(numRegularPages)

                            if needed > availableBeforeBoundary then
                                // this part of the overflow will end on the boundary,
                                // perhaps because it finishes exactly there, or perhaps
                                // because it doesn't fit and needs to continue into the
                                // next block.
                                pbOverflow.SetPageFlag(FLAG_ENDS_ON_BOUNDARY)

                            // now we can flush the first page
                            pbOverflow.Flush(fs)

                            // write out the regular pages.  these are full pages
                            // of data, with no header and no footer.  the last
                            // page actually might not be full, since it might be a
                            // partial page at the end of the overflow.  either way,
                            // these don't have a header, and they don't have a
                            // boundary ptr at the end.

                            let sofarAfterRegularPages = writeRegularPages numRegularPages sofarAfterFirstPage

                            if needed > availableBeforeBoundary then
                                // assert sofar < len
                                // assert nextPageNumber = boundaryPageNumber
                                //
                                // we need to write out a regular page with a
                                // boundary pointer in it.  if this is happening,
                                // then FLAG_ENDS_ON_BOUNDARY was set on the first
                                // overflow page in this block, since we can't set it
                                // here on this page, because this page has no header.
                                let sofarAfterBoundaryPage = buildBoundaryPage sofarAfterRegularPages
                                let (nextPage,boundaryPage) = pageManager.GetRange(token)
                                pbOverflow.SetLastInt32(nextPage)
                                pbOverflow.Flush(fs)
                                utils.SeekPage(fs, pageSize, nextPage)
                                writeOneBlock sofarAfterBoundaryPage nextPage boundaryPage
                            else
                                (firstRegularPageNumber + numRegularPages, boundaryPageNumber)

            writeOneBlock 0 startingNextPageNumber startingBoundaryPageNumber

        let pb = new PageBuilder(pageSize)
        let putKeyWithLength (k:byte[]) =
            pb.PutByte(0uy) // flags TODO are keys ever going to have flags?
            pb.PutVarint(int64 k.Length)
            pb.PutArray(k)

        let putOverflow strm nextPageNumber boundaryPageNumber =
            let overflowFirstPage = nextPageNumber
            let newRange = writeOverflow nextPageNumber boundaryPageNumber strm
            pb.PutByte(FLAG_OVERFLOW)
            pb.PutVarint(strm.Length)
            pb.PutInt32(overflowFirstPage)
            newRange

        let writeLeaves leavesFirstPage leavesBoundaryPage :int*int*System.Collections.Generic.List<int32 * byte[]> =
            // 2 for the page type and flags
            // 4 for the prev page
            // 2 for the stored count
            // 4 for lastInt32 (which isn't in pb.Available)
            let LEAF_PAGE_OVERHEAD = 2 + 4 + 2 + 4
            let OFFSET_COUNT_PAIRS = 6

            // TODO this wants to be an F# list
            let leaves = new System.Collections.Generic.List<int32 * byte[]>()

            let flushLeaf countPairs lastKey thisPageNumber boundaryPageNumber = 
                pb.PutInt16At (OFFSET_COUNT_PAIRS, countPairs)
                let isRootPage = not (csr.IsValid()) && (0 = leaves.Count)
                let range = 
                    if (not isRootPage) && (thisPageNumber = boundaryPageNumber) then
                        pb.SetPageFlag FLAG_BOUNDARY_NODE
                        let newRange = pageManager.GetRange(token)
                        // assert pb.Position <= (pageSize - 4)
                        pb.SetLastInt32(fst newRange)
                        newRange
                    else
                        (thisPageNumber + 1, boundaryPageNumber)
                pb.Flush(fs)
                pb.Reset()
                leaves.Add(thisPageNumber, lastKey)
                let nextPage = fst range
                if nextPage <> (thisPageNumber+1) then utils.SeekPage(fs, pageSize, nextPage)
                range

            let rec putKeys countPairs lastKey nextPageNumber boundaryPageNumber =
                let k = csr.Key()
                let v = csr.Value()
                // assert k <> null
                // but v might be null (a tombstone)

                let vlen = if v<>null then v.Length else int64 0

                let neededForOverflowPageNumber = sizeof<int32>

                let neededForKeyBase = 1 + Varint.SpaceNeededFor(int64 k.Length)
                let neededForKeyInline = neededForKeyBase + k.Length

                let neededForValueInline = 1 + if v<>null then Varint.SpaceNeededFor(int64 vlen) + int vlen else 0
                let neededForValueOverflow = 1 + if v<>null then Varint.SpaceNeededFor(int64 vlen) + neededForOverflowPageNumber else 0

                let neededForBothInline = neededForKeyInline + neededForValueInline
                let neededForKeyInlineValueOverflow = neededForKeyInline + neededForValueOverflow

                // determine if we need to flush this page
                let available = pb.Available - sizeof<int32> // for the lastInt32
                let fitBothInline = (available >= neededForBothInline)
                let wouldFitBothInlineOnNextPage = ((pageSize - LEAF_PAGE_OVERHEAD) >= neededForBothInline)
                let fitKeyInlineValueOverflow = (available >= neededForKeyInlineValueOverflow)
                let neededForKeyOverflow = neededForKeyBase + neededForOverflowPageNumber
                let neededForBothOverflow = neededForKeyOverflow + neededForValueOverflow
                let fitBothOverflow = (available >= neededForBothOverflow)
                let flushThisPage = (countPairs > 0) && (not fitBothInline) && (wouldFitBothInlineOnNextPage || ( (not fitKeyInlineValueOverflow) && (not fitBothOverflow) ) )

                if flushThisPage then
                    (countPairs,lastKey,nextPageNumber,boundaryPageNumber)
                else
                    let (newN, newB) = 
                        if fitBothInline then
                            putKeyWithLength k
                            if null = v then
                                pb.PutByte(FLAG_TOMBSTONE)
                                pb.PutVarint(0L)
                            else
                                pb.PutByte(0uy)
                                pb.PutVarint(int64 vlen)
                                pb.PutStream(v, int vlen)
                            (nextPageNumber,boundaryPageNumber)
                        else
                            // TODO is it possible for v to be a tombstone here?

                            if fitKeyInlineValueOverflow then
                                putKeyWithLength k

                                putOverflow v nextPageNumber boundaryPageNumber
                            else
                                let (n,b) = putOverflow (new MemoryStream(k)) nextPageNumber boundaryPageNumber
                                putOverflow v n b

                    csr.Next()
                    if csr.IsValid() then
                        putKeys (countPairs+1) k newN newB
                    else
                        (countPairs+1,k,newN,newB)

            let rec writeLeaf prevPageNumber nextPageNumber boundaryPageNumber =
                pb.PutByte(LEAF_NODE)
                pb.PutByte(0uy) // flags

                pb.PutInt32 (prevPageNumber) // prev page num.
                pb.PutInt16 (0) // number of pairs in this page. zero for now. written at end.
                // assert pb.Position is 8 (LEAF_PAGE_OVERHEAD - sizeof(lastInt32))

                let (countPairs,lastKey,n,b) = putKeys 0 null nextPageNumber boundaryPageNumber

                let thisPageNumber = n
                let (nextPage,boundaryPage) = flushLeaf countPairs lastKey n b

                if csr.IsValid() then
                    writeLeaf thisPageNumber nextPage boundaryPage
                else
                    (nextPage, boundaryPage)

            let (n,b) = writeLeaf 0 leavesFirstPage leavesBoundaryPage
            (n,b,leaves)

        let (startingPage,startingBoundary) = pageManager.GetRange(token)
        utils.SeekPage(fs, pageSize, startingPage)
        csr.First()
        let (pageAfterLeaves, boundaryAfterLeaves, leaves) = writeLeaves startingPage startingBoundary

        // all the leaves are written.
        // now write the parent pages.
        // maybe more than one level of them.
        // keep writing until we have written a level which has only one node,
        // which is the root node.
        if leaves.Count > 0 then
            let firstLeaf = fst leaves.[0]
            let lastLeaf = fst leaves.[leaves.Count-1]

            let writeParentNodes (children:System.Collections.Generic.List<int32 * byte[]>) startingPageNumber startingBoundaryPageNumber =
                // 2 for the page type and flags
                // 2 for the stored count
                // 5 for the extra ptr we will add at the end, a varint, 5 is worst case
                // 4 for lastInt32
                let PARENT_PAGE_OVERHEAD = 2 + 2 + 5 + 4

                let nextGeneration = new System.Collections.Generic.List<int32 * byte[]>()

                let calcAvailable currentSize couldBeRoot =
                    let basicSize = pageSize - currentSize
                    let allowanceForRootNode = if couldBeRoot then sizeof<int32> else 0 // first/last Leaf, lastInt32 already
                    basicSize - allowanceForRootNode

                let buildParentPage (items:(int32*byte[]) list) lastPtr (overflows:Map<byte[],int32>) =
                    pb.Reset ()
                    pb.PutByte (PARENT_NODE)
                    pb.PutByte (0uy)
                    pb.PutInt16 (items.Length)
                    // store all the ptrs, n+1 of them
                    List.iter (fun x -> pb.PutVarint(int64 (fst x))) items
                    pb.PutVarint(int64 lastPtr)
                    // store all the keys, n of them
                    let fn x = 
                        let k = snd x
                        match overflows.TryFind(k) with
                        | Some pg ->
                            pb.PutByte(FLAG_OVERFLOW)
                            pb.PutVarint(int64 k.Length)
                            pb.PutInt32(pg)
                        | None ->
                            putKeyWithLength k
                    List.iter fn items

                let flushParentPage args pair isRootNode =
                    let (pagenum,k:byte[]) = pair
                    let {items=items; nextPage=nextPageNumber; boundaryPage=boundaryPageNumber; overflows=overflows} = args
                    // assert args.sofar > 0
                    let thisPageNumber = nextPageNumber
                    // TODO needing to reverse the items list is rather unfortunate
                    buildParentPage (List.rev items) pagenum overflows
                    let (nextN,nextB) =
                        if isRootNode then
                            pb.SetPageFlag(FLAG_ROOT_NODE)
                            // assert pb.Position <= (pageSize - 8)
                            pb.SetSecondToLastInt32(firstLeaf)
                            pb.SetLastInt32(lastLeaf)
                            (thisPageNumber+1,boundaryPageNumber)
                        else
                            if (nextPageNumber = boundaryPageNumber) then
                                pb.SetPageFlag(FLAG_BOUNDARY_NODE)
                                let newRange = pageManager.GetRange(token)
                                pb.SetLastInt32(fst newRange)
                                newRange
                            else
                                (thisPageNumber+1,boundaryPageNumber)
                    pb.Flush(fs)
                    if nextN <> (thisPageNumber+1) then utils.SeekPage(fs, pageSize, nextN)
                    nextGeneration.Add(thisPageNumber, k)
                    {sofar=0; items=[]; nextPage=nextN; boundaryPage=nextB; overflows=Map.empty}

                let folder st pair =
                    let (pagenum,k:byte[]) = pair
                    let (i, _) = st

                    let neededEitherWay = 1 + Varint.SpaceNeededFor (int64 k.Length) + Varint.SpaceNeededFor (int64 pagenum)
                    let neededForInline = neededEitherWay + k.Length
                    let neededForOverflow = neededEitherWay + sizeof<int32>
                    let isLastChild = (i = (children.Count - 1))
                    let couldBeRoot = (nextGeneration.Count = 0)

                    let maybeFlush args = 
                        let available = calcAvailable (args.sofar) couldBeRoot
                        let fitsInline = (available >= neededForInline)
                        let wouldFitInlineOnNextPage = ((pageSize - PARENT_PAGE_OVERHEAD) >= neededForInline)
                        let fitsOverflow = (available >= neededForOverflow)
                        // TODO flush logic used to be guarded by if sofar > 0.  should it still be?
                        let flushThisPage = isLastChild || ((not fitsInline) && (wouldFitInlineOnNextPage || (not fitsOverflow))) 

                        if flushThisPage then
                            // assert sofar > 0
                            let isRootNode = isLastChild && couldBeRoot
                            flushParentPage args pair isRootNode
                        else
                            args

                    let initParent args = 
                        if isLastChild then 
                            args
                        else
                            let sofar = args.sofar
                            if sofar = 0 then
                                {args with sofar=PARENT_PAGE_OVERHEAD; items=[]; }
                            else
                                args

                    let addKeyToParent args = 
                        if isLastChild then 
                            args
                        else
                            let {sofar=sofar; items=items; nextPage=nextPageNumber; boundaryPage=boundaryPageNumber; overflows=overflows} = args
                            let argsWithK = {args with items=pair :: items}
                            if calcAvailable sofar (nextGeneration.Count = 0) >= neededForInline then
                                {argsWithK with sofar=sofar + neededForInline}
                            else
                                let keyOverflowFirstPage = nextPageNumber
                                let kRange = writeOverflow nextPageNumber boundaryPageNumber (new MemoryStream(k))
                                {argsWithK with sofar=sofar + neededForOverflow; nextPage=fst kRange; boundaryPage=snd kRange; overflows=overflows.Add(k,keyOverflowFirstPage)}


                    // this is the body of the folder function
                    let (_,args) = st
                    let args' = maybeFlush args |> initParent |> addKeyToParent
                    (i+1,args') 

                // TODO this would be much happier if children were already an F# list
                let fsChildren = List.ofSeq children
                let initialArgs = {sofar=0;items=[];nextPage=startingPageNumber;boundaryPage=startingBoundaryPageNumber;overflows=Map.empty}
                let initialState = (0,initialArgs)
                let finalState = List.fold folder initialState fsChildren
                let (_,{nextPage=n;boundaryPage=b}) = finalState
                (n,b,nextGeneration)

            let rec writeOneLayerOfParentPages next boundary (children:System.Collections.Generic.List<int32 * byte[]>) :int32 =
                if children.Count > 1 then
                    let (newNext,newBoundary,newChildren) = writeParentNodes children next boundary 
                    writeOneLayerOfParentPages newNext newBoundary newChildren
                else
                    fst children.[0]

            let rootPage = writeOneLayerOfParentPages pageAfterLeaves boundaryAfterLeaves leaves

            pageManager.End(token, rootPage)
            rootPage
        else
            pageManager.End(token, 0)
            0

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
        let mutable countRegularDataPagesInBlock = 0 // not counting the first, and not counting any boundary
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
            firstPageInBlock <- currentPage
            ReadPage()
            if PageType() <> OVERFLOW_NODE then
                raise (new Exception())
            if CheckPageFlag(FLAG_BOUNDARY_NODE) then
                // first page landed on a boundary node
                // lastInt32 is the next page number, which we'll fetch later
                boundaryPageNumber <- currentPage
                countRegularDataPagesInBlock <- 0
            else 
                countRegularDataPagesInBlock <- GetLastInt32()
                if CheckPageFlag(FLAG_ENDS_ON_BOUNDARY) then
                    boundaryPageNumber <- currentPage + countRegularDataPagesInBlock + 1
                else
                    boundaryPageNumber <- 0

        do ReadFirstPage()

        override this.Length = int64 len
        override this.CanRead = sofarOverall < len

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
        override this.CanWrite = false
        override this.SetLength(v) = raise (new NotSupportedException())
        override this.Flush() = raise (new NotSupportedException())
        override this.Seek(offset,origin) = raise (new NotSupportedException())
        override this.Write(buf,off,len) = raise (new NotSupportedException())
        override this.Position
            with get() = int64 sofarOverall
            and set(value) = raise (new NotSupportedException())

    let private readOverflow len fs pageSize (firstPage:int) =
        let ostrm = new myOverflowReadStream(fs, pageSize, firstPage, len)
        utils.ReadAll(ostrm)

    type private myCursor(_fs:Stream, pageSize:int, _rootPage:int) =
        let fs = _fs
        let rootPage = _rootPage
        let pr = new PageReader(pageSize)

        let mutable currentPage:int = 0
        let mutable leafKeys:int[] = null
        let mutable countLeafKeys = 0 // only realloc leafKeys when it's too small
        let mutable previousLeaf:int = 0
        let mutable currentKey = -1

        let resetLeaf() =
            countLeafKeys <- 0
            previousLeaf <- 0
            currentKey <- -1

        let setCurrentPage (pagenum:int) = 
            currentPage <- pagenum
            resetLeaf()
            if 0 = currentPage then false
            else                
                if pagenum <= rootPage then
                    utils.SeekPage(fs, pr.PageSize, currentPage)
                    pr.Read(fs)
                    true
                else
                    false
                    
        let getFirstAndLastLeaf() = 
            if not (setCurrentPage rootPage) then raise (new Exception())
            if pr.PageType = LEAF_NODE then
                (rootPage, rootPage)
            else if pr.PageType = PARENT_NODE then
                if not (pr.CheckPageFlag(FLAG_ROOT_NODE)) then
                    raise (new Exception())
                let first = pr.GetSecondToLastInt32()
                let last = pr.GetLastInt32()
                (first, last)
            else
                raise (new Exception())
              
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
                pr.Skip(int klen)
            else
                pr.Skip(sizeof<int32>)

        let skipValue() =
            let vflag = pr.GetByte()
            let vlen = pr.GetVarint()
            if 0uy <> (vflag &&& FLAG_TOMBSTONE) then ()
            else if 0uy <> (vflag &&& FLAG_OVERFLOW) then pr.Skip(sizeof<int32>)
            else pr.Skip(int vlen)

        let readLeaf() =
            resetLeaf()
            pr.Reset()
            if pr.GetByte() <> LEAF_NODE then 
                raise (new Exception())
            pr.GetByte() |> ignore
            previousLeaf <- pr.GetInt32()
            countLeafKeys <- pr.GetInt16() |> int
            // only realloc leafKeys if it's too small
            if leafKeys=null || leafKeys.Length<countLeafKeys then
                leafKeys <- Array.zeroCreate countLeafKeys
            for i in 0 .. (countLeafKeys-1) do
                leafKeys.[i] <- pr.Position
                skipKey()
                skipValue()

        let compareKeyInLeaf n other = 
            pr.SetPosition(leafKeys.[n])
            let kflag = pr.GetByte()
            let klen = pr.GetVarint() |> int
            if 0uy = (kflag &&& FLAG_OVERFLOW) then
                pr.Compare(klen, other)
            else
                let pagenum = pr.GetInt32()
                let k = readOverflow klen fs pr.PageSize pagenum
                ByteComparer.Compare k other

        let keyInLeaf n = 
            pr.SetPosition(leafKeys.[n])
            let kflag = pr.GetByte()
            let klen = pr.GetVarint() |> int
            if 0uy = (kflag &&& FLAG_OVERFLOW) then
                pr.GetArray(klen)
            else
                let pagenum = pr.GetInt32()
                let k = readOverflow klen fs pr.PageSize pagenum
                k

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
            if pr.GetByte() <> PARENT_NODE then 
                raise (new Exception())
            let pflag = pr.GetByte()
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
            let pt = pr.PageType
            if pt = LEAF_NODE then true
            else if pt = PARENT_NODE then 
                // if we bump into a parent node, that means there are
                // no more leaves.
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
                // overflow (which could be a leadf or a parent or
                // another overflow), or it might just take us to a
                // boundary page (in the case where the overflow didn't
                // fit).  it doesn't matter.  we just skip ahead.
                //
                if pr.CheckPageFlag(FLAG_BOUNDARY_NODE) then
                    if setCurrentPage (lastInt32) then
                        searchForwardForLeaf()
                    else
                        false
                else 
                    let endsOnBoundary = pr.CheckPageFlag(FLAG_ENDS_ON_BOUNDARY)
                    if setCurrentPage (currentPage + lastInt32) then
                        if endsOnBoundary then
                            let next = pr.GetLastInt32()
                            if setCurrentPage (next) then
                                searchForwardForLeaf()
                            else
                                false
                        else
                            searchForwardForLeaf()
                    else
                        false

        let leafIsValid() =
            let ok = (leafKeys <> null) && (countLeafKeys > 0) && (currentKey >= 0) && (currentKey < countLeafKeys)
            ok

        let rec searchInParentPage k (ptrs:int[]) (keys:byte[][]) (i:int) :int =
            // TODO linear search?  really?
            if i < keys.Length then
                let cmp = ByteComparer.Compare k (keys.[int i])
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

        interface ICursor with
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
                let vlen = pr.GetVarint() |> int
                if 0uy <> (vflag &&& FLAG_TOMBSTONE) then null
                else if 0uy <> (vflag &&& FLAG_OVERFLOW) then 
                    let pagenum = pr.GetInt32()
                    upcast (new myOverflowReadStream(fs, pr.PageSize, pagenum, vlen))
                else 
                    upcast (new MemoryStream(pr.GetArray (vlen)))

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
                        else currentPage + 1
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
    
    let OpenCursor(fs, pageSize:int, rootPage:int) :ICursor =
        upcast (new myCursor(fs, pageSize, rootPage))

