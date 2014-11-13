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
open System.Linq // used for OrderBy

open Zumero.LSM

type kvp = System.Collections.Generic.KeyValuePair<byte[],Stream>

type pgitem = 
    struct
        val page: int32
        val key: byte[]
        new (p,k) = { page = p; key = k; }
    end


module utils =
    // TODO why aren't the functions here in utils using curried params?

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
    member this.Reset() = cur <- 0
    member this.Compare(len, other) = bcmp.CompareWithin buf cur len other
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

    type private LeafState =
        // TODO considered making this a struct, but it doesn't make much/any perf difference
        {
            keys:(byte[] list)
            firstLeaf:int32
            leaves:pgitem list 
            blk:PageBlock
        }

    let CreateFromSortedSequenceOfKeyValuePairs(fs:Stream, pageManager:IPages, source:seq<kvp>) = 
        let pageSize = pageManager.PageSize
        let token = pageManager.Begin()
        let pbOverflow = PageBuilder(pageSize)

        let writeOverflow (startingBlk:PageBlock) (ba:Stream) =
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
                        pbOverflow.Write(fs)
                        fn (i+1) (sofar + num)
                    else
                        sofar

                fn 0 _sofar

            let countRegularPagesNeeded siz = 
                let pages = siz / pageSize
                let extra = if (siz % pageSize) <> 0 then 1 else 0
                pages + extra

            let rec writeOneBlock sofarBeforeFirstPage (firstBlk:PageBlock) :PageBlock =
                // each trip through this loop will write out one
                // block, starting with the overflow first page,
                // followed by zero-or-more "regular" overflow pages,
                // which have no header.  we'll stop at the block boundary,
                // either because we land there or because the whole overflow
                // won't fit and we have to continue into the next block.
                // the boundary page will be like a regular overflow page,
                // headerless, but it is four bytes smaller.
                if sofarBeforeFirstPage >= len then
                    firstBlk
                else
                    let sofarAfterFirstPage = buildFirstPage sofarBeforeFirstPage
                    // note that we haven't written this page yet.  we may have to fix
                    // a couple of things before it gets written out.
                    if firstBlk.firstPage = firstBlk.lastPage then
                        // the first page landed on a boundary
                        pbOverflow.SetPageFlag(FLAG_BOUNDARY_NODE)
                        let blk = pageManager.GetBlock(token)
                        pbOverflow.SetLastInt32(blk.firstPage)
                        pbOverflow.Write(fs)
                        utils.SeekPage(fs, pageSize, blk.firstPage)
                        writeOneBlock sofarAfterFirstPage blk
                    else 
                        let firstRegularPageNumber = firstBlk.firstPage + 1
                        // assert sofar <= len
                        if sofarAfterFirstPage = len then
                            // the first page is also the last one
                            pbOverflow.SetLastInt32(0) // number of regular pages following
                            pbOverflow.Write(fs)
                            PageBlock(firstRegularPageNumber,firstBlk.lastPage)
                        else
                            // assert sofar < len

                            // needed gives us the number of pages, NOT including the first one
                            // which would be necessary to finish this overflow.
                            let needed = countRegularPagesNeeded (len - sofarAfterFirstPage)

                            // availableBeforeBoundary is the number of pages until the boundary,
                            // NOT counting the boundary page, and the first page in the block
                            // has already been accounted for, so we're just talking about data pages.
                            let availableBeforeBoundary = 
                                if firstBlk.lastPage > 0 
                                then (firstBlk.lastPage - firstRegularPageNumber) 
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

                            // now we can write the first page
                            pbOverflow.Write(fs)

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
                                let blk = pageManager.GetBlock(token)
                                pbOverflow.SetLastInt32(blk.firstPage)
                                pbOverflow.Write(fs)
                                utils.SeekPage(fs, pageSize, blk.firstPage)
                                writeOneBlock sofarAfterBoundaryPage blk
                            else
                                PageBlock(firstRegularPageNumber + numRegularPages, firstBlk.lastPage)

            writeOneBlock 0 startingBlk

        let pb = PageBuilder(pageSize)
        let putKeyWithLength (k:byte[]) =
            pb.PutByte(0uy) // flags TODO are keys ever going to have flags?  prefix compression probably.
            pb.PutVarint(int64 k.Length)
            pb.PutArray(k)

        let putOverflow strm (blk:PageBlock) =
            let overflowFirstPage = blk.firstPage
            let newBlk = writeOverflow blk strm
            pb.PutByte(FLAG_OVERFLOW)
            pb.PutVarint(strm.Length)
            pb.PutInt32(overflowFirstPage)
            newBlk

        let writeLeaves (leavesBlk:PageBlock) :PageBlock*(pgitem list)*int =
            // 2 for the page type and flags
            // 4 for the prev page
            // 2 for the stored count
            // 4 for lastInt32 (which isn't in pb.Available)
            let LEAF_PAGE_OVERHEAD = 2 + 4 + 2 + 4
            let OFFSET_COUNT_PAIRS = 6

            let writeLeaf st isRootPage = 
                pb.PutInt16At (OFFSET_COUNT_PAIRS, st.keys.Length)
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
                pb.Reset()
                if nextBlk.firstPage <> (thisPageNumber+1) then utils.SeekPage(fs, pageSize, nextBlk.firstPage)
                {keys=[]; firstLeaf=firstLeaf; blk=nextBlk; leaves=pgitem(thisPageNumber,List.head st.keys)::st.leaves}

            let foldLeaf st (pair:kvp) = 
                let k = pair.Key
                let v = pair.Value
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

                let maybeWriteLeaf st = 
                    let available = pb.Available - sizeof<int32> // for the lastInt32
                    let fitBothInline = (available >= neededForBothInline)
                    let wouldFitBothInlineOnNextPage = ((pageSize - LEAF_PAGE_OVERHEAD) >= neededForBothInline)
                    let fitKeyInlineValueOverflow = (available >= neededForKeyInlineValueOverflow)
                    let neededForKeyOverflow = neededForKeyBase + neededForOverflowPageNumber
                    let neededForBothOverflow = neededForKeyOverflow + neededForValueOverflow
                    let fitBothOverflow = (available >= neededForBothOverflow)
                    let writeThisPage = (not (List.isEmpty st.keys)) && (not fitBothInline) && (wouldFitBothInlineOnNextPage || ( (not fitKeyInlineValueOverflow) && (not fitBothOverflow) ) )

                    if writeThisPage then
                        writeLeaf st false
                    else
                        st

                let initLeaf (st:LeafState) =
                    if pb.Position = 0 then
                        pb.PutByte(LEAF_NODE)
                        pb.PutByte(0uy) // flags

                        let prevPage = if List.isEmpty st.leaves then 0 else (List.head st.leaves).page
                        pb.PutInt32 (prevPage) // prev page num.
                        pb.PutInt16 (0) // number of pairs in this page. zero for now. written at end.
                        // assert pb.Position is 8 (LEAF_PAGE_OVERHEAD - sizeof(lastInt32))
                        {st with keys=[];}
                    else
                        st

                let addPairToLeaf (st:LeafState) =
                    let available = pb.Available - sizeof<int32> // for the lastInt32
                    let fitBothInline = (available >= neededForBothInline)
                    let fitKeyInlineValueOverflow = (available >= neededForKeyInlineValueOverflow)
                    let {keys=_;blk=blk} = st
                    let newBlk = 
                        if fitBothInline then
                            putKeyWithLength k
                            if null = v then
                                pb.PutByte(FLAG_TOMBSTONE)
                                pb.PutVarint(0L)
                            else
                                pb.PutByte(0uy)
                                pb.PutVarint(int64 vlen)
                                pb.PutStream(v, int vlen)
                            blk
                        else
                            // TODO is it possible for v to be a tombstone here?

                            if fitKeyInlineValueOverflow then
                                putKeyWithLength k

                                putOverflow v blk
                            else
                                let tmpBlk = putOverflow (new MemoryStream(k)) blk
                                putOverflow v tmpBlk
                    {st with blk=newBlk;keys=k::st.keys}
                        
                // this is the body of the foldLeaf function
                maybeWriteLeaf st |> initLeaf |> addPairToLeaf

            // this is the body of writeLeaves
            //let source = seq { csr.First(); while csr.IsValid() do yield (csr.Key(), csr.Value()); csr.Next(); done }
            let initialState = {firstLeaf=0;keys=[];leaves=[];blk=leavesBlk}
            let middleState = Seq.fold foldLeaf initialState source
            let finalState = 
                if not (List.isEmpty middleState.keys) then
                    let isRootNode = List.isEmpty middleState.leaves
                    writeLeaf middleState isRootNode
                else
                    middleState
            let {blk=blk;leaves=leaves;firstLeaf=firstLeaf} = finalState
            (blk,leaves,firstLeaf)

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
            // 5 for the extra ptr we will add at the end, a varint, 5 is worst case
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
                        putKeyWithLength k
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
                    // TODO this logic used to be guarded by if sofar > 0.  should it still be?
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
                        let newBlk = writeOverflow blk (new MemoryStream(k))
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
            if PageType() <> OVERFLOW_NODE then failwith "first overflow page has invalid page type"
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
        override this.SetLength(_) = raise (NotSupportedException())
        override this.Flush() = raise (NotSupportedException())
        override this.Seek(_,_) = raise (NotSupportedException())
        override this.Write(_,_,_) = raise (NotSupportedException())
        override this.Position
            with get() = int64 sofarOverall
            and set(_) = raise (NotSupportedException())

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
            if pr.GetByte() <> LEAF_NODE then failwith "leaf has invalid page type"
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
                bcmp.Compare k other

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
                // overflow (which could be a leaf or a parent or
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
                ()
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
                let vlen = pr.GetVarint() |> int
                if 0uy <> (vflag &&& FLAG_TOMBSTONE) then null
                else if 0uy <> (vflag &&& FLAG_OVERFLOW) then 
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
    
    let OpenCursor(fs, pageSize:int, rootPage:int, hook:Action<ICursor>) :ICursor =
        new myCursor(fs, pageSize, rootPage, hook) :> ICursor

[<AbstractClass;Sealed>]
type BTreeSegment =
    static member CreateFromSortedSequence(fs:Stream, pageManager:IPages, source:seq<kvp>) = 
        bt.CreateFromSortedSequenceOfKeyValuePairs (fs, pageManager, source)

    static member CreateFromSortedSequence(fs:Stream, pageManager:IPages, pairs:seq<byte[]*Stream>) = 
        let source = seq { for t in pairs do yield kvp(fst t,snd t) done }
        bt.CreateFromSortedSequenceOfKeyValuePairs (fs, pageManager, source)

    static member SortAndCreate(fs:Stream, pageManager:IPages, pairs:System.Collections.Generic.IDictionary<byte[],Stream>) =
#if not
        let keys:byte[][] = (Array.ofSeq pairs.Keys)
        let sortfunc x y = bcmp.Compare x y
        Array.sortInPlaceWith sortfunc keys
        let sortedSeq = seq { for k in keys do yield kvp(k,pairs.[k]) done }
#else
        // TODO which is faster?  how does linq OrderBy implement sorting
        // of a sequence?
        let sortedSeq = pairs.AsEnumerable().OrderBy((fun (x:kvp) -> x.Key), ByteComparer())
#endif
        bt.CreateFromSortedSequenceOfKeyValuePairs (fs, pageManager, sortedSeq)

    static member SortAndCreate(fs:Stream, pageManager:IPages, pairs:Map<byte[],Stream>) =
        let keys:byte[][] = pairs |> Map.toSeq |> Seq.map fst |> Array.ofSeq
        let sortfunc x y = bcmp.Compare x y
        Array.sortInPlaceWith sortfunc keys
        let sortedSeq = seq { for k in keys do yield kvp(k,pairs.[k]) done }
        bt.CreateFromSortedSequenceOfKeyValuePairs (fs, pageManager, sortedSeq)

    // TODO Create overload for System.Collections.Immutable.SortedDictionary
    // which would be trusting that it was created with the right comparer?

    static member OpenCursor(fs, pageSize:int, rootPage:int, hook:Action<ICursor>) :ICursor =
        bt.OpenCursor(fs,pageSize,rootPage,hook)

type trivialPendingSegment() =
    interface IPendingSegment

type trivialMemoryPageManager(_pageSize) =
    let pageSize = _pageSize

    interface IPages with
        member this.PageSize = pageSize
        member this.Begin() = trivialPendingSegment() :> IPendingSegment
        member this.End(_,_) = Guid.Empty
        member this.GetBlock(_) = PageBlock(1,-1)

type dbf(_path) = 
    let path = _path

    // TODO this code should move elsewhere, since this file wants to be a PCL

    interface IDatabaseFile with
        member this.OpenForWriting() =
            new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite) :> Stream
        member this.OpenForReading() =
            new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite) :> Stream

type private SegmentInfoListLocation =
    {
        firstPage: int
        lastPage: int
        innerPageSize: int
        len: int
    }

type private HeaderData =
    {
        currentState: Guid list
        sill: SegmentInfoListLocation
        segments: Map<Guid,PageBlock list>
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
        if lastPage < lastBlock.lastPage then
            //printfn "gap: %d to %d\n" (lastPage+1) lastGivenPage
            blockList <- PageBlock(lastBlock.firstPage, lastPage) :: (List.tail blockList)
        (Guid.NewGuid(), blockList)


type Database(_io:IDatabaseFile) =
    let io = _io
    let fsMine = io.OpenForWriting()

    let HEADER_SIZE_IN_BYTES = 4096

    let WASTE_PAGES_AFTER_EACH_BLOCK = 0 // TODO remove.  testing only.
    let PAGES_PER_BLOCK = 10 // TODO not hard-coded.  and 10 is way too low.

    let readHeader() =
        let read() =
            if fsMine.Length >= (HEADER_SIZE_IN_BYTES |> int64) then
                let pr = PageReader(HEADER_SIZE_IN_BYTES)
                pr.Read(fsMine)
                Some pr
            else
                None

        let parse (pr:PageReader) =
            let readSegmentList () =
                let rec f more cur =
                    if more > 0 then
                        let token = pr.GetArray(16)
                        let g = Guid(token)
                        // TODO need the list to be in the right order
                        f (more-1) ( g :: cur)
                    else
                        cur
                let count = pr.GetVarint() |> int
                f count []

            let readBlockList (prBlocks:PageReader) =
                let rec f more cur =
                    if more > 0 then
                        let firstPage = prBlocks.GetVarint() |> int
                        let lastPage = prBlocks.GetVarint() |> int
                        f (more-1) (PageBlock(firstPage,lastPage) :: cur)
                    else
                        cur

                let count = prBlocks.GetVarint() |> int
                f count []

            let readSegmentInfoList outerPageSize sill =
                utils.SeekPage(fsMine, outerPageSize, sill.firstPage)
                let buf:byte[] = Array.zeroCreate sill.len
                utils.ReadFully(fsMine, buf, 0, sill.len)
                let ms = new MemoryStream(buf)
                let innerRootPage = sill.len / sill.innerPageSize
                let csr = BTreeSegment.OpenCursor(ms, sill.innerPageSize, innerRootPage, null)
                csr.First()

                let rec f (cur:Map<Guid,PageBlock list>) =
                    if csr.IsValid() then
                        let g = Guid(csr.Key())
                        let prBlocks = PageReader(csr.ValueLength())
                        prBlocks.Read(csr.Value())
                        let blocks = readBlockList(prBlocks)
                        csr.Next()
                        f (cur.Add(g, blocks))
                    else
                        cur

                f Map.empty

            // --------

            let pageSize = pr.GetInt32()

            let segmentInfoListFirstPage = pr.GetInt32()
            let segmentInfoListLastPage = pr.GetInt32()
            let segmentInfoListInnerPageSize = pr.GetInt32()
            let segmentInfoListLength = pr.GetInt32()
            let sill = 
                {
                    firstPage=segmentInfoListFirstPage 
                    lastPage=segmentInfoListLastPage
                    innerPageSize=segmentInfoListInnerPageSize
                    len = segmentInfoListLength
                }

            let state = readSegmentList()

            let segments = readSegmentInfoList pageSize sill

            let hd = 
                {
                    currentState=state 
                    segments=segments 
                    sill = sill
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
                let defaultPageSize = 256 // TODO very low default, only for testing
                let h = 
                    {
                        segments = Map.empty
                        currentState = []
                        sill = {firstPage=0;lastPage=0;innerPageSize=0;len=0;}
                    }
                let nextAvailablePage = calcNextPage defaultPageSize (int64 HEADER_SIZE_IN_BYTES)
                (h, defaultPageSize, nextAvailablePage)

    let (firstReadOfHeader,pageSize,firstAvailablePage) = readHeader()

    // TODO need to build a list of all the available pages

    let mutable header = firstReadOfHeader
    let mutable nextPage = firstAvailablePage
    let mutable segmentsInWaiting: Map<Guid,PageBlock list> = Map.empty

    // TODO need a free block list so the following code can reuse
    // pages.  right now it always returns more blocks at the end
    // of the file.

    let critSectionNextPage = obj()
    let getBlock num =
        lock critSectionNextPage (fun () -> 
            let blk = PageBlock(nextPage, nextPage+num-1) 
            nextPage <- nextPage + num + WASTE_PAGES_AFTER_EACH_BLOCK
            blk
            )

    // a block list for a segment is a single blob of bytes.
    // number of pairs
    // each pair is startBlock,lastBlock
    // all in varints

    let spaceNeededForBlockList (blocks:PageBlock list) =
        let a = List.sumBy (fun (t:PageBlock) -> Varint.SpaceNeededFor(t.firstPage |> int64) + Varint.SpaceNeededFor(t.lastPage |> int64)) blocks
        let b = Varint.SpaceNeededFor(List.length blocks |> int64)
        a + b

    let buildBlockList (blocks:PageBlock list) =
        let space = spaceNeededForBlockList blocks
        let pb = PageBuilder(space)
        pb.PutVarint(List.length blocks |> int64)
        List.iter (fun (t:PageBlock) -> pb.PutVarint(t.firstPage |> int64); pb.PutVarint(t.lastPage |> int64);) blocks
        pb.Buffer

    // the segment info list is a list of segments (by guid), and for
    // each one, a block list (a blob described above).  it is written
    // into a memory btree which always has page size 512.

    let buildSegmentInfoList innerPageSize (sd:Map<Guid,PageBlock list>) = 
        let sd2 = Map.fold (fun acc (g:Guid) blocks -> Map.add (g.ToByteArray()) ((new MemoryStream(buildBlockList blocks)) :> Stream) acc) Map.empty sd
        let ms = new MemoryStream()
        let pm = trivialMemoryPageManager(innerPageSize)
        BTreeSegment.SortAndCreate(ms, pm, sd2) |> ignore
        ms

    // the segment info list is written to a contiguous set of pages.
    // those pages are special.  they are not obtained from the "page manager"
    // (this class's implementation of IPages below).  the segment info list
    // is not a segment in the segment info list.  it is a btree segment,
    // but it is written into a set of pages obtained directly from getBlock.
    // a reference to the segment info list page range is recorded in the
    // header of the file.

    let saveSegmentInfoList (sd:Map<Guid,PageBlock list>) = 
        let innerPageSize = 512
        let ms = buildSegmentInfoList innerPageSize sd
        let len = ms.Length |> int
        let pagesNeeded = len / pageSize + if 0 <> len % pageSize then 1 else 0
        let blk = getBlock (pagesNeeded)
        utils.SeekPage(fsMine, pageSize, blk.firstPage)
        fsMine.Write(ms.GetBuffer(), 0, len)
        {firstPage=blk.firstPage;lastPage=blk.lastPage;innerPageSize=innerPageSize;len=len}

    let critSectionCursors = obj()
    let mutable cursors:Map<Guid,ICursor list> = Map.empty

    let getCursor h g =
        let seg = Map.find g h.segments
        let lastBlock = List.head seg
        let rootPage = lastBlock.lastPage
        let fs = io.OpenForReading() // TODO pool and reuse these?
        let hook (csr:ICursor) =
            fs.Close()
            lock critSectionCursors (fun () -> 
                let cur = Map.find g cursors
                let removed = List.filter (fun x -> Object.ReferenceEquals(csr, x)) cur
                cursors <- Map.add g removed cursors
                )
        let csr = BTreeSegment.OpenCursor(fs, pageSize, rootPage, Action<ICursor>(hook))
        lock critSectionCursors (fun () -> 
            let cur = match Map.tryFind g cursors with
                       | Some c -> c
                       | None -> []
            cursors <- Map.add g (csr :: cur) cursors
            )
        csr

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
                let blk = getBlock PAGES_PER_BLOCK
                ps.AddBlock(blk)
                blk

            member this.End(token, lastPage) =
                let ps = token :?> PendingSegment
                let (g,blocks) = ps.End(lastPage)
                lock critSectionSegmentsInWaiting (fun () -> 
                    segmentsInWaiting <- Map.add g blocks segmentsInWaiting
                )
                g
        }

    // TODO we only want one merge going on at a time.  or rather, we only
    // want a given segment to be involved in one merge at a time.  no reason
    // to waste effort.

    // TODO do we basically always want merges to happen in the background?
    // no.  sometimes a merge has to happen because the header has no more
    // room.

    // TODO we need a way to get the writelock and wait for it.  because a
    // merge doesn't care when it gets the lock, just that it gets it.

    let merge segs = 
        let h = header
        let cursors = List.map (fun g -> getCursor h g) segs
        let mc = MultiCursor.Create cursors
        let pairs = CursorUtils.ToSortedSequenceOfKeyValuePairs mc
        use fs = io.OpenForWriting() // TODO pool and reuse?
        let (g,_) = BTreeSegment.CreateFromSortedSequence(fs, pageManager, pairs)
        g

    let critSectionInTransaction = obj()
    let mutable inTransaction = false 

    let critSectionHeader = obj()

    let writeHeader hdr =
        let pb = PageBuilder(HEADER_SIZE_IN_BYTES)
        pb.PutInt32(pageSize)

        pb.PutInt32(hdr.sill.firstPage)
        pb.PutInt32(hdr.sill.lastPage)
        pb.PutInt32(hdr.sill.innerPageSize)
        pb.PutInt32(hdr.sill.len)

        pb.PutVarint(List.length hdr.currentState |> int64)
        List.iter (fun (g:Guid) -> 
            pb.PutArray(g.ToByteArray())
            ) hdr.currentState

        fsMine.Seek(0L, SeekOrigin.Begin) |> ignore
        pb.Write(fsMine)
        fsMine.Flush()

    let consolidateBlockList h =
        // TODO filter the segments by guid, ignore anything with a cursor.
        // if this is called at startup, there should be no cursors yet.  still.
        let allBlocks = Map.fold (fun acc _ value -> value @ acc) [] h.segments
        let sortedBlocks = List.sortBy (fun (x:PageBlock) -> x.firstPage) allBlocks
        let fldr acc (t:PageBlock) =
            let (blk:PageBlock, pile) = acc
            if blk.lastPage + 1 = t.firstPage then
                (PageBlock(blk.firstPage, t.lastPage), pile)
            else
                (PageBlock(t.firstPage, t.lastPage), blk :: pile)
        let folded = List.fold fldr (List.head sortedBlocks, []) (List.tail sortedBlocks)
        let consolidated = (fst folded) :: (snd folded)
        // TODO note that the the blocks consumed by the segmentInfoList are ignored here
        consolidated

    let dispose itIsSafeToAlsoFreeManagedObjects =
        //let blocks = consolidateBlockList header
        //printfn "%A" blocks
        if itIsSafeToAlsoFreeManagedObjects then
            fsMine.Close()

    let tryGetWriteLock() =
        let gotLock = lock critSectionInTransaction (fun () -> if inTransaction then false else inTransaction <- true; true)
        if not gotLock then None
        else Some { 
            new System.Object() with
                override this.Finalize() =
                    // no need for lock critSectionInTransaction here because there is
                    // no way for the code to be here unless the caller is holding the
                    // lock.
                    inTransaction <- false

            interface IWriteLock with
                member this.Dispose() =
                    // no need for lock critSectionInTransaction here because there is
                    // no way for the code to be here unless the caller is holding the
                    // lock.
                    inTransaction <- false
                    GC.SuppressFinalize(this)

                member this.PrependSegments(newGuids:seq<Guid>) =
                    // TODO if there is more than one new segment, I suppose we could
                    // immediately start a background task to merge them?  after the
                    // commit happens.
                    let newGuidsAsSet = Seq.fold (fun acc g -> Set.add g acc) Set.empty newGuids
                    let mySegmentsInWaiting = Map.filter (fun g _ -> Set.contains g newGuidsAsSet) segmentsInWaiting
                    lock critSectionHeader (fun () -> 
                        let newState = (List.ofSeq newGuids) @ header.currentState
                        let newSegments = Map.fold (fun acc key value -> Map.add key value acc) header.segments mySegmentsInWaiting
                        let newSill = saveSegmentInfoList newSegments
                        let newHeader = {currentState=newState; segments=newSegments; sill = newSill;}
                        writeHeader newHeader
                        // TODO the pages for the old location of the segment info list
                        // could now be reused
                        header <- newHeader
                    )
                    // no need for lock critSectionInTransaction here because there is
                    // no way for the code to be here unless the caller is holding the
                    // lock.
                    inTransaction <- false
                    // all the segments we just committed can now be removed from
                    // the segments in waiting list
                    lock critSectionSegmentsInWaiting (fun () ->
                        let remainingSegmentsInWaiting = Map.filter (fun g _ -> Set.contains g newGuidsAsSet |> not) segmentsInWaiting
                        segmentsInWaiting <- remainingSegmentsInWaiting
                    )
        }

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
            use fs = io.OpenForWriting() // TODO pool and reuse?
            let (g,_) = BTreeSegment.CreateFromSortedSequence(fs, pageManager, pairs)
            g

        member this.WriteSegment(pairs:System.Collections.Generic.IDictionary<byte[],Stream>) =
            use fs = io.OpenForWriting() // TODO pool and reuse?
            let (g,_) = BTreeSegment.SortAndCreate(fs, pageManager, pairs)
            g

        member this.OpenCursor() =
            // TODO we also need a way to open a cursor on segments in waiting
            let h = header
            let cursors = List.map (fun g -> getCursor h g) h.currentState
            let mc = MultiCursor.Create cursors
            LivingCursor.Create mc

        member this.RequestWriteLock() =
            match tryGetWriteLock() with
                | Some x -> x
                | None -> failwith "already inTransaction"


