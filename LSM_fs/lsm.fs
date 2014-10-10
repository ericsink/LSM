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
open System.Collections.Generic

open Zumero.LSM

module utils =
        let ReadFully(strm:Stream, buf, off, len) =
            let mutable sofar = 0
            while sofar<len do
                let got = strm.Read(buf, off + sofar, len - sofar)
                //if 0 = got then throw?
                sofar <- sofar + got
            sofar

        let ReadAll (strm:Stream) =
            let len = int strm.Length
            let buf:byte[] = Array.zeroCreate len
            let mutable sofar = 0
            while sofar<len do
                let got = strm.Read(buf, sofar, len - sofar)
                //if 0 = got then throw?
                sofar <- sofar + got
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

type private PageBuilder(pgsz:int) =
    let mutable cur = 0
    let buf:byte[] = Array.zeroCreate pgsz

    member this.Reset() =
        cur <- 0

    member this.Flush(s:Stream) =
        s.Write(buf, 0, buf.Length)

    member this.Position =
        cur

    member this.Available =
        buf.Length - cur

    member this.PutByte(x:byte) =
        buf.[cur] <- byte x
        cur <- cur+1

    member this.PutStream(s:Stream, len:int) =
        ignore (utils.ReadFully(s, buf, cur, len))
        cur <- cur+len

    member this.PutArray(ba:byte[]) =
        System.Array.Copy (ba, 0, buf, cur, ba.Length)
        cur <- cur + ba.Length

    member this.PutUInt32(v:uint32) =
        buf.[cur+0] <- byte (v >>>  24)
        buf.[cur+1] <- byte (v >>>  16)
        buf.[cur+2] <- byte (v >>>  8)
        buf.[cur+3] <- byte (v >>>  0)
        cur <- cur+4

    member this.PutUInt16(v:uint16) =
        buf.[cur+0] <- byte (v >>>  8)
        buf.[cur+1] <- byte (v >>>  0)
        cur <- cur+2

    member this.PutUInt16At(at:int, v:uint16) =
        buf.[at+0] <- byte (v >>>  8)
        buf.[at+1] <- byte (v >>>  0)

    member this.PutVarint(v:uint64) =
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

    member this.Position =
        cur

    member this.SetPosition(x) =
        cur <- x

    member this.Read(s:Stream) =
        s.Read(buf, 0, buf.Length)

    member this.Reset() =
        cur <- 0

    member this.GetByte() =
        let r = buf.[cur]
        cur <- cur + 1
        r

    member this.Compare(len, other) =
        ByteComparer.CompareWithin buf cur len other

    member this.PageType =
        buf.[0]

    member this.ReadUInt32() =
        let a0 = uint64 buf.[cur+0]
        let a1 = uint64 buf.[cur+1]
        let a2 = uint64 buf.[cur+2]
        let a3 = uint64 buf.[cur+3]
        let r = (a0 <<< 24) ||| (a1 <<< 16) ||| (a2 <<< 8) ||| (a3 <<< 0)
        cur <- cur + 4
        uint32 r

    member this.GetArray(len) =
        let ba:byte[] = Array.zeroCreate len
        System.Array.Copy(buf, cur, ba, 0, len)
        cur <- cur + len
        ba

    member this.GetUInt16() =
        let a0 = uint64 buf.[cur+0]
        let a1 = uint64 buf.[cur+1]
        let r = (a0 <<< 8) ||| (a1 <<< 0)
        cur <- cur + 2
        uint16 r

    member this.GetVarint() =
        let a0 = uint64 buf.[cur]
        if a0 <= 240UL then 
            cur <- cur + 1
            uint64 a0
        else if a0 <= 248UL then
            let a1 = uint64 buf.[cur+1]
            cur <- cur + 2
            (240UL + 256UL * (a0 - 241UL) + a1)
        else if a0 = 249UL then
            let a1 = uint64 buf.[cur+1]
            let a2 = uint64 buf.[cur+2]
            cur <- cur + 3
            (2288UL + 256UL * a1 + a2)
        else if a0 = 250UL then
            let a1 = uint64 buf.[cur+1]
            let a2 = uint64 buf.[cur+2]
            let a3 = uint64 buf.[cur+3]
            cur <- cur + 4
            (a1<<<16) ||| (a2<<<8) ||| a3
        else if a0 = 251UL then
            let a1 = uint64 buf.[cur+1]
            let a2 = uint64 buf.[cur+2]
            let a3 = uint64 buf.[cur+3]
            let a4 = uint64 buf.[cur+4]
            cur <- cur + 5
            (a1<<<24) ||| (a2<<<16) ||| (a3<<<8) ||| a4
        else if a0 = 252UL then
            let a1 = uint64 buf.[cur+1]
            let a2 = uint64 buf.[cur+2]
            let a3 = uint64 buf.[cur+3]
            let a4 = uint64 buf.[cur+4]
            let a5 = uint64 buf.[cur+5]
            cur <- cur + 6
            (a1<<<32) ||| (a2<<<24) ||| (a3<<<16) ||| (a4<<<8) ||| a5
        else if a0 = 253UL then
            let a1 = uint64 buf.[cur+1]
            let a2 = uint64 buf.[cur+2]
            let a3 = uint64 buf.[cur+3]
            let a4 = uint64 buf.[cur+4]
            let a5 = uint64 buf.[cur+5]
            let a6 = uint64 buf.[cur+6]
            cur <- cur + 7
            (a1<<<40) ||| (a2<<<32) ||| (a3<<<24) ||| (a4<<<16) ||| (a5<<<8) ||| a6
        else if a0 = 254UL then
            let a1 = uint64 buf.[cur+1]
            let a2 = uint64 buf.[cur+2]
            let a3 = uint64 buf.[cur+3]
            let a4 = uint64 buf.[cur+4]
            let a5 = uint64 buf.[cur+5]
            let a6 = uint64 buf.[cur+6]
            let a7 = uint64 buf.[cur+7]
            cur <- cur + 8
            (a1<<<48) ||| (a2<<<40) ||| (a3<<<32) ||| (a4<<<24) ||| (a5<<<16) ||| (a6<<<8) ||| a7
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
            (a1<<<56) ||| (a2<<<48) ||| (a3<<<40) ||| (a4<<<32) ||| (a5<<<24) ||| (a6<<<16) ||| (a7<<<8) ||| a8
            
    member this.Skip(len) =
        cur <- cur + len

type MemorySegment() =
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
        let keys:byte[][] = (Array.ofSeq pairs.Keys)
        let sortfunc x y = ByteComparer.Compare x y
        Array.sortInPlaceWith sortfunc keys

        let rec search k min max sop le ge = 
            if max < min then
                if sop = SeekOp.SEEK_EQ then -1
                else if sop = SeekOp.SEEK_LE then le
                else ge
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
                let v = pairs.[keys.[!cur]]
                if v <> null then ignore (v.Seek(0L, SeekOrigin.Begin))
                v

            member this.ValueLength() =
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
            openCursor()

    static member Create() =
        new MemorySegment()

type private Direction = FORWARD=0 | BACKWARD=1 | WANDERING=2

type MultiCursor(_subcursors:IEnumerable<ICursor>) =
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

    static member create([<ParamArray>] _subcursors: ICursor[]) :ICursor =
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
        if v<=240UL then 1
        else if v<=2287UL then 2
        else if v<=67823UL then 3
        else if v<=16777215UL then 4
        else if v<=4294967295UL then 5
        else if v<=1099511627775UL then 6
        else if v<=281474976710655UL then 7
        else if v<=72057594037927935UL then 8
        else 9

module BTreeSegment =
    let private LEAF_NODE:byte = 1uy
    let private PARENT_NODE:byte = 2uy
    let private OVERFLOW_NODE:byte = 3uy
    let private FLAG_OVERFLOW:byte = 1uy
    let private FLAG_TOMBSTONE:byte = 2uy
    let private FLAG_ROOT_NODE:byte = 1uy
    let private PAGE_SIZE = 4096
    let private OVERFLOW_PAGE_HEADER_SIZE = 6
    let private PARENT_NODE_HEADER_SIZE = 8
    let private LEAF_HEADER_SIZE = 8
    let private OFFSET_COUNT_PAIRS = 6

    let private countOverflowPagesFor len = 
        let bytesPerPage = PAGE_SIZE - OVERFLOW_PAGE_HEADER_SIZE
        let pages = len / bytesPerPage
        let extra = if (len % bytesPerPage) <> 0 then 1 else 0
        pages + extra

    let private putArrayWithLength (pb:PageBuilder) (ba:byte[]) =
        if null = ba then
            pb.PutByte(FLAG_TOMBSTONE)
            pb.PutVarint(0UL)
        else
            pb.PutByte(0uy)
            pb.PutVarint(uint64 ba.Length)
            pb.PutArray(ba)

    let private putStreamWithLength (pb:PageBuilder) (ba:Stream) vlen =
        if null = ba then
            pb.PutByte(FLAG_TOMBSTONE)
            pb.PutVarint(0UL)
        else
            pb.PutByte(0uy)
            pb.PutVarint(uint64 vlen)
            pb.PutStream(ba, int vlen)

    let private writeOverflowFromStream (pb:PageBuilder) (fs:Stream) (ba:Stream) =
        let needed = countOverflowPagesFor (int ba.Length)
        let len = int ba.Length

        let rec fn sofar count =
            pb.Reset()
            pb.PutByte(OVERFLOW_NODE)
            pb.PutByte(0uy)
            pb.PutUInt32(uint32 (needed - count))
            let num = Math.Min((PAGE_SIZE - OVERFLOW_PAGE_HEADER_SIZE), (len - sofar))
            pb.PutStream(ba, num)
            pb.Flush(fs)
            if (sofar+num) < len then
                fn (sofar+num) (count+1)
            else
                count + 1

        fn 0 0

    let private writeOverflowFromArray (pb:PageBuilder) (fs:Stream) (ba:byte[]) =
        writeOverflowFromStream pb fs (new MemoryStream(ba))

    let private buildParentPage root firstLeaf lastLeaf (overflows:System.Collections.Generic.Dictionary<int,uint32>) (pb:PageBuilder) (children:System.Collections.Generic.List<uint32 * byte[]>) stop start =
        let countKeys = stop - start
        pb.Reset ()
        pb.PutByte (PARENT_NODE)
        pb.PutByte (if root then FLAG_ROOT_NODE else 0uy)
        pb.PutUInt16 (uint16 countKeys)
        if root then
            pb.PutUInt32(firstLeaf)
            pb.PutUInt32(lastLeaf)
        for q in start .. stop do
            pb.PutVarint(uint64 (fst (children.[q])))
        for q in start .. (stop-1) do
            let k = snd children.[q]
            if ((overflows <> null) && overflows.ContainsKey (q)) then
                pb.PutByte(FLAG_OVERFLOW)
                pb.PutVarint(uint64 k.Length)
                pb.PutUInt32(overflows.[q])
            else
                putArrayWithLength pb k

    let private calcAvailable currentSize couldBeRoot =
        let basicSize = PAGE_SIZE - currentSize
        if couldBeRoot then
            basicSize - 2 * 4
        else
            basicSize

    let private writeParentNodes firstLeaf lastLeaf (children:System.Collections.Generic.List<uint32 * byte[]>) startingPageNumber fs pb =
        let nextGeneration = new System.Collections.Generic.List<uint32 * byte[]>()
        // TODO encapsulate mutables in a class?
        let mutable sofar = 0
        let mutable nextPageNumber = startingPageNumber
        let overflows = new System.Collections.Generic.Dictionary<int,uint32>()
        let mutable first = 0
        for i in 0 .. children.Count-1 do
            let (pagenum,k) = children.[i]
            let neededForInline = 1 + Varint.SpaceNeededFor (uint64 k.Length) + k.Length + Varint.SpaceNeededFor (uint64 pagenum)
            let neededForOverflow = 1 + Varint.SpaceNeededFor (uint64 k.Length) + 4 + Varint.SpaceNeededFor (uint64 pagenum)
            let isLastChild = (i = (children.Count - 1))
            if (sofar > 0) then
                let couldBeRoot = (nextGeneration.Count = 0)
                let avail = calcAvailable sofar couldBeRoot 
                let fitsInline = (avail >= neededForInline)
                let wouldFitInlineOnNextPage = ((PAGE_SIZE - PARENT_NODE_HEADER_SIZE) >= neededForInline)
                let fitsOverflow = (avail >= neededForOverflow)
                let flushThisPage = isLastChild || ((not fitsInline) && (wouldFitInlineOnNextPage || (not fitsOverflow))) 
                let isRootNode = isLastChild && couldBeRoot

                if flushThisPage then
                    buildParentPage isRootNode firstLeaf lastLeaf overflows pb children i first
                    pb.Flush(fs)
                    nextGeneration.Add(nextPageNumber, snd children.[i-1])
                    sofar <- 0
                    first <- 0
                    overflows.Clear()
            if not isLastChild then 
                if sofar = 0 then
                    first <- i
                    overflows.Clear()
                    // 2 for the page type and flags
                    // 2 for the stored count
                    // 5 for the extra ptr we will add at the end, a varint, 5 is worst case
                    sofar <- 2 + 2 + 5
                if calcAvailable sofar (nextGeneration.Count = 0) >= neededForInline then
                    sofar <- sofar + k.Length
                else
                    // it's okay to pass our main PageBuilder here for working purposes.  we're not
                    // really using it yet, until we call buildParentPage
                    let keyOverflowFirstPage = nextPageNumber
                    let keyOverflowPageCount = writeOverflowFromArray pb fs k
                    nextPageNumber <- nextPageNumber + uint32 keyOverflowPageCount
                    sofar <- sofar + 4
                    overflows.[i] <- keyOverflowFirstPage
                // inline or not, we need space for the following things
                sofar <- sofar + 1 + Varint.SpaceNeededFor(uint64 k.Length) + Varint.SpaceNeededFor(uint64 pagenum)
        nextGeneration

    // TODO we probably want this function to accept a pagesize and base pagenumber
    let Create(fs:Stream, csr:ICursor) :uint32 = 
        let pb = new PageBuilder(PAGE_SIZE)
        let pbOverflow = new PageBuilder(PAGE_SIZE)
        // TODO encapsulate mutables in a class?
        let mutable nextPageNumber:uint32 = 1u
        let mutable prevPageNumber:uint32 = 0u
        let mutable countPairs = 0
        let mutable lastKey:byte[] = null
        let mutable nodelist = new System.Collections.Generic.List<uint32 * byte[]>()
        csr.First()
        while csr.IsValid() do
            let k = csr.Key()
            let v = csr.Value()
            // assert k <> null
            // but v might be null (a tombstone)
            let vlen = if v<>null then v.Length else int64 0
            let neededForOverflowPageNumber = 4 // TODO sizeof uint32

            let neededForKeyBase = 1 + Varint.SpaceNeededFor(uint64 k.Length)
            let neededForKeyInline = neededForKeyBase + k.Length
            let neededForKeyOverflow = neededForKeyBase + neededForOverflowPageNumber

            let neededForValueInline = 1 + if v<>null then Varint.SpaceNeededFor(uint64 vlen) + int vlen else 0
            let neededForValueOverflow = 1 + if v<>null then Varint.SpaceNeededFor(uint64 vlen) + neededForOverflowPageNumber else 0

            let neededForBothInline = neededForKeyInline + neededForValueInline
            let neededForKeyInlineValueOverflow = neededForKeyInline + neededForValueOverflow
            let neededForBothOverflow = neededForKeyOverflow + neededForValueOverflow

            csr.Next()

            if pb.Position > 0 then
                let avail = pb.Available
                let fitBothInline = (avail >= neededForBothInline)
                let wouldFitBothInlineOnNextPage = ((PAGE_SIZE - LEAF_HEADER_SIZE) >= neededForBothInline)
                let fitKeyInlineValueOverflow = (avail >= neededForKeyInlineValueOverflow)
                let fitBothOverflow = (avail >= neededForBothOverflow)
                let flushThisPage = (not fitBothInline) && (wouldFitBothInlineOnNextPage || ( (not fitKeyInlineValueOverflow) && (not fitBothOverflow) ) )

                if flushThisPage then
                    // note similar flush code below at the end of the loop
                    pb.PutUInt16At (OFFSET_COUNT_PAIRS, uint16 countPairs)
                    pb.Flush(fs)
                    nodelist.Add(nextPageNumber, lastKey)
                    prevPageNumber <- nextPageNumber
                    nextPageNumber <- nextPageNumber + uint32 1
                    pb.Reset()
                    countPairs <- 0
                    lastKey <- null
            if pb.Position = 0 then
                // we are here either because we just flushed a page
                // or because this is the very first page
                countPairs <- 0
                lastKey <- null
                pb.PutByte(LEAF_NODE)
                pb.PutByte(0uy) // flags

                pb.PutUInt32 (prevPageNumber) // prev page num.
                pb.PutUInt16 (0us) // number of pairs in this page. zero for now. written at end.
                // assert pb.Position is 8 (LEAF_HEADER_SIZE)
            let available = pb.Available
            (*
             * one of the following cases must now be true:
             * 
             * - both the key and val will fit
             * - key inline and overflow the val
             * - overflow both
             * 
             * note that we don't care about the case where the
             * val would fit if we overflowed the key.  if the key
             * needs to be overflowed, then we're going to overflow
             * the val as well, even if it would fit.
             * 
             * if bumping to the next page would help, we have
             * already done it above.
             * 
             *)
            if (available >= neededForBothInline) then
                putArrayWithLength pb k
                putStreamWithLength pb v vlen
            else
                if (available >= neededForKeyInlineValueOverflow) then
                    putArrayWithLength pb k
                else
                    let keyOverflowFirstPage = nextPageNumber
                    let keyOverflowPageCount = writeOverflowFromArray pbOverflow fs k
                    nextPageNumber <- nextPageNumber + uint32 keyOverflowPageCount
                    pb.PutByte(FLAG_OVERFLOW)
                    pb.PutVarint(uint64 k.Length)
                    pb.PutUInt32(keyOverflowFirstPage)

                let valueOverflowFirstPage = nextPageNumber
                let valueOverflowPageCount = writeOverflowFromStream pbOverflow fs v
                nextPageNumber <- nextPageNumber + uint32 valueOverflowPageCount
                pb.PutByte(FLAG_OVERFLOW)
                pb.PutVarint(uint64 vlen)
                pb.PutUInt32(valueOverflowFirstPage)
            lastKey <- k
            countPairs <- countPairs + 1
        if pb.Position > 0 then
            // note similar flush code above
            pb.PutUInt16At (OFFSET_COUNT_PAIRS, uint16 countPairs)
            pb.Flush(fs)
            nodelist.Add(nextPageNumber, lastKey)
        if nodelist.Count > 0 then
            let firstLeaf = fst nodelist.[0]
            let lastLeaf = fst nodelist.[nodelist.Count-1]
            // now write the parent pages.
            // maybe more than one level of them.
            // keep writing until we have written a level which has only one node,
            // which is the root node.
            while nodelist.Count > 1 do
                nodelist <- writeParentNodes firstLeaf lastLeaf nodelist nextPageNumber fs pb
                nextPageNumber <- nextPageNumber + uint32 nodelist.Count
            // assert nodelist.Count = 1
            fst nodelist.[0]
        else
            uint32 0

    type private myOverflowReadStream(_fs:Stream, first:uint32, _len:int) =
        inherit Stream()
        let fs = _fs
        let len = _len
        let buf:byte[] = Array.zeroCreate PAGE_SIZE
        let mutable curpage = first
        let mutable sofarOverall = 0
        let mutable sofarThisPage = 0

        // TODO consider supporting seek (if this.fs does)

        let ReadPage() =
            let pos = (curpage - 1u) * uint32 PAGE_SIZE
            fs.Seek(int64 pos, SeekOrigin.Begin) |> ignore
            utils.ReadFully(fs, buf, 0, PAGE_SIZE) |> ignore
            // assert PageType is OVERFLOW
            sofarThisPage <- 0

        do ReadPage()

        override this.Length = int64 len
        override this.CanRead = sofarOverall < len

        override this.Read(ba,offset,wanted) =
            if sofarOverall >= len then
                0
            else    
                if (sofarThisPage >= (PAGE_SIZE - OVERFLOW_PAGE_HEADER_SIZE)) then
                    curpage <- curpage + 1u
                    ReadPage()
                let available = Math.Min ((PAGE_SIZE - OVERFLOW_PAGE_HEADER_SIZE), len - sofarOverall)
                let num = Math.Min (available, wanted)
                System.Array.Copy (buf, OVERFLOW_PAGE_HEADER_SIZE + sofarThisPage, ba, offset, num)
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
            with get() = raise (new NotSupportedException())
            and set(value) = raise (new NotSupportedException())

    let private readOverflow len fs (firstPage:uint32) =
        let ostrm = new myOverflowReadStream(fs, firstPage, len)
        utils.ReadAll(ostrm)

    type private myCursor(_fs,_fsLength:int64) =
        let fs = _fs
        let fsLength = _fsLength
        let pr = new PageReader(PAGE_SIZE)

        let mutable currentPage:uint32 = 0u
        let mutable leafKeys:int[] = null
        let mutable countLeafKeys = 0 // only realloc leafKeys when it's too small
        let mutable previousLeaf:uint32 = 0u
        let mutable currentKey = -1

        let resetLeaf() =
            countLeafKeys <- 0
            previousLeaf <- 0u
            currentKey <- -1

        do resetLeaf()

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
                pr.Skip(4)

        let skipValue() =
            let vflag = pr.GetByte()
            let vlen = pr.GetVarint()
            if 0uy <> (vflag &&& FLAG_TOMBSTONE) then ()
            else if 0uy <> (vflag &&& FLAG_OVERFLOW) then pr.Skip(4)
            else pr.Skip(int vlen)

        let readLeaf() =
            resetLeaf()
            pr.Reset()
            if pr.GetByte() <> LEAF_NODE then 
                raise (new Exception())
            pr.GetByte() |> ignore
            previousLeaf <- uint32 (pr.ReadUInt32())
            countLeafKeys <- pr.GetUInt16() |> int
            if leafKeys=null || leafKeys.Length<countLeafKeys then
                leafKeys <- Array.zeroCreate countLeafKeys
            for i in 0 .. (countLeafKeys-1) do
                let pos = pr.Position
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
                let pagenum = pr.ReadUInt32()
                let k = readOverflow klen fs pagenum
                ByteComparer.Compare k other

        let keyInLeaf n = 
            pr.SetPosition(leafKeys.[n])
            let kflag = pr.GetByte()
            let klen = pr.GetVarint() |> int
            if 0uy = (kflag &&& FLAG_OVERFLOW) then
                pr.GetArray(klen)
            else
                let pagenum = pr.ReadUInt32()
                let k = readOverflow klen fs pagenum
                k

        let rec searchLeaf k min max sop le ge = 
            if max < min then
                if sop = SeekOp.SEEK_EQ then -1
                else if sop = SeekOp.SEEK_LE then le
                else ge
            else
                let mid = (max + min) / 2
                let kmid = keyInLeaf(mid)
                let cmp = compareKeyInLeaf mid k
                if 0 = cmp then mid
                else if cmp<0  then searchLeaf k (mid+1) max sop mid ge
                else searchLeaf k min (mid-1) sop le mid

        let setCurrentPage (pagenum:uint32) = 
            currentPage <- pagenum
            resetLeaf()
            if 0u = currentPage then false
            else
                let pos = (currentPage - 1u) * uint32 PAGE_SIZE
                if ((pos + uint32 PAGE_SIZE) <= uint32 fsLength) then
                    fs.Seek (int64 pos, SeekOrigin.Begin) |> ignore
                    pr.Read(fs) |> ignore
                    true
                else
                    false
                    
        let startRootPageRead() =
            pr.Reset()
            if pr.GetByte() <> PARENT_NODE then 
                raise (new Exception())
            let pflag = pr.GetByte()
            pr.GetUInt16() |> ignore
            if 0uy = (pflag &&& FLAG_ROOT_NODE) then 
                raise (new Exception())
                      
        let getFirstLeafFromRootPage() =
            startRootPageRead()
            let firstLeaf = pr.ReadUInt32()
            //let lastLeaf = pr.ReadUInt32()
            firstLeaf

        let getLastLeafFromRootPage() =
            startRootPageRead()
            //let firstLeaf = pr.ReadUInt32()
            pr.Skip(4)
            let lastLeaf = pr.ReadUInt32()
            lastLeaf

        let readParentPage() =
            pr.Reset()
            if pr.GetByte() <> PARENT_NODE then 
                raise (new Exception())
            let pflag = pr.GetByte()
            let count = pr.GetUInt16()
            let ptrs:uint32[] = Array.zeroCreate (int (count+1us))
            let keys:byte[][] = Array.zeroCreate (int count)
            if 0uy <> (pflag &&& FLAG_ROOT_NODE) then pr.Skip(2*4)
            for i in 0 .. int count do
                ptrs.[i] <-  pr.GetVarint() |> uint32
            for i in 0 .. int (count-1us) do
                let kflag = pr.GetByte()
                let klen = pr.GetVarint() |> int
                if 0uy = (kflag &&& FLAG_OVERFLOW) then
                    keys.[i] <- pr.GetArray(klen)
                else
                    let pagenum = pr.ReadUInt32()
                    keys.[i] <- readOverflow klen fs pagenum
            (ptrs,keys)

        // this is used when moving forward through the leaf pages.
        // we need to skip any overflow pages.  when moving backward,
        // this is not necessary, because each leaf has a pointer to
        // the leaf before it.
        let rec searchForwardForLeaf() = 
            let pt = pr.PageType
            if pt = LEAF_NODE then true
            else if pt = PARENT_NODE then false
            else
                pr.SetPosition(2)
                let skip = pr.ReadUInt32()
                if setCurrentPage (currentPage + skip) then
                    searchForwardForLeaf()
                else
                    false

        let leafIsValid() =
            let ok = (leafKeys <> null) && (countLeafKeys > 0) && (currentKey >= 0) && (currentKey < countLeafKeys)
            ok

        let rec searchInParentPage k (ptrs:uint32[]) (keys:byte[][]) (i:uint32) :uint32 =
            // TODO linear search?  really?
            let cmp = ByteComparer.Compare k (keys.[int i])
            if cmp>0 then
                searchInParentPage k ptrs keys (i+1u)
            else
                ptrs.[int i]

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
                                if (setCurrentPage (currentPage + 1u) && searchForwardForLeaf ()) then
                                    readLeaf()
                                    currentKey <- 0
                            else
                                if 0u = previousLeaf then
                                    resetLeaf()
                                else if setCurrentPage previousLeaf then
                                    readLeaf()
                                    currentKey <- countLeafKeys - 1
                else if PARENT_NODE = pr.PageType then
                    let (ptrs,keys) = readParentPage()
                    let found = searchInParentPage k ptrs keys 0u
                    if 0u = found then
                        search ptrs.[ptrs.Length - 1] k sop
                    else
                        search found k sop

        interface ICursor with
            member this.IsValid() =
                leafIsValid()

            member this.Seek(k,sop) =
                // start at the last page, which is always the root of the tree.  
                // it might be the leaf, in a tree with just one node.
                let pagenum = uint32 (fsLength / int64 PAGE_SIZE)
                search pagenum k sop

            member this.Key() =
                keyInLeaf currentKey
            
            member this.Value() =
                pr.SetPosition(leafKeys.[currentKey])

                skipKey()

                let vflag = pr.GetByte()
                let vlen = pr.GetVarint() |> int
                if 0uy <> (vflag &&& FLAG_TOMBSTONE) then null
                else if 0uy <> (vflag &&& FLAG_OVERFLOW) then 
                    let pagenum = pr.ReadUInt32()
                    upcast (new myOverflowReadStream(fs, pagenum, vlen))
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
                let pagenum = uint32 (fsLength / int64 PAGE_SIZE)
                if setCurrentPage pagenum then
                    let pt = pr.PageType
                    if LEAF_NODE <> pt then
                        let pg = getFirstLeafFromRootPage()
                        setCurrentPage pg |> ignore // TODO
                    readLeaf()
                    currentKey <- 0

            member this.Last() =
                let pagenum = uint32 (fsLength / int64 PAGE_SIZE)
                if setCurrentPage pagenum then
                    if LEAF_NODE <> pr.PageType then
                        let pg = getLastLeafFromRootPage()
                        setCurrentPage pg |> ignore // TODO
                    readLeaf()
                    currentKey <- countLeafKeys - 1

            member this.Next() =
                if not (nextInLeaf()) then
                    if setCurrentPage (currentPage + 1u) && searchForwardForLeaf() then
                        readLeaf()
                        currentKey <- 0

            member this.Prev() =
                if not (prevInLeaf()) then
                    if 0u = previousLeaf then
                        resetLeaf()
                    else if setCurrentPage previousLeaf then
                        readLeaf()
                        currentKey <- countLeafKeys - 1
    
    // TODO pass in a page size
    let OpenCursor(strm, length:int64) :ICursor =
        upcast (new myCursor(strm, length))

