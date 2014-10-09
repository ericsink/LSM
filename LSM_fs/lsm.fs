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

type ByteComparer() = 
    // TODO consider moving the static stuff into a module, with
    // just the IComparer interface in the class.
    static member compareSimple (x:byte[], y:byte[], xlen, ylen, i) =
        if i<xlen && i<ylen then
            //let c = x.[i].CompareTo (y.[i])
            let c = (int (x.[i])) - int (y.[i])
            if c <> 0 then c
            else ByteComparer.compareSimple (x, y, xlen, ylen, i+1)
        else xlen.CompareTo(ylen)

    static member compareWithOffsets (x:byte[], y:byte[], xlen, xoff, ylen, yoff, i) =
        if i<xlen && i<ylen then
            let c = x.[i + xoff].CompareTo (y.[i + yoff])
            if c <> 0 then c
            else ByteComparer.compareWithOffsets (x, y, xlen, xoff, ylen, yoff, i+1)
        else xlen.CompareTo(ylen)

    static member cmp (x, y) =
        ByteComparer.compareSimple(x,y,x.Length,y.Length, 0)

    static member cmp_within (x,off,len,y) =
        ByteComparer.compareWithOffsets(x,y,len,off,y.Length,0,0)

    interface IComparer<byte[]> with
        member this.Compare(x, y) =
            ByteComparer.compareSimple(x,y,x.Length,y.Length, 0)
            

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
        // TODO this inner function might be causing perf problems
        let wb x = 
            buf.[cur] <- byte x
            cur <- cur+1
        if v<=240UL then 
            wb v
        else if v<=2287UL then 
            wb ((v - 240UL) / 256UL + 241UL)
            wb ((v - 240UL) % 256UL)
        else if v<=67823UL then 
            wb 249UL
            wb ((v - 2288UL) / 256UL)
            wb ((v - 2288UL) % 256UL)
        else if v<=16777215UL then 
            wb 250UL
            wb (v >>> 16)
            wb (v >>>  8)
            wb (v >>>  0)
        else if v<=4294967295UL then 
            wb 251UL
            wb (v >>> 24)
            wb (v >>> 16)
            wb (v >>>  8)
            wb (v >>>  0)
        else if v<=1099511627775UL then 
            wb 252UL
            wb (v >>> 32)
            wb (v >>> 24)
            wb (v >>> 16)
            wb (v >>>  8)
            wb (v >>>  0)
        else if v<=281474976710655UL then 
            wb 253UL
            wb (v >>> 40)
            wb (v >>> 32)
            wb (v >>> 24)
            wb (v >>> 16)
            wb (v >>>  8)
            wb (v >>>  0)
        else if v<=72057594037927935UL then 
            wb 254UL
            wb (v >>> 48)
            wb (v >>> 40)
            wb (v >>> 32)
            wb (v >>> 24)
            wb (v >>> 16)
            wb (v >>>  8)
            wb (v >>>  0)
        else
            wb 255UL
            wb (v >>> 56)
            wb (v >>> 48)
            wb (v >>> 40)
            wb (v >>> 32)
            wb (v >>> 24)
            wb (v >>> 16)
            wb (v >>>  8)
            wb (v >>>  0)

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

    member this.cmp(len, other) =
        ByteComparer.cmp_within(buf, cur, len, other)

    // page type could just be a property
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
        let sortfunc x y = ByteComparer.cmp(x, y)
        Array.sortInPlaceWith sortfunc keys

        let rec search k min max sop le ge = 
            if max < min then
                if sop = SeekOp.SEEK_EQ then -1
                else if sop = SeekOp.SEEK_LE then le
                else ge
            else
                let mid = (max + min) / 2
                let kmid = keys.[mid]
                let cmp = ByteComparer.cmp (kmid, k)
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
                ByteComparer.cmp(keys.[!cur], k)

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

    let valid_sorted sortfunc = 
        let valids = List.filter (fun (csr:ICursor) -> csr.IsValid()) subcursors
        let sorted = List.sortWith sortfunc valids
        sorted

    let find sortfunc = 
        let vs = valid_sorted sortfunc
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
            let f (csr:ICursor) :unit =
                csr.Seek (k, sop)
                if cur.IsNone && csr.IsValid() && ( (SeekOp.SEEK_EQ = sop) || (0 = csr.KeyCompare (k)) ) then cur <- Some csr
                // TODO if found stop
            List.iter f subcursors
            dir <- Direction.WANDERING
            if cur.IsNone then
                if SeekOp.SEEK_GE = sop then
                    cur <- findMin()
                    if cur.IsSome then dir <- Direction.FORWARD
                else if SeekOp.SEEK_LE = sop then
                    cur <- findMax()
                    if cur.IsSome then dir <- Direction.BACKWARD


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
            if SeekOp.SEEK_GE = sop then skipTombstonesForward() else if SeekOp.SEEK_LE = sop then skipTombstonesBackward()

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

    let private putStreamWithLength (pb:PageBuilder) (ba:Stream) =
        if null = ba then
            pb.PutByte(FLAG_TOMBSTONE)
            pb.PutVarint(0UL)
        else
            pb.PutByte(0uy)
            pb.PutVarint(uint64 ba.Length)
            pb.PutStream(ba, int ba.Length)

    let private writeOverflowFromStream (pb:PageBuilder) (fs:Stream) (ba:Stream) =
        pb.Reset()
        let mutable sofar = 0
        let needed = countOverflowPagesFor (int ba.Length)
        let mutable count = 0;
        while sofar < int ba.Length do
            pb.Reset()
            pb.PutByte(OVERFLOW_NODE)
            pb.PutByte(0uy)
            pb.PutUInt32(uint32 (needed - count))
            let num = Math.Min((PAGE_SIZE - OVERFLOW_PAGE_HEADER_SIZE), ((int ba.Length) - sofar))
            pb.PutStream(ba, num)
            sofar <- sofar + num
            pb.Flush(fs)
            count <- count + 1
        count

    let private writeOverflowFromArray (pb:PageBuilder) (fs:Stream) (ba:byte[]) =
        writeOverflowFromStream pb fs (new MemoryStream(ba))

    let private buildParentPage root firstLeaf lastLeaf (overflows:System.Collections.Generic.Dictionary<int,uint32>) (pb:PageBuilder) (children:System.Collections.Generic.List<uint32 * byte[]>) stop start =
        let count_keys = stop - start
        pb.Reset ()
        pb.PutByte (PARENT_NODE)
        pb.PutByte (if root then FLAG_ROOT_NODE else 0uy)
        pb.PutUInt16 (uint16 count_keys)
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

    let private calcAvailable current_size could_be_root =
        let basic_size = PAGE_SIZE - current_size
        if could_be_root then
            basic_size - 2 * 4
        else
            basic_size

    let private writeParentNodes firstLeaf lastLeaf (children:System.Collections.Generic.List<uint32 * byte[]>) nextPageNumber fs pb =
        let next_generation = new System.Collections.Generic.List<uint32 * byte[]>()
        let mutable current_size = 0
        let mutable curpage = nextPageNumber
        let overflows = new System.Collections.Generic.Dictionary<int,uint32>()
        let mutable first = 0
        for i in 0 .. children.Count-1 do
            let (pagenum,k) = children.[i]
            let needed_for_inline = 1 + Varint.SpaceNeededFor (uint64 k.Length) + k.Length + Varint.SpaceNeededFor (uint64 pagenum)
            let needed_for_overflow = 1 + Varint.SpaceNeededFor (uint64 k.Length) + 4 + Varint.SpaceNeededFor (uint64 pagenum)
            let b_last_child = (i = (children.Count - 1))
            if (current_size > 0) then
                let mutable flushThisPage = false
                if b_last_child then flushThisPage <- true
                else if (calcAvailable current_size (next_generation.Count = 0) >= needed_for_inline) then ()
                else if ((PAGE_SIZE - PARENT_NODE_HEADER_SIZE) >= needed_for_inline) then flushThisPage <- true
                else if (calcAvailable current_size (next_generation.Count = 0) < needed_for_overflow) then flushThisPage <- true
                let b_this_is_the_root_node = b_last_child && (next_generation.Count = 0)
                if flushThisPage then
                    buildParentPage b_this_is_the_root_node firstLeaf lastLeaf overflows pb children i first
                    pb.Flush(fs)
                    next_generation.Add(curpage, snd children.[i-1])
                    current_size <- 0
                    first <- 0
                    overflows.Clear()
            if not b_last_child then 
                if current_size = 0 then
                    first <- i
                    overflows.Clear()
                    current_size <- 2 + 2 + 5
                if calcAvailable current_size (next_generation.Count = 0) >= needed_for_inline then
                    current_size <- current_size + k.Length
                else
                    let keyOverflowFirstPage = curpage
                    let keyOverflowPageCount = writeOverflowFromArray pb fs k
                    curpage <- curpage + uint32 keyOverflowPageCount
                    current_size <- current_size + 4
                    overflows.[i] <- keyOverflowFirstPage
                current_size <- current_size + 1 + Varint.SpaceNeededFor(uint64 k.Length) + Varint.SpaceNeededFor(uint64 pagenum)
        next_generation

    let Create(fs:Stream, csr:ICursor) :uint32 = 
        let pb = new PageBuilder(PAGE_SIZE)
        let pb2 = new PageBuilder(PAGE_SIZE)
        let mutable nextPageNumber:uint32 = 1u
        let mutable prevPageNumber:uint32 = 0u
        let mutable countPairs = 0
        let mutable lastKey:byte[] = null
        let mutable nodelist = new System.Collections.Generic.List<uint32 * byte[]>()
        csr.First()
        while csr.IsValid() do
            let k = csr.Key()
            let v = csr.Value()
            let neededForOverflowPageNumber = 4 // TODO sizeof uint32

            let neededForKeyBase = 1 + Varint.SpaceNeededFor(uint64 k.Length)
            let neededForKeyInline = neededForKeyBase + k.Length
            let neededForKeyOverflow = neededForKeyBase + neededForOverflowPageNumber

            let neededForValueInline = 1 + if v<>null then Varint.SpaceNeededFor(uint64 v.Length) + int v.Length else 0
            let neededForValueOverflow = 1 + if v<>null then Varint.SpaceNeededFor(uint64 v.Length) + neededForOverflowPageNumber else 0

            let neededForInlineBoth = neededForKeyInline + neededForValueInline
            let neededForKeyInlineValueOverflow = neededForKeyInline + neededForValueOverflow
            let neededForOverflowBoth = neededForKeyOverflow + neededForValueOverflow

            csr.Next()

            if pb.Position > 0 then
                let avail = pb.Available
                let mutable flushThisPage = false
                if (avail >= neededForInlineBoth) then ()
                else if ((PAGE_SIZE - LEAF_HEADER_SIZE) >= neededForInlineBoth) then flushThisPage <- true
                else if (avail >= neededForKeyInlineValueOverflow) then ()
                else if (avail < neededForOverflowBoth) then flushThisPage <- true
                if flushThisPage then
                    pb.PutUInt16At (OFFSET_COUNT_PAIRS, uint16 countPairs)
                    pb.Flush(fs)
                    nodelist.Add(nextPageNumber, lastKey)
                    prevPageNumber <- nextPageNumber
                    nextPageNumber <- nextPageNumber + uint32 1
                    pb.Reset()
                    countPairs <- 0
                    lastKey <- null
            if pb.Position = 0 then
                countPairs <- 0
                lastKey <- null
                pb.PutByte(LEAF_NODE)
                pb.PutByte(0uy) // flags

                pb.PutUInt32 (prevPageNumber) // prev page num.
                pb.PutUInt16 (0us) // number of pairs in this page. zero for now. written at end.
            let available = pb.Available
            if (available >= neededForInlineBoth) then
                putArrayWithLength pb k
                putStreamWithLength pb v
            else
                if (available >= neededForKeyInlineValueOverflow) then
                    putArrayWithLength pb k
                else
                    let keyOverflowFirstPage = nextPageNumber
                    let keyOverflowPageCount = writeOverflowFromArray pb2 fs k
                    nextPageNumber <- nextPageNumber + uint32 keyOverflowPageCount
                    pb.PutByte(FLAG_OVERFLOW)
                    pb.PutVarint(uint64 k.Length)
                    pb.PutUInt32(keyOverflowFirstPage)

                let valueOverflowFirstPage = nextPageNumber
                let valueOverflowPageCount = writeOverflowFromStream pb2 fs v
                nextPageNumber <- nextPageNumber + uint32 valueOverflowPageCount
                pb.PutByte(FLAG_OVERFLOW)
                pb.PutVarint(uint64 v.Length)
                pb.PutUInt32(valueOverflowFirstPage)
            lastKey <- k
            countPairs <- countPairs + 1
        if pb.Position > 0 then
            pb.PutUInt16At (OFFSET_COUNT_PAIRS, uint16 countPairs)
            pb.Flush(fs)
            nodelist.Add(nextPageNumber, lastKey)
        if nodelist.Count > 0 then
            let foo = nodelist.[0]
            let firstLeaf = fst nodelist.[0]
            let lastLeaf = fst nodelist.[nodelist.Count-1]
            while nodelist.Count > 1 do
                nodelist <- writeParentNodes firstLeaf lastLeaf nodelist nextPageNumber fs pb
                nextPageNumber <- nextPageNumber + uint32 nodelist.Count
            fst nodelist.[0]
        else
            uint32 0

    type private myOverflowReadStream(_fs:Stream, first:uint32, _len:int) =
        inherit Stream()
        let fs = _fs
        let len = _len
        let mutable curpage = first
        let buf:byte[] = Array.zeroCreate PAGE_SIZE
        let mutable sofarOverall = 0
        let mutable sofarThisPage = 0

        let ReadPage() =
            let pos = (curpage - 1u) * uint32 PAGE_SIZE
            fs.Seek(int64 pos, SeekOrigin.Begin) |> ignore
            utils.ReadFully(fs, buf, 0, PAGE_SIZE) |> ignore
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

    let private readOverflow len fs (first_page:uint32) =
        let ostrm = new myOverflowReadStream(fs, first_page, len)
        utils.ReadAll(ostrm)

    type private myCursor(_fs,_fsLength:int64) =
        let fs = _fs
        let fsLength = _fsLength
        let pr = new PageReader(PAGE_SIZE)

        let mutable currentPage:uint32 = 0u
        let mutable leafKeys:int[] = null
        let mutable previousLeaf:uint32 = 0u
        let mutable currentKey = -1

        let resetLeaf() =
            leafKeys <- null
            previousLeaf <- 0u
            currentKey <- -1

        do resetLeaf()

        let nextInLeaf() =
            if (currentKey+1) < leafKeys.Length then
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
            let count = pr.GetUInt16() |> int
            leafKeys <- Array.zeroCreate count
            for i in 0 .. (count-1) do
                let pos = pr.Position
                leafKeys.[i] <- pr.Position

                skipKey()
                skipValue()

        let compareKeyInLeaf n other = 
            pr.SetPosition(leafKeys.[n])
            let kflag = pr.GetByte()
            let klen = pr.GetVarint() |> int
            if 0uy = (kflag &&& FLAG_OVERFLOW) then
                pr.cmp(klen, other)
            else
                let pagenum = pr.ReadUInt32()
                let k = readOverflow klen fs pagenum
                ByteComparer.cmp (k, other)

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

        let parentpage_read() =
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
            let ok = (leafKeys <> null) && (leafKeys.Length > 0) && (currentKey >= 0) && (currentKey < leafKeys.Length)
            ok

        let rec searchInParentPage k (ptrs:uint32[]) (keys:byte[][]) (i:uint32) :uint32 =
            let cmp = ByteComparer.cmp (k, keys.[int i])
            if cmp>0 then
                searchInParentPage k ptrs keys (i+1u)
            else
                ptrs.[int i]

        let rec search pg k sop =
            if setCurrentPage pg then
                if LEAF_NODE = pr.PageType then
                    readLeaf()
                    currentKey <- searchLeaf k 0 (leafKeys.Length - 1) sop -1 -1
                    if SeekOp.SEEK_EQ <> sop then
                        if not (leafIsValid()) then
                            if SeekOp.SEEK_GE = sop then
                                if (setCurrentPage (currentPage + 1u) && searchForwardForLeaf ()) then
                                    readLeaf()
                                    currentKey <- 0
                            else
                                if 0u = previousLeaf then
                                    resetLeaf()
                                else if setCurrentPage previousLeaf then
                                    readLeaf()
                                    currentKey <- leafKeys.Length - 1
                else if PARENT_NODE = pr.PageType then
                    let (ptrs,keys) = parentpage_read()
                    let found = searchInParentPage k ptrs keys 0u
                    if 0u = found then
                        search ptrs.[ptrs.Length - 1] k sop
                    else
                        search found k sop

        interface ICursor with
            member this.IsValid() =
                leafIsValid()

            member this.Seek(k,sop) =
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
                    currentKey <- leafKeys.Length - 1

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
                        currentKey <- leafKeys.Length - 1
    
    let OpenCursor(strm, length:int64) :ICursor =
        upcast (new myCursor(strm, length))

