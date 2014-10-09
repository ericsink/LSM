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

    static member cmp_with_offsets (x:byte[], y:byte[], xlen, xoff, ylen, yoff, i) =
        if i<xlen && i<ylen then
            let c = x.[i + xoff].CompareTo (y.[i + yoff])
            if c <> 0 then c
            else ByteComparer.cmp_with_offsets (x, y, xlen, xoff, ylen, yoff, i+1)
        else xlen.CompareTo(ylen)

    static member cmp (x, y) =
        ByteComparer.compareSimple(x,y,x.Length,y.Length, 0)

    static member cmp_within (x,off,len,y) =
        ByteComparer.cmp_with_offsets(x,y,len,off,y.Length,0,0)

    interface IComparer<byte[]> with
        member this.Compare(x, y) =
            ByteComparer.cmp_with_offsets(x,y,x.Length,0,y.Length,0, 0)
            

type private PageBuilder(pgsz:int) =
    let mutable cur = 0
    let buf:byte[] = Array.zeroCreate pgsz

    member this.Reset() =
        cur <- 0

    member this.Flush(s:Stream) =
        s.Write(buf, 0, buf.Length)

    member this.Position =
        cur

    member this.Available() =
        buf.Length - cur

    member this.PutByte(x:byte) =
        buf.[cur] <- byte x
        cur <- cur+1

    member this.write_byte_stream(s:Stream, len:int) =
        ignore (utils.ReadFully(s, buf, cur, len))
        cur <- cur+len

    member this.write_byte_array(ba:byte[]) =
        System.Array.Copy (ba, 0, buf, cur, ba.Length)
        cur <- cur + ba.Length

    member this.write_uint32(v:uint32) =
        buf.[cur+0] <- byte (v >>>  24)
        buf.[cur+1] <- byte (v >>>  16)
        buf.[cur+2] <- byte (v >>>  8)
        buf.[cur+3] <- byte (v >>>  0)
        cur <- cur+4

    member this.write_uint16(v:uint16) =
        buf.[cur+0] <- byte (v >>>  8)
        buf.[cur+1] <- byte (v >>>  0)
        cur <- cur+2

    member this.write_uint16_at(at:int, v:uint16) =
        buf.[at+0] <- byte (v >>>  8)
        buf.[at+1] <- byte (v >>>  0)

    member this.write_varint(v:uint64) =
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

    member this.read_byte() =
        let r = buf.[cur]
        cur <- cur + 1
        r

    member this.cmp(len, other) =
        ByteComparer.cmp_within(buf, cur, len, other)

    // page type could just be a property
    member this.page_type() =
        buf.[0]

    member this.read_uint32() =
        let a0 = uint64 buf.[cur+0]
        let a1 = uint64 buf.[cur+1]
        let a2 = uint64 buf.[cur+2]
        let a3 = uint64 buf.[cur+3]
        let r = (a0 <<< 24) ||| (a1 <<< 16) ||| (a2 <<< 8) ||| (a3 <<< 0)
        cur <- cur + 4
        uint32 r

    member this.read_byte_array(len) =
        let ba:byte[] = Array.zeroCreate len
        System.Array.Copy(buf, cur, ba, 0, len)
        cur <- cur + len
        ba

    member this.read_uint16() =
        let a0 = uint64 buf.[cur+0]
        let a1 = uint64 buf.[cur+1]
        let r = (a0 <<< 8) ||| (a1 <<< 0)
        cur <- cur + 2
        uint16 r

    member this.read_varint() =
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
    let _pairs = new System.Collections.Generic.Dictionary<byte[],Stream>()

    let open_cursor() = 
        let cur = ref -1
        let _keys:byte[][] = (Array.ofSeq _pairs.Keys)
        let sortfunc x y = ByteComparer.cmp(x, y)
        Array.sortInPlaceWith sortfunc _keys

        let rec search k min max sop le ge = 
            if max < min then
                if sop = SeekOp.SEEK_EQ then -1
                else if sop = SeekOp.SEEK_LE then le
                else ge
            else
                let mid = (max + min) / 2
                let kmid = _keys.[mid]
                let cmp = ByteComparer.cmp (kmid, k)
                if 0 = cmp then mid
                else if cmp<0  then search k (mid+1) max sop mid ge
                else search k min (mid-1) sop le mid

        { new ICursor with
            member this.IsValid() =
                (!cur >= 0) && (!cur < _keys.Length)

            member this.First() =
                cur := 0

            member this.Last() =
                cur := _keys.Length - 1
            
            member this.Next() =
                cur := !cur + 1

            member this.Prev() =
                cur := !cur - 1

            member this.Key() =
                _keys.[!cur]
            
            member this.KeyCompare(k) =
                ByteComparer.cmp(_keys.[!cur], k)

            member this.Value() =
                let v = _pairs.[_keys.[!cur]]
                if v <> null then ignore (v.Seek(0L, SeekOrigin.Begin))
                v

            member this.ValueLength() =
                let v = _pairs.[_keys.[!cur]]
                if v <> null then (int v.Length) else -1

            member this.Seek (k, sop) =
                cur := search k 0 (_keys.Length-1) sop -1 -1
        }

    interface IWrite with
        member this.Insert (k:byte[], s:Stream) =
            _pairs.[k] <- s

        member this.Delete (k:byte[]) =
            _pairs.[k] <- null

        member this.OpenCursor() =
            open_cursor()

    static member Create() =
        new MemorySegment()

type private Direction = FORWARD=0 | BACKWARD=1 | WANDERING=2

type MultiCursor(others:IEnumerable<ICursor>) =
    let _csrs = List.ofSeq others
    let mutable _cur:ICursor option = None
    let mutable _dir = Direction.WANDERING

    let valid_sorted sortfunc = 
        let valids = List.filter (fun (csr:ICursor) -> csr.IsValid()) _csrs
        let sorted = List.sortWith sortfunc valids
        sorted

    let find_cur sortfunc = 
        let vs = valid_sorted sortfunc
        if vs.IsEmpty then None
        else Some vs.Head

    let find_min() = 
        let sortfunc (a:ICursor) (b:ICursor) = a.KeyCompare(b.Key())
        find_cur sortfunc
    
    let find_max() = 
        let sortfunc (a:ICursor) (b:ICursor) = b.KeyCompare(a.Key())
        find_cur sortfunc

    static member create([<ParamArray>] others: ICursor[]) :ICursor =
        upcast (MultiCursor others)
               
    interface ICursor with
        member this.IsValid() = 
            match _cur with
            | Some csr -> csr.IsValid()
            | None -> false

        member this.First() =
            let f (x:ICursor) = x.First()
            List.iter f _csrs
            _cur <- find_min()

        member this.Last() =
            let f (x:ICursor) = x.Last()
            List.iter f _csrs
            _cur <- find_max()

        member this.Key() = _cur.Value.Key()
        member this.KeyCompare(k) = _cur.Value.KeyCompare(k)
        member this.Value() = _cur.Value.Value()
        member this.ValueLength() = _cur.Value.ValueLength()

        member this.Next() =
            let k = _cur.Value.Key()
            let f (csr:ICursor) :unit = 
                if (_dir <> Direction.FORWARD) && (csr <> _cur.Value) then csr.Seek (k, SeekOp.SEEK_GE)
                if csr.IsValid() && (0 = csr.KeyCompare(k)) then csr.Next()
            List.iter f _csrs
            _cur <- find_min()
            _dir <- Direction.FORWARD

        member this.Prev() =
            let k = _cur.Value.Key()
            let f (csr:ICursor) :unit = 
                if (_dir <> Direction.BACKWARD) && (csr <> _cur.Value) then csr.Seek (k, SeekOp.SEEK_LE)
                if csr.IsValid() && (0 = csr.KeyCompare(k)) then csr.Prev()
            List.iter f _csrs
            _cur <- find_max()
            _dir <- Direction.BACKWARD

        member this.Seek (k, sop) =
            _cur <- None
            let f (csr:ICursor) :unit =
                csr.Seek (k, sop)
                if _cur.IsNone && csr.IsValid() && ( (SeekOp.SEEK_EQ = sop) || (0 = csr.KeyCompare (k)) ) then _cur <- Some csr
                // TODO if found stop
            List.iter f _csrs
            _dir <- Direction.WANDERING
            if _cur.IsNone then
                if SeekOp.SEEK_GE = sop then
                    _cur <- find_min()
                    if _cur.IsSome then _dir <- Direction.FORWARD
                else if SeekOp.SEEK_LE = sop then
                    _cur <- find_max()
                    if _cur.IsSome then _dir <- Direction.BACKWARD


type LivingCursor(ch:ICursor) =
    let chain = ch

    let next_until() =
        while (chain.IsValid() && (chain.ValueLength() < 0)) do
            chain.Next()

    let prev_until() =
        while (chain.IsValid() && (chain.ValueLength() < 0)) do
            chain.Prev()

    interface ICursor with
        member this.First() = 
            chain.First()
            next_until()

        member this.Last() = 
            chain.Last()
            prev_until()

        member this.Key() =
            chain.Key()

        member this.Value() = 
            chain.Value()

        member this.ValueLength() =
            chain.ValueLength()

        member this.IsValid() =
            chain.IsValid() && (chain.ValueLength() > 0)

        member this.Next() =
            chain.Next()
            next_until()

        member this.Prev() =
            chain.Prev()
            prev_until()
        
        member this.KeyCompare k =
            chain.KeyCompare k

        member this.Seek (k, sop) =
            chain.Seek (k, sop)
            if SeekOp.SEEK_GE = sop then next_until() else if SeekOp.SEEK_LE = sop then prev_until()

module varint =
    let space_needed v :int = 
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
        let bytes_per_overflow_page = PAGE_SIZE - OVERFLOW_PAGE_HEADER_SIZE
        let pages = len / bytes_per_overflow_page
        let extra = if (len % bytes_per_overflow_page) <> 0 then 1 else 0
        let this_overflow_needed = pages + extra
        this_overflow_needed

    let private write_byte_array_with_length (pb:PageBuilder) (ba:byte[]) =
        if null = ba then
            pb.PutByte(FLAG_TOMBSTONE)
            pb.write_varint(0UL)
        else
            pb.PutByte(0uy)
            pb.write_varint(uint64 ba.Length)
            pb.write_byte_array(ba)

    let private write_byte_stream_with_length (pb:PageBuilder) (ba:Stream) =
        if null = ba then
            pb.PutByte(FLAG_TOMBSTONE)
            pb.write_varint(0UL)
        else
            pb.PutByte(0uy)
            pb.write_varint(uint64 ba.Length)
            pb.write_byte_stream(ba, int ba.Length)

    let private write_overflow_from_stream (pb:PageBuilder) (fs:Stream) (ba:Stream) =
        pb.Reset()
        let mutable sofar = 0
        let needed = countOverflowPagesFor (int ba.Length)
        let mutable count = 0;
        while sofar < int ba.Length do
            pb.Reset()
            pb.PutByte(OVERFLOW_NODE)
            pb.PutByte(0uy)
            pb.write_uint32(uint32 (needed - count))
            let num = Math.Min((PAGE_SIZE - OVERFLOW_PAGE_HEADER_SIZE), ((int ba.Length) - sofar))
            pb.write_byte_stream(ba, num)
            sofar <- sofar + num
            pb.Flush(fs)
            count <- count + 1
        count

    let private write_overflow_from_bytearray (pb:PageBuilder) (fs:Stream) (ba:byte[]) =
        write_overflow_from_stream pb fs (new MemoryStream(ba))

    let private build_parent_page root first_leaf last_leaf (overflows:System.Collections.Generic.Dictionary<int,uint32>) (pb:PageBuilder) (children:System.Collections.Generic.List<uint32 * byte[]>) stop start =
        let count_keys = stop - start
        pb.Reset ()
        pb.PutByte (PARENT_NODE)
        pb.PutByte (if root then FLAG_ROOT_NODE else 0uy)
        pb.write_uint16 (uint16 count_keys)
        if root then
            pb.write_uint32(first_leaf)
            pb.write_uint32(last_leaf)
        for q in start .. stop do
            pb.write_varint(uint64 (fst (children.[q])))
        for q in start .. (stop-1) do
            let k = snd children.[q]
            if ((overflows <> null) && overflows.ContainsKey (q)) then
                pb.PutByte(FLAG_OVERFLOW)
                pb.write_varint(uint64 k.Length)
                pb.write_uint32(overflows.[q])
            else
                write_byte_array_with_length pb k

    let private calcAvailable current_size could_be_root =
        let basic_size = PAGE_SIZE - current_size
        if could_be_root then
            basic_size - 2 * 4
        else
            basic_size

    let private write_parent_nodes first_leaf last_leaf (children:System.Collections.Generic.List<uint32 * byte[]>) next_page_number fs pb =
        let next_generation = new System.Collections.Generic.List<uint32 * byte[]>()
        let mutable current_size = 0
        let mutable curpage = next_page_number
        let overflows = new System.Collections.Generic.Dictionary<int,uint32>()
        let mutable first = 0
        for i in 0 .. children.Count-1 do
            let (pagenum,k) = children.[i]
            let needed_for_inline = 1 + varint.space_needed (uint64 k.Length) + k.Length + varint.space_needed (uint64 pagenum)
            let needed_for_overflow = 1 + varint.space_needed (uint64 k.Length) + 4 + varint.space_needed (uint64 pagenum)
            let b_last_child = (i = (children.Count - 1))
            if (current_size > 0) then
                let mutable flush_this_page = false
                if b_last_child then flush_this_page <- true
                else if (calcAvailable current_size (next_generation.Count = 0) >= needed_for_inline) then ()
                else if ((PAGE_SIZE - PARENT_NODE_HEADER_SIZE) >= needed_for_inline) then flush_this_page <- true
                else if (calcAvailable current_size (next_generation.Count = 0) < needed_for_overflow) then flush_this_page <- true
                let b_this_is_the_root_node = b_last_child && (next_generation.Count = 0)
                if flush_this_page then
                    build_parent_page b_this_is_the_root_node first_leaf last_leaf overflows pb children i first
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
                    let k_overflow_page_number = curpage
                    let k_overflow_page_count = write_overflow_from_bytearray pb fs k
                    curpage <- curpage + uint32 k_overflow_page_count
                    current_size <- current_size + 4
                    overflows.[i] <- k_overflow_page_number
                current_size <- current_size + 1 + varint.space_needed(uint64 k.Length) + varint.space_needed(uint64 pagenum)
        next_generation

    let Create(fs:Stream, csr:ICursor) :uint32 = 
        let pb = new PageBuilder(PAGE_SIZE)
        let pb2 = new PageBuilder(PAGE_SIZE)
        let mutable next_page_number:uint32 = 1u
        let mutable prev_page_number:uint32 = 0u
        let mutable count_pairs = 0
        let mutable last_key:byte[] = null
        let mutable nodelist = new System.Collections.Generic.List<uint32 * byte[]>()
        csr.First()
        while csr.IsValid() do
            let k = csr.Key()
            let v = csr.Value()
            let needed_overflow_page_number = 4 // TODO sizeof uint32

            let needed_k_base = 1 + varint.space_needed(uint64 k.Length)
            let needed_k_inline = needed_k_base + k.Length
            let needed_k_overflow = needed_k_base + needed_overflow_page_number

            let needed_v_inline = 1 + if v<>null then varint.space_needed(uint64 v.Length) + int v.Length else 0
            let needed_v_overflow = 1 + if v<>null then varint.space_needed(uint64 v.Length) + needed_overflow_page_number else 0

            let needed_for_inline_both = needed_k_inline + needed_v_inline
            let needed_for_inline_key_overflow_val = needed_k_inline + needed_v_overflow
            let needed_for_overflow_both = needed_k_overflow + needed_v_overflow

            csr.Next()

            if pb.Position > 0 then
                let avail = pb.Available()
                let mutable flush_this_page = false
                if (avail >= needed_for_inline_both) then ()
                else if ((PAGE_SIZE - LEAF_HEADER_SIZE) >= needed_for_inline_both) then flush_this_page <- true
                else if (avail >= needed_for_inline_key_overflow_val) then ()
                else if (avail < needed_for_overflow_both) then flush_this_page <- true
                if flush_this_page then
                    pb.write_uint16_at (OFFSET_COUNT_PAIRS, uint16 count_pairs)
                    pb.Flush(fs)
                    nodelist.Add(next_page_number, last_key)
                    prev_page_number <- next_page_number
                    next_page_number <- next_page_number + uint32 1
                    pb.Reset()
                    count_pairs <- 0
                    last_key <- null
            if pb.Position = 0 then
                count_pairs <- 0
                last_key <- null
                pb.PutByte(LEAF_NODE)
                pb.PutByte(0uy) // flags

                pb.write_uint32 (prev_page_number) // prev page num.
                pb.write_uint16 (0us) // number of pairs in this page. zero for now. written at end.
            let available = pb.Available()
            if (available >= needed_for_inline_both) then
                write_byte_array_with_length pb k
                write_byte_stream_with_length pb v
            else
                if (available >= needed_for_inline_key_overflow_val) then
                    write_byte_array_with_length pb k
                else
                    let k_overflow_page_number = next_page_number
                    let k_overflow_page_count = write_overflow_from_bytearray pb2 fs k
                    next_page_number <- next_page_number + uint32 k_overflow_page_count
                    pb.PutByte(FLAG_OVERFLOW)
                    pb.write_varint(uint64 k.Length)
                    pb.write_uint32(k_overflow_page_number)

                let v_overflow_page_number = next_page_number
                let v_overflow_page_count = write_overflow_from_stream pb2 fs v
                next_page_number <- next_page_number + uint32 v_overflow_page_count
                pb.PutByte(FLAG_OVERFLOW)
                pb.write_varint(uint64 v.Length)
                pb.write_uint32(v_overflow_page_number)
            last_key <- k
            count_pairs <- count_pairs + 1
        if pb.Position > 0 then
            pb.write_uint16_at (OFFSET_COUNT_PAIRS, uint16 count_pairs)
            pb.Flush(fs)
            nodelist.Add(next_page_number, last_key)
        if nodelist.Count > 0 then
            let foo = nodelist.[0]
            let first_leaf = fst nodelist.[0]
            let last_leaf = fst nodelist.[nodelist.Count-1]
            while nodelist.Count > 1 do
                nodelist <- write_parent_nodes first_leaf last_leaf nodelist next_page_number fs pb
                next_page_number <- next_page_number + uint32 nodelist.Count
            fst nodelist.[0]
        else
            uint32 0

    type private BTreeOverflowReadStream(strm:Stream, first:uint32, len:int) =
        inherit Stream()
        let _fs = strm
        let _len = len
        let mutable curpage = first
        let buf:byte[] = Array.zeroCreate PAGE_SIZE
        let mutable sofar_thispage = 0
        let mutable sofar_overall = 0

        let ReadPage() =
            let pos = (curpage - 1u) * uint32 PAGE_SIZE
            _fs.Seek(int64 pos, SeekOrigin.Begin) |> ignore
            utils.ReadFully(_fs, buf, 0, PAGE_SIZE) |> ignore
            sofar_thispage <- 0

        do ReadPage()

        override this.Length = int64 _len
        override this.CanRead = sofar_overall < _len

        override this.Read(ba,offset,wanted) =
            if sofar_overall >= _len then
                0
            else    
                if (sofar_thispage >= (PAGE_SIZE - OVERFLOW_PAGE_HEADER_SIZE)) then
                    curpage <- curpage + 1u
                    ReadPage()
                let available = Math.Min ((PAGE_SIZE - OVERFLOW_PAGE_HEADER_SIZE), _len - sofar_overall)
                let num = Math.Min (available, wanted)
                System.Array.Copy (buf, OVERFLOW_PAGE_HEADER_SIZE + sofar_thispage, ba, offset, num)
                sofar_overall <- sofar_overall + num
                sofar_thispage <- sofar_thispage + num
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

    let private read_overflow len fs (first_page:uint32) =
        let ostrm = new BTreeOverflowReadStream(fs, first_page, len)
        utils.ReadAll(ostrm)

    type private my_cursor(strm,length:int64) =
        let fs = strm
        let fs_length = length
        let pr = new PageReader(PAGE_SIZE)

        let mutable cur_page:uint32 = 0u
        let mutable leaf_keys:int[] = null
        let mutable prev_leaf:uint32 = 0u
        let mutable cur_key = -1

        let leaf_reset() =
            leaf_keys <- null
            prev_leaf <- 0u
            cur_key <- -1

        do leaf_reset()

        let leaf_forward() =
            if (cur_key+1) < leaf_keys.Length then
                cur_key <- cur_key + 1
                true
            else
                false

        let leaf_backward() =
            if (cur_key > 0) then
                cur_key <- cur_key - 1
                true
            else
                false

        let skip_key() =
            let kflag = pr.read_byte()
            let klen = pr.read_varint()
            if 0uy = (kflag &&& FLAG_OVERFLOW) then
                pr.Skip(int klen)
            else
                pr.Skip(4)

        let skip_value() =
            let vflag = pr.read_byte()
            let vlen = pr.read_varint()
            if 0uy <> (vflag &&& FLAG_TOMBSTONE) then ()
            else if 0uy <> (vflag &&& FLAG_OVERFLOW) then pr.Skip(4)
            else pr.Skip(int vlen)

        let leaf_read() =
            leaf_reset()
            pr.Reset()
            if pr.read_byte() <> LEAF_NODE then 
                raise (new Exception())
            pr.read_byte() |> ignore
            prev_leaf <- uint32 (pr.read_uint32())
            let count = pr.read_uint16() |> int
            leaf_keys <- Array.zeroCreate count
            for i in 0 .. (count-1) do
                let pos = pr.Position
                leaf_keys.[i] <- pr.Position

                skip_key()
                skip_value()

        let leaf_cmp_key n other = 
            pr.SetPosition(leaf_keys.[n])
            let kflag = pr.read_byte()
            let klen = pr.read_varint() |> int
            if 0uy = (kflag &&& FLAG_OVERFLOW) then
                pr.cmp(klen, other)
            else
                let pagenum = pr.read_uint32()
                let k = read_overflow klen fs pagenum
                ByteComparer.cmp (k, other)

        let leaf_key n = 
            pr.SetPosition(leaf_keys.[n])
            let kflag = pr.read_byte()
            let klen = pr.read_varint() |> int
            if 0uy = (kflag &&& FLAG_OVERFLOW) then
                pr.read_byte_array(klen)
            else
                let pagenum = pr.read_uint32()
                let k = read_overflow klen fs pagenum
                k

        let rec leaf_search k min max sop le ge = 
            if max < min then
                if sop = SeekOp.SEEK_EQ then -1
                else if sop = SeekOp.SEEK_LE then le
                else ge
            else
                let mid = (max + min) / 2
                let kmid = leaf_key(mid)
                let cmp = leaf_cmp_key mid k
                if 0 = cmp then mid
                else if cmp<0  then leaf_search k (mid+1) max sop mid ge
                else leaf_search k min (mid-1) sop le mid

        let set_current_page (pagenum:uint32) = 
            cur_page <- pagenum
            leaf_reset()
            if 0u = cur_page then false
            else
                let pos = (cur_page - 1u) * uint32 PAGE_SIZE
                if ((pos + uint32 PAGE_SIZE) <= uint32 fs_length) then
                    fs.Seek (int64 pos, SeekOrigin.Begin) |> ignore
                    pr.Read(fs) |> ignore
                    true
                else
                    false
                    
        let start_rootpage_read() =
            pr.Reset()
            if pr.read_byte() <> PARENT_NODE then 
                raise (new Exception())
            let pflag = pr.read_byte()
            pr.read_uint16() |> ignore
            if 0uy = (pflag &&& FLAG_ROOT_NODE) then 
                raise (new Exception())
                      
        let get_rootpage_first_leaf() =
            start_rootpage_read()
            let rootpage_first_leaf = pr.read_uint32()
            let rootpage_last_leaf = pr.read_uint32()
            rootpage_first_leaf

        let get_rootpage_last_leaf() =
            start_rootpage_read()
            let rootpage_first_leaf = pr.read_uint32()
            let rootpage_last_leaf = pr.read_uint32()
            rootpage_last_leaf

        let parentpage_read() =
            pr.Reset()
            if pr.read_byte() <> PARENT_NODE then 
                raise (new Exception())
            let pflag = pr.read_byte()
            let count = pr.read_uint16()
            let my_ptrs:uint32[] = Array.zeroCreate (int (count+1us))
            let my_keys:byte[][] = Array.zeroCreate (int count)
            if 0uy <> (pflag &&& FLAG_ROOT_NODE) then pr.Skip(8)
            for i in 0 .. int count do
                my_ptrs.[i] <-  pr.read_varint() |> uint32
            for i in 0 .. int (count-1us) do
                let kflag = pr.read_byte()
                let klen = pr.read_varint() |> int
                if 0uy = (kflag &&& FLAG_OVERFLOW) then
                    my_keys.[i] <- pr.read_byte_array(klen)
                else
                    let pagenum = pr.read_uint32()
                    my_keys.[i] <- read_overflow klen fs pagenum
            (my_ptrs,my_keys)

        let rec search_forward_for_leaf() = 
            let pt = pr.page_type()
            if pt = LEAF_NODE then true
            else if pt = PARENT_NODE then false
            else
                pr.SetPosition(2)
                let skip = pr.read_uint32()
                if set_current_page (cur_page + skip) then
                    search_forward_for_leaf()
                else
                    false

        let leaf_valid() =
            let ok = (leaf_keys <> null) && (leaf_keys.Length > 0) && (cur_key >= 0) && (cur_key < leaf_keys.Length)
            ok

        let rec search_ptrs k (my_ptrs:uint32[]) (my_keys:byte[][]) (i:uint32) :uint32 =
            let cmp = ByteComparer.cmp (k, my_keys.[int i])
            if cmp>0 then
                search_ptrs k my_ptrs my_keys (i+1u)
            else
                my_ptrs.[int i]

        let rec search pg k sop =
            if set_current_page pg then
                if LEAF_NODE = pr.page_type() then
                    leaf_read()
                    cur_key <- leaf_search k 0 (leaf_keys.Length - 1) sop -1 -1
                    if SeekOp.SEEK_EQ <> sop then
                        if not (leaf_valid()) then
                            if SeekOp.SEEK_GE = sop then
                                if (set_current_page (cur_page + 1u) && search_forward_for_leaf ()) then
                                    leaf_read()
                                    cur_key <- 0
                            else
                                if 0u = prev_leaf then
                                    leaf_reset()
                                else if set_current_page prev_leaf then
                                    leaf_read()
                                    cur_key <- leaf_keys.Length - 1
                else if PARENT_NODE = pr.page_type() then
                    let (my_ptrs,my_keys) = parentpage_read()
                    let found = search_ptrs k my_ptrs my_keys 0u
                    if 0u = found then
                        search my_ptrs.[my_ptrs.Length - 1] k sop
                    else
                        search found k sop

        interface ICursor with
            member this.IsValid() =
                leaf_valid()

            member this.Seek(k,sop) =
                let pagenum = uint32 (fs_length / int64 PAGE_SIZE)
                search pagenum k sop

            member this.Key() =
                leaf_key cur_key
            
            member this.Value() =
                pr.SetPosition(leaf_keys.[cur_key])

                skip_key()

                let vflag = pr.read_byte()
                let vlen = pr.read_varint() |> int
                if 0uy <> (vflag &&& FLAG_TOMBSTONE) then null
                else if 0uy <> (vflag &&& FLAG_OVERFLOW) then 
                    let pagenum = pr.read_uint32()
                    upcast (new BTreeOverflowReadStream(fs, pagenum, vlen))
                else 
                    upcast (new MemoryStream(pr.read_byte_array (vlen)))

            member this.ValueLength() =
                pr.SetPosition(leaf_keys.[cur_key])

                skip_key()

                let vflag = pr.read_byte()
                if 0uy <> (vflag &&& FLAG_TOMBSTONE) then -1
                else
                    let vlen = pr.read_varint() |> int
                    vlen

            member this.KeyCompare(k) =
                leaf_cmp_key cur_key k

            member this.First() =
                let pagenum = uint32 (fs_length / int64 PAGE_SIZE)
                if set_current_page pagenum then
                    let pt = pr.page_type()
                    if LEAF_NODE <> pt then
                        let pg = get_rootpage_first_leaf()
                        set_current_page pg |> ignore // TODO
                    leaf_read()
                    cur_key <- 0

            member this.Last() =
                let pagenum = uint32 (fs_length / int64 PAGE_SIZE)
                if set_current_page pagenum then
                    if LEAF_NODE <> pr.page_type() then
                        let pg = get_rootpage_last_leaf()
                        set_current_page pg |> ignore // TODO
                    leaf_read()
                    cur_key <- leaf_keys.Length - 1

            member this.Next() =
                if not (leaf_forward()) then
                    if set_current_page (cur_page + 1u) && search_forward_for_leaf() then
                        leaf_read()
                        cur_key <- 0

            member this.Prev() =
                if not (leaf_backward()) then
                    if 0u = prev_leaf then
                        leaf_reset()
                    else if set_current_page prev_leaf then
                        leaf_read()
                        cur_key <- leaf_keys.Length - 1
    
    let OpenCursor(strm, length:int64) :ICursor =
        upcast (new my_cursor(strm, length))

