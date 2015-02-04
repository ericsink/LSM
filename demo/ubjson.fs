(*
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

// Parts of this code are based on FSharp.Data/src/Json/JsonValue.fs
// https://github.com/fsharp/FSharp.Data/blob/master/src/Json/JsonValue.fs

// --------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation 2005-2012.
// This sample code is provided "as is" without warranty of any kind.
// We disclaim all warranties, either express or implied, including the
// warranties of merchantability and fitness for a particular purpose.
//
// A simple F# portable parser for JSON data
// --------------------------------------------------------------------------------------

namespace Zumero

module ubjson =

    open System
    open System.IO
    open System.Text
    open json_parser // TODO rename this
    open Zumero.LSM

    let to_utf8 (s:string) =
        System.Text.Encoding.UTF8.GetBytes (s) // null/zero terminator NOT included

    let from_utf8 (ba:byte[]) =
        System.Text.Encoding.UTF8.GetString (ba, 0, ba.Length)

    // TODO decimal would be cool, for precision/accounting/etc, but how to format it
    // for lexicographic sort?  Collatable.  and probably nobody else supports this.
    // couch/mongo/raven/documentdb
    // json.net

    // TODO DateTime ?

    // TODO blob?

    // TODO rename this.  no ub.
    type private ubJsonParser (ub:byte[]) =
        let mutable i = 0
        let ba = ub

        let af:byte[] = Array.zeroCreate 8
        let ad:byte[] = Array.zeroCreate 16

        let throw() =
            let msg = sprintf "Invalid ubJSON starting at %d: byte=%d" i (int (ba.[i]))
            failwith msg
        let ensure cond =
            if not cond then throw()

        let rec parseValue() =
            match ba.[i] with
            | 'i'B -> parseInt8() |> int64 |> JsonValue.JInteger
            | 'U'B -> parseByte() |> int64 |> JsonValue.JInteger
            | 'I'B -> parseInt16() |> int64 |> JsonValue.JInteger
            | 'l'B -> parseInt32() |> int64 |> JsonValue.JInteger
            | 'L'B -> parseInt64() |> int64 |> JsonValue.JInteger
            | 'D'B -> parseFloat() |> JsonValue.JFloat
            | 'c'B -> parseDecimal() |> JsonValue.JDecimal
            | '{'B -> parseRecord()
            | '['B -> parseArray()
            | 't'B -> i <- i + 1; true |> JsonValue.JBoolean
            | 'f'B -> i <- i + 1; false |> JsonValue.JBoolean
            | 'Z'B -> i <- i + 1; JsonValue.JNull
            | 'S'B -> i <- i + 1; parseString() |> JsonValue.JString
            | _ -> throw()

        and parseRootValue() =
            match ba.[i] with
            | '{'B -> parseRecord()
            | '['B -> parseArray()
            | _ -> throw()

        and parseLength() =
            match ba.[i] with
            | 'i'B -> parseInt8() |> int
            | 'U'B -> parseByte() |> int
            | 'I'B -> parseInt16() |> int
            | 'l'B -> parseInt32() |> int
            | 'L'B -> parseInt64() |> int // TODO 64 bit trunc 32
            | _ -> throw()

        and parseDecimal() =
            ensure (ba.[i] = 'c'B)
            Array.Copy(ba, i+1, ad, 0, 16)
            i <- i + 17
            let d = 0m // TODO use constructor with three ints
            d

        and parseFloat() =
            ensure (ba.[i] = 'D'B)
            Array.Copy(ba, i+1, af, 0, 8)
            i <- i + 9
            Array.Reverse af // TODO always?
            let f = BitConverter.ToDouble(af, 0)
            f

        and parseInt8() =
            ensure (ba.[i] = 'i'B)
            let a1 = ba.[i+1]
            i <- i + 2
            a1 |> sbyte

        and parseInt16() =
            ensure (ba.[i] = 'I'B)
            let a1 = ba.[i+1] |> uint64
            let a2 = ba.[i+2] |> uint64
            i <- i + 3
            (a1<<<8) ||| (a2) |> int16

        and parseInt32() =
            ensure (ba.[i] = 'l'B)
            let a1 = ba.[i+1] |> uint64
            let a2 = ba.[i+2] |> uint64
            let a3 = ba.[i+3] |> uint64
            let a4 = ba.[i+4] |> uint64
            i <- i + 5
            (a1<<<24) ||| (a2<<<16) ||| (a3<<<8) ||| (a4) |> int32

        and parseInt64() =
            ensure (ba.[i] = 'L'B)
            let a1 = ba.[i+1] |> uint64
            let a2 = ba.[i+2] |> uint64
            let a3 = ba.[i+3] |> uint64
            let a4 = ba.[i+4] |> uint64
            let a5 = ba.[i+5] |> uint64
            let a6 = ba.[i+6] |> uint64
            let a7 = ba.[i+7] |> uint64
            let a8 = ba.[i+8] |> uint64
            i <- i + 9
            (a1<<<56) ||| (a2<<<48) ||| (a3<<<40) ||| (a4<<<32) ||| (a5<<<24) ||| (a6<<<16) ||| (a7<<<8) ||| (a8) |> int64

        and parseByte() =
            ensure (ba.[i] = 'U'B)
            i <- i + 1
            let v = ba.[i]
            i <- i + 1
            v

        and parseArray() =
            ensure (ba.[i] = '['B)
            i <- i + 1
            let vals = ResizeArray<_>()
            while ba.[i] <> ']'B do
                vals.Add(parseValue())
            ensure (ba.[i] = ']'B)
            i <- i + 1
            JsonValue.JArray (vals.ToArray())

        and parseRecord() =
            ensure (ba.[i] = '{'B)
            i <- i + 1
            let pairs = ResizeArray<_>()
            while ba.[i] <> '}'B do
                pairs.Add(parsePair())
            ensure (ba.[i] = '}'B)
            i <- i + 1
            JsonValue.JObject (pairs.ToArray())

        and parseString() =
            let len = parseLength()
            let s = System.Text.Encoding.UTF8.GetString(ba, i, len)
            i <- i + len
            s

        and parsePair() =
            let k = parseString()
            let v = parseValue()
            (k,v)

        // Start by parsing the top-level value
        member x.Parse() =
            let value = parseRootValue()
            if i <> ba.Length then
                throw()
            value

    let encodeDecimal (ms:MemoryStream) (d:decimal) =
        ms.WriteByte('c'B) // TODO or maybe m (the F# suffix)
        let a = System.Decimal.GetBits(d)
        for i in a do
            let ba = BitConverter.GetBytes(i)
            ms.Write(ba, 0, ba.Length)

    let encodeInteger (ms:MemoryStream) (i64:int64) =
        if i64 >= -128L && i64 <= 127L then
            let v = int8 i64
            ms.WriteByte('i'B)
            ms.WriteByte(byte v)
        else if i64 >= 0L && i64 <= 255L then
            let v = byte i64
            ms.WriteByte('U'B)
            ms.WriteByte(v)
        else if i64 >= (int64 System.Int16.MinValue) && i64 <= (int64 System.Int16.MaxValue) then
            let v = int16 i64
            ms.WriteByte('I'B)
            let ba = BitConverter.GetBytes(v)
            if BitConverter.IsLittleEndian then
                Array.Reverse ba
            ms.Write(ba, 0, ba.Length)
        else if i64 >= (int64 System.Int32.MinValue) && i64 <= (int64 System.Int32.MaxValue) then
            let v = int32 i64
            ms.WriteByte('l'B)
            let ba = BitConverter.GetBytes(v)
            if BitConverter.IsLittleEndian then
                Array.Reverse ba
            ms.Write(ba, 0, ba.Length)
        else
            ms.WriteByte('L'B)
            let ba = BitConverter.GetBytes(i64)
            if BitConverter.IsLittleEndian then
                Array.Reverse ba
            ms.Write(ba, 0, ba.Length)

    let encodeString (ms:MemoryStream) s =
        let ba = to_utf8 s
        encodeInteger ms (ba.Length |> int64)
        ms.Write(ba, 0, ba.Length)

    let toJson uv =
        let sb = StringBuilder()

        let rec f (sb:StringBuilder) uv =
            match uv with
            | JsonValue.JBoolean b -> (if b then sb.Append("true") else sb.Append("false")) |> ignore
            | JsonValue.JNull -> sb.Append("null") |> ignore
            | JsonValue.JFloat f -> sb.Append(f.ToString()) |> ignore
            | JsonValue.JDecimal d -> sb.Append(d.ToString()) |> ignore
            | JsonValue.JInteger i -> sb.Append(i.ToString()) |> ignore
            | JsonValue.JString s -> sb.Append(sprintf "\"%s\"" s) |> ignore // TODO escape
            | JsonValue.JObject a -> 
                sb.Append("{") |> ignore
                for i in 0 .. a.Length-1 do
                    if i > 0 then sb.Append(",") |> ignore
                    let (k,v) = a.[i]
                    sb.Append(sprintf "\"%s\":" k) |> ignore
                    f sb v
                sb.Append("}") |> ignore
            | JsonValue.JArray a -> 
                sb.Append("[") |> ignore
                for i in 0 .. a.Length-1 do
                    if i > 0 then sb.Append(",") |> ignore
                    let v = a.[i]
                    f sb v
                sb.Append("]") |> ignore

        f sb uv
        sb.ToString()

    // TODO rename the following function toBinary           
    let rec encode (ms:MemoryStream) jv =
        match jv with
        | JsonValue.JBoolean b -> if b then ms.WriteByte('T'B) else ms.WriteByte('F'B)
        | JsonValue.JNull -> ms.WriteByte('Z'B)
        | JsonValue.JFloat f ->
            ms.WriteByte('D'B)
            let ba = BitConverter.GetBytes(f)
            if BitConverter.IsLittleEndian then
                Array.Reverse ba
            ms.Write(ba, 0, ba.Length)
        | JsonValue.JInteger i ->
            encodeInteger ms i
        | JsonValue.JDecimal n ->
            let optAsInt64 = try Some (System.Decimal.ToInt64(n)) with :? System.OverflowException -> None
            match optAsInt64 with
            | Some i64 ->
                let dec = decimal i64
                if n = dec then
                    encodeInteger ms i64
                else
                    encodeDecimal ms n
            | None ->
                encodeDecimal ms n
        | JsonValue.JString s ->
                ms.WriteByte('S'B)
                encodeString ms s
        | JsonValue.JObject a -> 
            ms.WriteByte('{'B)
            for (k,v) in a do
                encodeString ms k
                encode ms v
            ms.WriteByte('}'B)
        | JsonValue.JArray a -> 
            ms.WriteByte('['B)
            for i in 0 .. a.Length-1 do
                let v = a.[i]
                encode ms v
            ms.WriteByte(']'B)
          
    let kEndSequence = 0uy
    let kNull = 1uy
    let kFalse = 2uy
    let kTrue = 3uy
    let kNegInt = 4uy
    let kPosInt = 5uy
    let kNegFloat = 6uy
    let kPosFloat = 7uy
    let kNegDecimal = 8uy
    let kPosDecimal = 9uy
    let kString = 10uy
    let kArray = 11uy
    let kRecord = 12uy
    let kSpecial = 13uy
    let kError = 255uy

    // TODO is there any chance this collatable format should just be the format
    // we use for storage?  one problem is that integers here always use 8-9 bytes,
    // but the ub-like format is more compact for small values.

    let rec toCollatable (ms:MemoryStream) jv =
        match jv with
        | JsonValue.JBoolean b -> if b then ms.WriteByte(kTrue) else ms.WriteByte(kFalse)
        | JsonValue.JNull -> ms.WriteByte(kNull)
        | JsonValue.JFloat f ->
            if f < 0.0 then ms.WriteByte(kNegFloat) else ms.WriteByte(kPosFloat)
            let ba = BitConverter.GetBytes(f)
            if BitConverter.IsLittleEndian then
                Array.Reverse ba
            if f < 0.0 then
                for i in 0 .. ba.Length-1 do
                    ba.[i] <- (ba.[i]) ^^^ 255uy
            ms.Write(ba, 0, ba.Length)
        | JsonValue.JInteger i ->
            if i < 0L then ms.WriteByte(kNegInt) else ms.WriteByte(kPosInt)
            let ba = BitConverter.GetBytes(i)
            if BitConverter.IsLittleEndian then
                Array.Reverse ba
            if i < 0L then
                for i in 0 .. ba.Length-1 do
                    ba.[i] <- (ba.[i]) ^^^ 255uy
            ms.Write(ba, 0, ba.Length)
        | JsonValue.JDecimal n ->
            if n < 0m then ms.WriteByte(kNegDecimal) else ms.WriteByte(kPosDecimal)
            let a = System.Decimal.GetBits(n)
            // TODO change endian
            // TODO invert on negative?
            for i in a do
                let ba = BitConverter.GetBytes(i)
                ms.Write(ba, 0, ba.Length)
        | JsonValue.JString s ->
                // TODO do we need to rework this string so it will sort with case insensitivity?
                ms.WriteByte(kString)
                // TODO write this zero-terminated?  or with a length prefix?
                // zero-terminated would allow prefix search
                let ba = to_utf8 s
                ms.Write(ba, 0, ba.Length)
                ms.WriteByte(0uy)
        | JsonValue.JObject a -> 
            ms.WriteByte(kRecord)
            for (k,v) in a do
                let ba = to_utf8 k
                ms.Write(ba, 0, ba.Length)
                ms.WriteByte(0uy)
                toCollatable ms v
            ms.WriteByte(kEndSequence)
        | JsonValue.JArray a -> 
            ms.WriteByte(kArray)
            for i in 0 .. a.Length-1 do
                let v = a.[i]
                toCollatable ms v
            ms.WriteByte(kEndSequence)

    type JsonValue with

        member this.TryGetProperty(propertyName) = 
            match this with
            | JsonValue.JObject properties -> 
                Array.tryFind (fst >> (=) propertyName) properties |> Option.map snd
            | _ -> None

        // TODO ParseBinary
        static member Parse(a:byte[]) =
            ubJsonParser(a).Parse()

        member this.ToJson() =
            toJson this

        member this.ToCollatable (ms:MemoryStream) =
            toCollatable ms this

        member this.Encode (ms:MemoryStream) =
            encode ms this


