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
    open FSharp.Data
    open Zumero.LSM

    let to_utf8 (s:string) =
        System.Text.Encoding.UTF8.GetBytes (s)

    let from_utf8 (ba:byte[]) =
        System.Text.Encoding.UTF8.GetString (ba, 0, ba.Length)

    // TODO decimal would be cool, for precision/accounting/etc, but how to format it
    // for lexicographic sort?  Collatable.  and probably nobody else supports this.
    // couch/mongo/raven/documentdb

    //[<RequireQualifiedAccess>]
    type ubJsonValue =
      | String of string
      | Integer of int64
      | Decimal of decimal // TODO not sure we want this.  But maybe.
      | Float of float
      | Record of properties:(string * ubJsonValue)[] // TODO shouldn't this be a dictionary instead of an array of tuples?
      | Array of elements:ubJsonValue[]
      | Boolean of bool
      | Null  

    type private ubJsonParser (ub:byte[]) =
        let mutable i = 0
        let ba = ub

        let af:byte[] = Array.zeroCreate 8

        let throw() =
            let msg = sprintf "Invalid ubJSON starting at %d: byte=%d" i (int (ba.[i]))
            failwith msg
        let ensure cond =
            if not cond then throw()

        let rec parseValue() =
            match ba.[i] with
            | 'i'B -> parseInt8() |> int64 |> ubJsonValue.Integer
            | 'U'B -> parseByte() |> int64 |> ubJsonValue.Integer
            | 'I'B -> parseInt16() |> int64 |> ubJsonValue.Integer
            | 'l'B -> parseInt32() |> int64 |> ubJsonValue.Integer
            | 'L'B -> parseInt64() |> int64 |> ubJsonValue.Integer
            | 'D'B -> parseFloat() |> ubJsonValue.Float
            | 'H'B -> parseH() |> ubJsonValue.Decimal
            | '{'B -> parseRecord()
            | '['B -> parseArray()
            | 't'B -> i <- i + 1; true |> ubJsonValue.Boolean
            | 'f'B -> i <- i + 1; false |> ubJsonValue.Boolean
            | 'Z'B -> i <- i + 1; ubJsonValue.Null
            | 'S'B -> i <- i + 1; parseString() |> ubJsonValue.String
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
            | _ -> throw()

        // TODO not sure we want H
        and parseH() =
            ensure (ba.[i] = 'H'B)
            i <- i + 1
            let s = parseString()
            match TextConversions.AsDecimal (System.Globalization.CultureInfo.InvariantCulture) s with
            | Some d -> d
            | None -> throw()

        and parseFloat() =
            ensure (ba.[i] = 'D'B)
            Array.Copy(ba, i+1, af, 0, 8)
            i <- i + 9
            Array.Reverse af
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
            ubJsonValue.Array (vals.ToArray())

        and parseRecord() =
            ensure (ba.[i] = '{'B)
            i <- i + 1
            let pairs = ResizeArray<_>()
            while ba.[i] <> '}'B do
                pairs.Add(parsePair())
            ensure (ba.[i] = '}'B)
            i <- i + 1
            ubJsonValue.Record (pairs.ToArray())

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

    type ubJsonValue with

      /// Parses the specified ubJSON bytes
      static member Parse(a:byte[]) =
        ubJsonParser(a).Parse()

    let write_decimal (ms:MemoryStream) (d:decimal) =
        ms.WriteByte('c'B)
        let a = System.Decimal.GetBits(d)
        for i in a do
            let ba = BitConverter.GetBytes(i)
            ms.Write(ba, 0, ba.Length)

    let write_integer (ms:MemoryStream) (i64:int64) =
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

    let write_string (ms:MemoryStream) s =
        let ba = to_utf8 s
        write_integer ms (ba.Length |> int64)
        ms.Write(ba, 0, ba.Length)

    let rec toJson (sb:StringBuilder) uv =
        match uv with
        | ubJsonValue.Boolean b -> (if b then sb.Append("true") else sb.Append("false")) |> ignore
        | ubJsonValue.Null -> sb.Append("null") |> ignore
        | ubJsonValue.Float f -> sb.Append(f.ToString()) |> ignore
        | ubJsonValue.Decimal d -> sb.Append(d.ToString()) |> ignore
        | ubJsonValue.Integer i -> sb.Append(i.ToString()) |> ignore
        | ubJsonValue.String s -> sb.Append(sprintf "\"%s\"" s) |> ignore
        | ubJsonValue.Record a -> 
            sb.Append("{") |> ignore
            for i in 0 .. a.Length-1 do
                if i > 0 then sb.Append(",") |> ignore
                let (k,v) = a.[i]
                sb.Append(sprintf "\"%s\":" k) |> ignore
                toJson sb v
            sb.Append("}") |> ignore
        | ubJsonValue.Array a -> 
            sb.Append("[") |> ignore
            for i in 0 .. a.Length-1 do
                if i > 0 then sb.Append(",") |> ignore
                let v = a.[i]
                toJson sb v
            sb.Append("]") |> ignore

    // TODO maybe this should build ubJsonValue objects instead
    let rec toUbjson (ms:MemoryStream) jv =
        match jv with
        | JsonValue.Boolean b -> if b then ms.WriteByte('T'B) else ms.WriteByte('F'B)
        | JsonValue.Null -> ms.WriteByte('Z'B)
        | JsonValue.Float f ->
            ms.WriteByte('D'B)
            let ba = BitConverter.GetBytes(f)
            if BitConverter.IsLittleEndian then
                Array.Reverse ba
            ms.Write(ba, 0, ba.Length)
        | JsonValue.Number n ->
            let optAsInt64 = try Some (System.Decimal.ToInt64(n)) with :? System.OverflowException -> None
            match optAsInt64 with
            | Some i64 ->
                let dec = decimal i64
                if n = dec then
                    write_integer ms i64
                else
                    // TODO try float?
                    ms.WriteByte('H'B)
                    let s = jv.AsString()
                    write_string ms s
                    //write_decimal ms n
            | None ->
                // TODO try float?
                ms.WriteByte('H'B)
                let s = jv.AsString()
                write_string ms s
                //write_decimal ms n
        | JsonValue.String s ->
                ms.WriteByte('S'B)
                let s = jv.AsString()
                write_string ms s
        | JsonValue.Record a -> 
            ms.WriteByte('{'B)
            for (k,v) in a do
                write_string ms k
                toUbjson ms v
            ms.WriteByte('}'B)
        | JsonValue.Array a -> 
            ms.WriteByte('['B)
            for i in 0 .. a.Length-1 do
                let v = a.[i]
                toUbjson ms v
            ms.WriteByte(']'B)
            
    let rec toUbjson2 (ms:MemoryStream) jv =
        match jv with
        | ubJsonValue.Boolean b -> if b then ms.WriteByte('T'B) else ms.WriteByte('F'B)
        | ubJsonValue.Null -> ms.WriteByte('Z'B)
        | ubJsonValue.Float f ->
            ms.WriteByte('D'B)
            let ba = BitConverter.GetBytes(f)
            if BitConverter.IsLittleEndian then
                Array.Reverse ba
            ms.Write(ba, 0, ba.Length)
        | ubJsonValue.Integer i ->
            write_integer ms i
        | ubJsonValue.Decimal n ->
            let optAsInt64 = try Some (System.Decimal.ToInt64(n)) with :? System.OverflowException -> None
            match optAsInt64 with
            | Some i64 ->
                let dec = decimal i64
                if n = dec then
                    write_integer ms i64
                else
                    // TODO try float?
                    ms.WriteByte('H'B)
                    let s = n.ToString()
                    write_string ms s
                    //write_decimal ms n
            | None ->
                // TODO try float?
                ms.WriteByte('H'B)
                let s = n.ToString()
                write_string ms s
                //write_decimal ms n
        | ubJsonValue.String s ->
                ms.WriteByte('S'B)
                write_string ms s
        | ubJsonValue.Record a -> 
            ms.WriteByte('{'B)
            for (k,v) in a do
                write_string ms k
                toUbjson2 ms v
            ms.WriteByte('}'B)
        | ubJsonValue.Array a -> 
            ms.WriteByte('['B)
            for i in 0 .. a.Length-1 do
                let v = a.[i]
                toUbjson2 ms v
            ms.WriteByte(']'B)
            

