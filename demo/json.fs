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

// Copyright (c) Stephan Tolksdorf 2008-2011
// License: Simplified BSD License. See accompanying documentation.

namespace Zumero

module JsonStuff =

    open System
    open System.IO
    open System.Text
    open FParsec

    let to_utf8 (s:string) =
        System.Text.Encoding.UTF8.GetBytes (s) // null/zero terminator NOT included

    let from_utf8 (ba:byte[]) =
        System.Text.Encoding.UTF8.GetString (ba, 0, ba.Length)

    type JsonValue = 
        | JString of string
        | JInteger of int64
        | JFloat of float
        | JNumberString of string
        | JBoolean   of bool
        | JNull
        | JArray   of JsonValue[]
        | JObject of (string*JsonValue)[] // TODO Map

    type PathElement =
        | Key of string
        | Index of int

    let rec flatten fn path jv =
        match jv with
        | JsonValue.JObject a -> 
            for (k,v) in a do
                let newpath = (PathElement.Key k) :: path
                flatten fn newpath v
        | JsonValue.JArray a -> 
            for i in 0 .. a.Length-1 do
                let newpath = (PathElement.Index i) :: path
                let v = a.[i]
                flatten fn newpath v
        | _ -> fn path jv

    // some abbreviations
    let ws   = spaces // eats any whitespace
    let str s = pstring s

    let stringLiteral =
        let escape =  anyOf "\"\\/bfnrt"
                      |>> function
                          | 'b' -> "\b"
                          | 'f' -> "\u000C"
                          | 'n' -> "\n"
                          | 'r' -> "\r"
                          | 't' -> "\t"
                          | c   -> string c // every other char is mapped to itself

        let unicodeEscape =
            str "u" >>. pipe4 hex hex hex hex (fun h3 h2 h1 h0 ->
                let hex2int c = (int c &&& 15) + (int c >>> 6)*9 // hex char to int
                (hex2int h3)*4096 + (hex2int h2)*256 + (hex2int h1)*16 + hex2int h0
                |> char |> string
            )

        between (str "\"") (str "\"")
                (stringsSepBy (manySatisfy (fun c -> c <> '"' && c <> '\\'))
                              (str "\\" >>. (escape <|> unicodeEscape)))

    let jstring = stringLiteral |>> JString

    // -?[0-9]+(\.[0-9]*)?([eE][+-]?[0-9]+)?
    let numberFormat =     NumberLiteralOptions.AllowMinusSign
                       ||| NumberLiteralOptions.AllowFraction
                       ||| NumberLiteralOptions.AllowExponent

    let jnumber : Parser<JsonValue, unit> =
        numberLiteral numberFormat "number"
        |>> fun nl ->
            try
                // TODO if the number is 25.0 is IsInteger true?
                if nl.IsInteger then JInteger (int64 nl.String)
                else JFloat (float nl.String)
            with
            | :? System.OverflowException as e ->
                JNumberString (nl.String)

    let jtrue  = stringReturn "true"  (JBoolean true)
    let jfalse = stringReturn "false" (JBoolean false)
    let jnull  = stringReturn "null" JNull

    // jvalue, jlist and jobject are three mutually recursive grammar productions.
    // In order to break the cyclic dependency, we make jvalue a parser that
    // forwards all calls to a parser in a reference cell.
    let jvalue, jvalueRef = createParserForwardedToRef() // initially jvalueRef holds a reference to a dummy parser

    let listBetweenStrings sOpen sClose pElement f =
        between (str sOpen) (str sClose)
                (ws >>. sepBy (pElement .>> ws) (str "," .>> ws) |>> f)

    let keyValue = tuple2 stringLiteral (ws >>. str ":" >>. ws >>. jvalue)

    let jlist   = listBetweenStrings "[" "]" jvalue (Array.ofList >> JArray)
    let jobject = listBetweenStrings "{" "}" keyValue (Array.ofList >> JObject) // TODO Map

    do jvalueRef := choice [jobject
                            jlist
                            jstring
                            jnumber
                            jtrue
                            jfalse
                            jnull]

    let json = ws >>. jvalue .>> ws .>> eof

    let parseJsonString str = run json str

    // UTF8 is the default, but it will detect UTF16 or UTF32 byte-order marks automatically
    let parseJsonFile fileName encoding =
        runParserOnFile json () fileName System.Text.Encoding.UTF8

    let parseJsonStream stream encoding =
        runParserOnStream json () "" stream System.Text.Encoding.UTF8

    let toJson uv =
        let escape (s:string) (sb:StringBuilder) =
            sb.Append('"') |> ignore
            for i in 0 .. s.Length-1 do
                let c = s.[i]
                match c with
                | '\\' | '"' | '/' -> sb.Append('\\').Append(c) |> ignore
                | '\b' -> sb.Append("\\b") |> ignore
                | '\t' -> sb.Append("\\t") |> ignore
                | '\n' -> sb.Append("\\n") |> ignore
                | '\f' -> sb.Append("\\f") |> ignore
                | '\r' -> sb.Append("\\r") |> ignore
                | _ ->
                    if c < ' ' then
                        let t = "000" + String.Format("X", c)
                        sb.Append("\\u" + t.Substring(t.Length-4)) |> ignore
                    else
                        sb.Append(c) |> ignore
            sb.Append('"') |> ignore

        let rec f (sb:StringBuilder) uv =
            match uv with
            | JsonValue.JBoolean b -> (if b then sb.Append("true") else sb.Append("false")) |> ignore
            | JsonValue.JNull -> sb.Append("null") |> ignore
            | JsonValue.JFloat f -> sb.Append(f.ToString()) |> ignore
            | JsonValue.JNumberString s -> sb.Append(s) |> ignore
            | JsonValue.JInteger i -> sb.Append(i.ToString()) |> ignore
            | JsonValue.JString s -> escape s sb
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

        let sb = StringBuilder()
        f sb uv
        sb.ToString()

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

    let rec toBinary (ms:MemoryStream) jv =
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
        | JsonValue.JNumberString s ->
            ms.WriteByte('H'B)
            encodeString ms s
        | JsonValue.JString s ->
            ms.WriteByte('S'B)
            encodeString ms s
        | JsonValue.JObject a -> 
            ms.WriteByte('{'B)
            for (k,v) in a do
                encodeString ms k
                toBinary ms v
            ms.WriteByte('}'B)
        | JsonValue.JArray a -> 
            ms.WriteByte('['B)
            for i in 0 .. a.Length-1 do
                let v = a.[i]
                toBinary ms v
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
    // but the ub-like format is more compact for small values.  also, if we ever store
    // strings in a munged format so they will order differently, that's a problem.
    // also, JNumberString is not collatable.

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
        | JsonValue.JNumberString n ->
            failwith "JNumberString cannot be used in index keys"
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

    // TODO decimal would be cool, for precision/accounting/etc, but how to format it
    // for lexicographic sort?  Collatable.  and probably nobody else supports this.
    // couch/mongo/raven/documentdb
    // json.net

    // TODO DateTime ?

    // TODO blob?

    type private binaryJsonParser (ub:byte[]) =
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
            | 'H'B -> i <- i + 1; parseString() |> JsonValue.JNumberString
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

    type JsonValue with

        member this.TryGetProperty(propertyName) = 
            match this with
            | JsonValue.JObject properties -> 
                Array.tryFind (fst >> (=) propertyName) properties |> Option.map snd
            | _ -> None

        static member ParseBinary(a:byte[]) =
            binaryJsonParser(a).Parse()

        member this.ToJson() =
            toJson this

        // TODO need ParseCollatable

        member this.ToCollatable (ms:MemoryStream) =
            toCollatable ms this

        member this.ToBinary (ms:MemoryStream) =
            toBinary ms this

