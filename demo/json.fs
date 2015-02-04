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

module json_parser =

    open FParsec

    type JsonValue = 
        | JString of string
        | JInteger of int64
        | JFloat of float
        | JDecimal of decimal // TODO ?
        | JBoolean   of bool
        | JNull
        | JArray   of JsonValue[]
        | JObject of (string*JsonValue)[] // TODO Map


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

    // TODO we could return unrepresentable numbers as a JNumberString?  or decimal.
    let jnumber : Parser<JsonValue, unit> =
        numberLiteral numberFormat "number"
        |>> fun nl ->
                if nl.IsInteger then JInteger (int64 nl.String)
                else JFloat (float nl.String)

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
