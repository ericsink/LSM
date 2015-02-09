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

namespace Zumero

module SqlStuff =

    open System
    open System.IO
    open System.Text
    open FParsec

    type UserState = unit // doesn't have to be unit, of course
    type Parser<'t> = Parser<'t, UserState>

    let unescaped_identifier :Parser<_> =
        let isIdentifierFirstChar c = isLetter c || c = '_'
        let isIdentifierChar c = isLetter c || isDigit c || c = '_' || c = '$'
        many1Satisfy2L isIdentifierFirstChar isIdentifierChar "unescaped_identifier"

    // TODO escape sequences inside
    let escaped_identifier :Parser<_> = 
        let normalChar = satisfy (fun c -> c <> '`')
        let escapedChar = stringReturn "``" '`'
        between (pstring "`") (pstring "`") (manyChars (normalChar <|> escapedChar))
 
    let stringLiteral :Parser<_> =
        let escape =  anyOf "\"\\/bfnrt"
                      |>> function
                          | 'b' -> "\b"
                          | 'f' -> "\u000C"
                          | 'n' -> "\n"
                          | 'r' -> "\r"
                          | 't' -> "\t"
                          | c   -> string c // every other char is mapped to itself

        let unicodeEscape =
            pstring "u" >>. pipe4 hex hex hex hex (fun h3 h2 h1 h0 ->
                let hex2int c = (int c &&& 15) + (int c >>> 6)*9 // hex char to int
                (hex2int h3)*4096 + (hex2int h2)*256 + (hex2int h1)*16 + hex2int h0
                |> char |> string
            )

        between (pstring "\"") (pstring "\"")
                (stringsSepBy (manySatisfy (fun c -> c <> '"' && c <> '\\')) (pstring "\\" >>. (escape <|> unicodeEscape)))
                              
                             

