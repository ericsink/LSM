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

open Zumero.LSM

module diag =

    let list_segments dbFile =
        let f = dbf(dbFile)
        use db = new Database(f) :> IDatabase
        let segs = db.ListSegments()
        printfn "%A" segs

    [<EntryPoint>]
    let main argv = 
        let dbFile = argv.[0]
        let op = argv.[1]
        match op with
        | "list_segments" -> list_segments dbFile
        | _ -> failwith "Unknown op"

        0 // return an integer exit code

