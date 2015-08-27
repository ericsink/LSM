// Learn more about F# at http://fsharp.net
// See the 'F# Tutorial' project for more help.

open System
open System.IO
open System.Collections.Generic
open FSharp.Data
open Couchbase
open Couchbase.Lite

module cb =
    let rec convert jv =
        match jv with
        | JsonValue.Boolean b -> box b
        | JsonValue.Float f -> box f
        | JsonValue.Null -> null
        | JsonValue.Number n -> box n
        | JsonValue.String s -> box s
        | JsonValue.Record a -> 
            let d = Dictionary<string,obj>()
            for (k,v) in a do
                d.[k] <- convert v
            d :> obj
        | JsonValue.Array a -> 
            let a2:obj[] = Array.zeroCreate a.Length
            for i in 0 .. a.Length-1 do
                let v = a.[i]
                a2.[i] <- convert v
            a2 :> obj

    type mess() =
        static member beef(doc:IDictionary<string,obj>) (emit:EmitDelegate) =
            let id = doc.["id"]
            let desc = doc.["description"]
            let group = doc.["group"]
            if (group :?> string) = "Beef Products" then
                emit.Invoke(id, desc)

    [<EntryPoint>]
    let main argv = 
        printfn "%A" argv
        let db = Manager.SharedInstance.GetDatabase(argv.[0])
        let jsonFile = argv.[1]
        let json = File.ReadAllText(jsonFile)
        let parsed = JsonValue.Parse(json)
        let a =
            match parsed with
            | JsonValue.Array a -> a
            | _ -> failwith "wrong"

        for i in 0 .. a.Length-1 do
            let jdoc = a.[i]
            let d = convert jdoc
            let dict = d :?> IDictionary<string,obj>
            let doc = db.CreateDocument()
            doc.PutProperties(dict)
        let vw = db.GetView("beef")
        let del = new MapDelegate(mess.beef)
        if vw.SetMap(del, "1") then
            ()
        else
            failwith "setmap failed"
        let q = vw.CreateQuery()
        q.IndexUpdateMode <- IndexUpdateMode.Before
        let res = q.Run()
        for x in res do
            let k = x.Key.ToString()
            let v = x.Value.ToString()
            printfn "%s : %s" k v

        0 // return an integer exit code

