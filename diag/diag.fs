
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

