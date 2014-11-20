
module fsTests

open System
open System.IO
open Xunit

open Zumero.LSM
open Zumero.LSM.fs

let tid() = 
    let g = Guid.NewGuid().ToString()
    let g2 = g.Replace ("{", "")
    let g3 = g2.Replace ("}", "")
    let g4 = g3.Replace ("-", "")
    g4

[<Fact>]
let empty_cursor() = 
    let f = dbf("empty_cursor_" + tid())
    use db = new Database(f) :> IDatabase
    use csr = db.OpenCursor()
    csr.First ();
    Assert.False (csr.IsValid ());
    csr.Last ();
    Assert.False (csr.IsValid ());

