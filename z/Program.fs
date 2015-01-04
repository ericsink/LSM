// Learn more about F# at http://fsharp.net
// See the 'F# Tutorial' project for more help.

open System
open System.IO
open Microsoft.FSharp.Compiler.SourceCodeServices

// Create an interactive checker instance 
let checker = FSharpChecker.Create(keepAssemblyContents=true)

let parseAndCheckSingleFile (input) = 
    let file = Path.ChangeExtension(System.IO.Path.GetTempFileName(),"fsx")  
    File.WriteAllText(file, input)
    // Get context representing a stand-alone (script) file
    let projOptions = 
        checker.GetProjectOptionsFromScript(file, input)
        |> Async.RunSynchronously

    checker.ParseAndCheckProject(projOptions) 
    |> Async.RunSynchronously

let rec visitExpr f (e:FSharpExpr) = 
    f e
    match e with 
    | BasicPatterns.AddressOf(e1) -> visitExpr f e1
    | BasicPatterns.AddressSet(e1,e2) -> visitExpr f e1; visitExpr f e2
    | BasicPatterns.Application(ef,tyargs,args) -> visitExpr f ef; visitExprs f args
    | BasicPatterns.Call(obj,v,tyargs1,tyargs2,args) -> visitObjArg f obj; visitExprs f args
    | BasicPatterns.Coerce(ty1,e1) -> visitExpr f e1
    | BasicPatterns.FastIntegerForLoop(e1,e2,e3,isUp) -> visitExpr f e1; visitExpr f e2; visitExpr f e3
    | BasicPatterns.ILAsm(s,tyargs,args) -> visitExprs f args
    | BasicPatterns.ILFieldGet (objOpt, fieldType, fieldName) -> visitObjArg f objOpt
    | BasicPatterns.ILFieldSet (objOpt, fieldType, fieldName, ve) -> visitObjArg f objOpt
    | BasicPatterns.IfThenElse (ge,te,ee) -> visitExpr f ge; visitExpr f te; visitExpr f ee
    | BasicPatterns.Lambda(v,body) -> visitExpr f body
    | BasicPatterns.Let((v,ve),body) -> visitExpr f ve; visitExpr f body
    | BasicPatterns.LetRec(vse,body) -> List.iter (snd >> visitExpr f) vse; visitExpr f body
    | BasicPatterns.NewArray(ty,args) -> visitExprs f args
    | BasicPatterns.NewDelegate(ty,arg) -> visitExpr f arg
    | BasicPatterns.NewObject(v,tys,args) -> visitExprs f args
    | BasicPatterns.NewRecord(v,args) ->  visitExprs f args
    | BasicPatterns.NewTuple(v,args) -> visitExprs f args
    | BasicPatterns.NewUnionCase(ty,uc,args) -> visitExprs f args
    | BasicPatterns.Quote(e1) -> visitExpr f e1
    | BasicPatterns.FSharpFieldGet(objOpt, ty,fieldInfo) -> visitObjArg f objOpt
    | BasicPatterns.FSharpFieldSet(objOpt, ty,fieldInfo,arg) -> visitObjArg f objOpt; visitExpr f arg
    | BasicPatterns.Sequential(e1,e2) -> visitExpr f e1; visitExpr f e2
    | BasicPatterns.TryFinally(e1,e2) -> visitExpr f e1; visitExpr f e2
    | BasicPatterns.TryWith(body,_,_,vCatch,eCatch) -> visitExpr f body; visitExpr f eCatch
    | BasicPatterns.TupleGet(ty,n,e1) -> visitExpr f e1
    | BasicPatterns.DecisionTree(dtree,targets) -> visitExpr f dtree; List.iter (snd >> visitExpr f) targets
    | BasicPatterns.DecisionTreeSuccess (tg,es) -> visitExprs f es
    | BasicPatterns.TypeLambda(gp1,body) -> visitExpr f body
    | BasicPatterns.TypeTest(ty,e1) -> visitExpr f e1
    | BasicPatterns.UnionCaseSet(obj,ty,uc,f1,e1) -> visitExpr f obj; visitExpr f e1
    | BasicPatterns.UnionCaseGet(obj,ty,uc,f1) -> visitExpr f obj
    | BasicPatterns.UnionCaseTest(obj,ty,f1) -> visitExpr f obj
    | BasicPatterns.UnionCaseTag(obj,ty) -> visitExpr f obj
    | BasicPatterns.ObjectExpr(ty,basecall,overrides,iimpls) -> 
        visitExpr f basecall
        List.iter (visitObjMember f) overrides
        List.iter (snd >> List.iter (visitObjMember f)) iimpls
    | BasicPatterns.TraitCall(tys,nm,argtys,tinst,args) -> visitExprs f args
    | BasicPatterns.ValueSet(v,e1) -> visitExpr f e1
    | BasicPatterns.WhileLoop(e1,e2) -> visitExpr f e1; visitExpr f e2
    | BasicPatterns.BaseValue _ -> ()
    | BasicPatterns.DefaultValue _ -> ()
    | BasicPatterns.ThisValue _ -> ()
    | BasicPatterns.Const(obj,ty) -> ()
    | BasicPatterns.Value(v) -> 
        printfn "    DeclarationLocation: %A" (v.DeclarationLocation)
        printfn "    CompiledName: %A" (v.CompiledName)
        printfn "    FullName: %A" (v.FullName)
        printfn "    FullType: %A" (v.FullType)
        printfn "    ImplementationLocation: %A" (v.ImplementationLocation)
        printfn "    IsCompilerGenerated: %A" (v.IsCompilerGenerated)
        printfn "    IsActivePattern: %A" (v.IsActivePattern)
        printfn "    IsInstanceMember: %A" (v.IsInstanceMember)
        printfn "    IsMember: %A" (v.IsMember)
        printfn "    IsModuleValueOrMember: %A" (v.IsModuleValueOrMember)
        printfn "    IsMutable: %A" (v.IsMutable)
        printfn "    IsOverrideOrExplicitInterfaceImplementation: %A" (v.IsOverrideOrExplicitInterfaceImplementation)
        printfn "    IsProperty: %A" (v.IsProperty)
        printfn "    IsTypeFunction: %A" (v.IsTypeFunction)
        printfn "    IsUnresolved: %A" (v.IsUnresolved)
        //printfn "    CurriedParameterGroups: %A" (v.CurriedParameterGroups)
        try 
            printfn "    EnclosingEntity: %A" (v.EnclosingEntity)
        with
            | :? System.Exception -> printfn "    EnclosingEntity: %A" "none"
        ()
    | _ -> failwith (sprintf "unrecognized %+A" e)

and visitExprs f exprs = 
    List.iter (visitExpr f) exprs

and visitObjArg f objOpt = 
    Option.iter (visitExpr f) objOpt

and visitObjMember f memb = 
    visitExpr f memb.Body


[<EntryPoint>]
let main argv = 
    let s = File.ReadAllText(argv.[0])
    let checkProjectResults = parseAndCheckSingleFile(s)

    printfn "Errors: %A" (checkProjectResults.Errors)

    let checkedFile = checkProjectResults.AssemblyContents.ImplementationFiles.[0]

    let rec printDecl prefix d = 
        match d with 
        | FSharpImplementationFileDeclaration.Entity (e,subDecls) -> 
            printfn "%sEntity %s was declared and contains %d sub-declarations" prefix e.CompiledName subDecls.Length
            for subDecl in subDecls do 
                printDecl (prefix+"    ") subDecl
        | FSharpImplementationFileDeclaration.MemberOrFunctionOrValue(v,vs,ex) -> 
            printfn "%sMember or value %s was declared" prefix  v.CompiledName
            ex |> visitExpr (fun e -> printfn "Visiting %A" e)
        | FSharpImplementationFileDeclaration.InitAction(ex) -> 
            printfn "%sA top-level expression was declared" prefix 
            ex |> visitExpr (fun e -> printfn "Visiting %A" e)


    for d in checkedFile.Declarations do 
       printDecl "" d

    0 // return an integer exit code

