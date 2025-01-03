module Test.Flux.Concurrency.LoadingCache

open FsCheck
open FsCheck.FSharp
open Expecto
open Hopac
open Hopac.Infixes
open Flux.Concurrency

let (|==>) condition assertion = condition ==> Job.toAsync assertion
let (.=.) left right = left = right |> Prop.label $"%A{left} = %A{right}"
let (==) actual expected = Expect.equal actual expected $"%A{actual} = %A{expected}"
let (!==) actual expected = Expect.notEqual actual expected $"%A{actual} != %A{expected}"

type JobFunction<'T, 'U> = JobFunction of ('T -> Job<'U>)

type Generators =
    static member JobFunction<'T, 'U>() =
        { new Arbitrary<JobFunction<'T, 'U>>() with
            override _.Generator =
                ArbMap.defaults.ArbFor<'T -> 'U>().Generator
                |> Gen.map (fun f -> JobFunction (Job.result << f)) }

let config =
    { FsCheckConfig.defaultConfig with
        FsCheckConfig.arbitrary = [ typeof<Generators> ] }

let loadWhenTriggered f =
    let trigger = Ch ()
    trigger, (fun key -> Ch.take trigger ^->. f key)

let loadWhenTriggeredIVar f =
    let trigger = IVar ()
    trigger, (fun key -> trigger ^->. f key)

let countCalls f =
    let counter = MVar 0
    counter, (fun p -> MVar.mutateFun ((+) 1) counter >>=. f p)

[<Tests>]
let properties =
    testList
        "LoadingCache"
        [ testPropertyWithConfig config "When requesting an new item, the load function must be called to get the value"
          <| fun (NonEmptyString key) (JobFunction f) ->
              job {
                  let! cache = loadingCache { loadWith f }
                  let! expected = f key >>- Result.mapError LoadingCache.CouldNotLoadValue

                  let! result = LoadingCache.maybeFind key cache
                  result == expected
              }
              |> Job.toAsync
          testPropertyWithConfig
              config
              "When requesting the same item multiple times before it's loaded, the load function must be called only once if its result is Ok"
          <| fun (NonEmptyString key) f ->
              job {
                  let trigger, loadFun = loadWhenTriggeredIVar (Ok << f)
                  let counter, loadFunCounted = countCalls loadFun
                  let! cache = loadingCache { loadWith loadFunCounted }
                  let expected = f key |> Ok

                  let! finds =
                      Seq.init 10 (fun _ -> LoadingCache.maybeFind key cache)
                      |> Job.conCollect
                      |> Promise.start

                  do! trigger *<= ()

                  let! results = finds
                  let! callCount = counter
                  callCount == 1


                  Expect.all results ((=) expected) "All the results must be the same"
              }
              |> Job.toAsync ]
