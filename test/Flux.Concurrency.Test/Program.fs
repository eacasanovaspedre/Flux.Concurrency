module Flux.Concurrency.Test

open Hopac
open Logary.Configuration
open Logary.Adapters.Facade
open Logary.Targets
open Expecto.Tests

[<EntryPoint>]
let main argv =
    // let logary =
    //     Config.create "MyProject.Tests" "localhost"
    //     |> Config.targets [ LiterateConsole.create LiterateConsole.empty "console" ]
    //     |> Config.processing (Events.events |> Events.sink ["console";])
    //     |> Config.build
    //     |> run
    // LogaryFacadeAdapter.initialise<Expecto.Logging.Logger> logary

    runTestsInAssemblyWithCLIArgs [] argv