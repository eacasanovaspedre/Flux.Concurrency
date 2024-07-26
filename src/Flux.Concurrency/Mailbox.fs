namespace Flux.Concurrency

open Hopac
open Hopac.Extensions
open Hopac.Infixes

type NackOption = unit Promise option

type 'Letter LetterSender = ('Letter -> unit Job) -> unit Job
type 'Letter LetterCmd = 'Letter LetterSender list

module internal LetterSender =
    let inline internal create f : 'Letter LetterSender = f

    let inline map (f: 'Letter -> 'NewLetter) (letterSender: 'Letter LetterSender) =
        create <| fun send -> letterSender (f >> send)

module LetterCmd =
    let inline create list : 'Letter LetterCmd = list

    let none: 'Letter LetterCmd = []

    let inline ofLetter (letter: 'Letter) = create [ LetterSender.create <| fun send -> send letter ]

    let inline ofLetterJob (letterJob: 'Letter Job) = create [ LetterSender.create <| fun send -> letterJob >>= send ]

    let inline ofLetterOption (letterOption: 'Letter option) =
        create
            [ LetterSender.create
              <| fun send -> letterOption |> Option.map send |> Option.defaultWith Job.unit ]

    let inline ofLetterOptionJob (letterOptionJob: 'Letter option Job) =
        create
            [ LetterSender.create
              <| fun send -> letterOptionJob >>= (Option.map send >> Option.defaultWith Job.unit) ]

    let inline ofLetters (letters: #seq<'Letter>) =
        create [ LetterSender.create <| fun send -> letters |> Seq.Con.iterJob send ]

    let inline ofLetterOptions (letterOptions: #seq<'Letter option>) =
        create
            [ LetterSender.create
              <| fun send ->
                  letterOptions
                  |> Seq.Con.iterJob (Option.map send >> Option.defaultWith Job.unit) ]

    let inline ofLetterJobs (letterJobs: #seq<#Job<'Letter>>) =
        create [ LetterSender.create <| fun send -> letterJobs |> Seq.Con.iterJob (Job.bind send) ]

    let inline ofLetterOptionJobs (letterOptionJobs: #seq<#Job<'Letter option>>) =
        create
            [ LetterSender.create
              <| fun send ->
                  letterOptionJobs
                  |> Seq.Con.iterJob (Job.bind (Option.map send >> Option.defaultWith Job.unit)) ]

    let inline ofAnyJob anyJob = create [ LetterSender.create <| fun send -> anyJob |> Job.Ignore ]

    let inline batch (letterCmds: #seq<'Letter LetterCmd>) = letterCmds |> List.concat |> create

    let inline map (f: 'Letter -> 'NewLetter) (letterCmd: 'Letter LetterCmd) =
        letterCmd |> List.map (LetterSender.map f) |> create

    let inline internal exec send (letterCmd: 'Letter LetterCmd) =
        letterCmd |> Seq.Con.iterJob (fun sender -> sender send) |> Job.queue

type MailboxAgent<'Letter, 'StoppedToken, 'StopToken> =
    private
        { Mailbox: Mailbox<'Letter * NackOption>
          Stopped: 'StoppedToken Alt
          Stop: 'StopToken IVar }

module MailboxAgent =

    type CouldNotSendLetter<'StopToken> = AgentStopping of 'StopToken

    exception CouldNotSendLetterException of Error: obj CouldNotSendLetter

    type LetterOrStop<'Letter, 'StopToken> =
        | Stop of 'StopToken
        | Letter of 'Letter * NackOption

    module Create =
        open Hopac

        let ofAgentFunc agent : MailboxAgent<'Letter, 'StoppedToken, 'StopToken> Job =
            let mailbox = Mailbox ()
            let stopIVar = IVar ()
            let inline takeMsg () = stopIVar ^-> Stop <|> Mailbox.take mailbox ^-> Letter
            let inline sendMsg msg = Mailbox.send mailbox (msg, None)

            (takeMsg, sendMsg) ||> agent |> Promise.start
            >>- fun stopped ->
                { Mailbox = mailbox
                  Stopped = stopped
                  Stop = stopIVar }

        let ofUpdateWithCmds
            (stop: 'StopToken -> Job<'StoppedToken>)
            (init: unit -> Job<'State>)
            (update: NackOption -> 'Letter -> 'State -> 'State * LetterCmd<'Letter>)
            =
            ofAgentFunc
            <| fun take send ->
                let rec loop state =
                    take ()
                    >>= fun msg ->
                        match msg with
                        | Stop token -> stop token
                        | Letter (body, nackOption) ->
                            let state', cmd = update nackOption body state

                            LetterCmd.exec send cmd >>= fun () -> loop state'

                init () >>= loop

        let ofUpdateWithIntents
            (stop: 'StopToken -> Job<'StoppedToken>)
            (init: unit -> Job<'State>)
            (update: NackOption -> 'Letter -> 'State -> 'State * 'Intent list)
            mapIntents
            =
            ofAgentFunc
            <| fun take send ->
                let rec loop state =
                    take ()
                    >>= fun msg ->
                        match msg with
                        | Stop token -> stop token
                        | Letter (body, nackOption) ->
                            let state', intents = update nackOption body state

                            intents |> Seq.map mapIntents |> LetterCmd.batch |> LetterCmd.exec send
                            >>=. loop state'

                init () >>= loop

        module State =
            let ofUpdateWithCmds
                (runState: '``StateMonad<'State, LetterCmd<'Letter>>`` -> 'State -> LetterCmd<'Letter> * 'State)
                (stop: 'StopToken -> Job<'StoppedToken>)
                (init: unit -> Job<'State>)
                (update: NackOption -> 'Letter -> _)
                =
                ofAgentFunc
                <| fun take send ->
                    let rec loop state =
                        take ()
                        >>= fun msg ->
                            match msg with
                            | Stop token -> stop token
                            | Letter (body, nackOption) ->
                                let cmd, state' = runState (update nackOption body) state

                                LetterCmd.exec send cmd >>= fun () -> loop state'

                    init () >>= loop

            let ofUpdateWithIntents
                (runState: '``StateMonad<'State, 'Intent list>`` -> 'State -> 'Intent list * 'State)
                (stop: 'StopToken -> Job<'StoppedToken>)
                (init: unit -> Job<'State>)
                (update: NackOption -> 'Letter -> _)
                mapIntent
                =
                ofAgentFunc
                <| fun take send ->
                    let rec loop state =
                        take ()
                        >>= fun msg ->
                            match msg with
                            | Stop token -> stop token
                            | Letter (body, nackOption) ->
                                let intents, state' = runState (update nackOption body) state

                                intents |> Seq.map mapIntent |> LetterCmd.batch |> LetterCmd.exec send
                                >>=. loop state'

                    init () >>= loop

    let trySend { Mailbox = mailbox; Stop = stop } msg =
        stop ^-> (Error << AgentStopping)
        <|> Alt.prepare (Mailbox.send mailbox (msg, None) >>-. Alt.always (Ok ()))
        |> asJob

    let maybeSend mailboxAgent msg =
        trySend mailboxAgent msg
        >>- function
            | Ok _ -> Some ()
            | Error _ -> None

    let send mailboxAgent msg =
        trySend mailboxAgent msg
        >>- function
            | Ok _ -> ()
            | Error error ->
                match error with
                | AgentStopping token -> token |> box |> AgentStopping |> CouldNotSendLetterException |> raise

    let trySendAndAwaitReply { Mailbox = mailbox; Stop = stop } msgBuilder =
        stop ^-> (Error << AgentStopping)
        <|> (Alt.withNackJob
             <| fun nack ->
                 let replyIVar = IVar ()

                 (msgBuilder replyIVar, Some nack) |> Mailbox.send mailbox >>-. replyIVar ^-> Ok)

    let maybeSendAndAwaitReply mailboxAgent msgBuilder =
        trySendAndAwaitReply mailboxAgent msgBuilder
        ^-> function
            | Ok x -> Some x
            | Error _ -> None

    let sendAndAwaitReply mailboxAgent msgBuilder =
        trySendAndAwaitReply mailboxAgent msgBuilder
        ^-> function
            | Ok x -> x
            | Error error ->
                match error with
                | AgentStopping token -> token |> box |> AgentStopping |> CouldNotSendLetterException |> raise

    let sendStop { Stop = stop } v = IVar.tryFill stop v

    let sendStopAndAwait { Stopped = stopped; Stop = stop } v = IVar.tryFill stop v >>-. stopped |> Alt.prepare

    let stopped { Stopped = stopped } = stopped

    let stopping { Stop = stop } = stop

    let isStopped { Stopped = stopped } = stopped ^->. true <|> Alt.always false

    let isStopping { Stop = stop } = stop ^->. true <|> Alt.always false
