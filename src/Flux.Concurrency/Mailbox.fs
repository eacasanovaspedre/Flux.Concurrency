namespace Flux.Concurrency

open Hopac
open Hopac.Extensions
open Hopac.Infixes

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

    let inline ofAnyJob anyJob = create [ LetterSender.create <| fun _ -> anyJob |> Job.Ignore ]

    let inline batch (letterCmds: #seq<'Letter LetterCmd>) = letterCmds |> List.concat |> create

    let inline map (f: 'Letter -> 'NewLetter) (letterCmd: 'Letter LetterCmd) =
        letterCmd |> List.map (LetterSender.map f) |> create

    let inline internal exec send (letterCmd: 'Letter LetterCmd) =
        letterCmd |> Seq.Con.iterJob (fun sender -> sender send) |> Job.queue

type Nack = Promise<unit>
type nack = Nack

type MailboxAgent<'Letter, 'StoppedToken, 'StopToken> =
    private
        { Mailbox: Mailbox<'Letter>
          Stopped: 'StoppedToken Promise
          Stop: 'StopToken IVar }

type MailboxPackage<'Letter, 'StopToken> =
    | Stop of 'StopToken
    | Envelope of 'Letter

module MailboxAgent =

    type CouldNotSendLetter<'StoppedToken, 'StopToken> =
        | AgentStopping of 'StopToken
        | AgentStopped of 'StoppedToken

    exception CouldNotSendLetterException of Error: CouldNotSendLetter<obj, obj>

    let ofAgentFun agent : MailboxAgent<'Envelope, 'StoppedToken, 'StopToken> Job =
        let mailbox = Mailbox ()
        let stopIVar = IVar ()
        let inline receiveLetter () = stopIVar ^-> Stop <|> Mailbox.take mailbox ^-> Envelope
        let inline sendLetter msg = Mailbox.send mailbox msg

        (receiveLetter, sendLetter) ||> agent |> Promise.start
        >>- fun stopped ->
            { Mailbox = mailbox
              Stopped = stopped
              Stop = stopIVar }

    let ofUpdateWithCmds
        (stop: 'StopToken -> 'State -> #Job<'StoppedToken>)
        (init: unit -> #Job<'State>)
        (update: 'Envelope -> 'State -> 'State * LetterCmd<'Envelope>)
        =
        ofAgentFun
        <| fun take send ->
            let rec loop state =
                take ()
                >>= function
                    | Stop token -> stop token state |> asJob
                    | Envelope letter ->
                        let state', cmd = update letter state

                        LetterCmd.exec send cmd >>= fun () -> loop state'

            init () >>= loop

    let ofUpdateWithIntents
        (stop: 'StopToken -> 'State -> #Job<'StoppedToken>)
        (init: unit -> #Job<'State>)
        (update: 'Envelope -> 'State -> 'State * 'Intent list)
        mapIntents
        =
        ofAgentFun
        <| fun take send ->
            let rec loop state =
                take ()
                >>= fun msg ->
                    match msg with
                    | Stop token -> stop token state |> asJob
                    | Envelope letter ->
                        let state', intents = update letter state

                        intents |> Seq.map mapIntents |> LetterCmd.batch |> LetterCmd.exec send
                        >>=. loop state'

            init () >>= loop

    module State =
        let ofUpdateWithCmds
            (runState: '``StateMonad<'State, LetterCmd<'Envelope>>`` -> 'State -> LetterCmd<'Envelope> * 'State)
            (stop: 'StopToken -> 'State -> #Job<'StoppedToken>)
            (init: unit -> #Job<'State>)
            (update: 'Envelope -> _)
            =
            ofAgentFun
            <| fun take send ->
                let rec loop state =
                    take ()
                    >>= fun msg ->
                        match msg with
                        | Stop token -> stop token state |> asJob
                        | Envelope letter ->
                            let cmd, state' = runState (update letter) state

                            LetterCmd.exec send cmd >>= fun () -> loop state'

                init () >>= loop

        let ofUpdateWithIntents
            (runState: '``StateMonad<'State, 'Intent list>`` -> 'State -> 'Intent list * 'State)
            (stop: 'StopToken -> 'State -> #Job<'StoppedToken>)
            (init: unit -> #Job<'State>)
            (update: 'Envelope -> _)
            mapIntent
            =
            ofAgentFun
            <| fun take send ->
                let rec loop state =
                    take ()
                    >>= fun msg ->
                        match msg with
                        | Stop token -> stop token state |> asJob
                        | Envelope letter ->
                            let intents, state' = runState (update letter) state

                            intents |> Seq.map mapIntent |> LetterCmd.batch |> LetterCmd.exec send
                            >>=. loop state'

                init () >>= loop

    [<RequireQualifiedAccess>]
    module Try =
        let send
            { Mailbox = mailbox
              Stopped = stopped
              Stop = stop }
            letter
            =
            Alt.choosy
                [| stopped ^-> (Error << AgentStopped)
                   stop ^-> (Error << AgentStopping)
                   Alt.prepare (mailbox *<<+ letter >>-. Alt.always (Ok ())) |]

        let sendAndAwaitReply
            { Mailbox = mailbox
              Stopped = stopped
              Stop = stop }
            letterJobBuilder
            =
            Alt.choosy
                [| stopped ^-> (Error << AgentStopped)
                   stop ^-> (Error << AgentStopping)
                   Alt.prepareJob
                   <| fun () ->
                       let replyIVar = IVar ()

                       letterJobBuilder replyIVar
                       >>= fun letter -> mailbox *<<+ letter >>-. replyIVar ^-> Ok |]

        [<RequireQualifiedAccess>]
        module WithNack =
            let sendAndAwaitReply
                { Mailbox = mailbox
                  Stopped = stopped
                  Stop = stop }
                letterJobBuilder
                =
                Alt.choosy
                    [| stopped ^-> (Error << AgentStopped)
                       stop ^-> (Error << AgentStopping)
                       Alt.withNackJob
                       <| fun nack ->
                           let replyCh = Ch ()

                           letterJobBuilder replyCh nack
                           >>= fun letter -> mailbox *<<+ letter >>-. replyCh ^-> Ok |]

    [<RequireQualifiedAccess>]
    module WithNack =
        let sendAndAwaitReply mailboxAgent letterJobBuilder =
            Try.WithNack.sendAndAwaitReply mailboxAgent letterJobBuilder
            ^=> function
                | Ok x -> Job.result x
                | Error error ->
                    match error with
                    | AgentStopping token -> token |> box
                    | AgentStopped token -> token |> box
                    |> AgentStopping
                    |> CouldNotSendLetterException
                    |> Job.raises

    let send mailboxAgent msg =
        Try.send mailboxAgent msg
        >>= function
            | Ok _ -> Job.unit ()
            | Error error ->
                match error with
                | AgentStopping token -> token |> box
                | AgentStopped token -> token |> box
                |> AgentStopping
                |> CouldNotSendLetterException
                |> Job.raises

    let sendAndAwaitReply mailboxAgent msgBuilder =
        Try.sendAndAwaitReply mailboxAgent msgBuilder
        ^=> function
            | Ok x -> Job.result x
            | Error error ->
                match error with
                | AgentStopping token -> token |> box
                | AgentStopped token -> token |> box
                |> AgentStopping
                |> CouldNotSendLetterException
                |> Job.raises

    let sendStop { Stop = stop } v = IVar.tryFill stop v

    let sendStopAndAwait { Stopped = stopped; Stop = stop } v = IVar.tryFill stop v >>-. stopped |> Alt.prepare

    let stopped { Stopped = stopped } = stopped

    let stopping { Stop = stop } = stop

    let isStopped { Stopped = stopped } = stopped ^->. true <|> Alt.always false

    let isStopping { Stop = stop } = stop ^->. true <|> Alt.always false
