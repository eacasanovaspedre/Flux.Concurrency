namespace Flux.Concurrency

open Hopac
open Hopac.Infixes

type NackOption = unit Promise option

[<Struct>]
type MailboxAgent<'T, 'StoppedToken, 'StopToken> =
    private | MailboxAgent of Mailbox: Mailbox<'T * NackOption> * Stopped: 'StoppedToken Alt * Stop: 'StopToken IVar

module MailboxAgent =

    type CouldNotSendLetter<'StopToken> = AgentStopping of 'StopToken

    exception CouldNotSendLetterException of Error: obj CouldNotSendLetter

    type LetterOrStop<'T, 'StopToken> =
        | Stop of 'StopToken
        | Letter of 'T * NackOption

    type LetterCmd<'T> = 'T option Job list

    module LetterCmd =

        let none: _ LetterCmd = []

        let inline ofLetterOption letterOption : _ LetterCmd = [ Job.result letterOption ]

        let inline ofLetter letter : _ LetterCmd = ofLetterOption (Some letter)

        let inline ofLetterJob letterJob : _ LetterCmd = [ letterJob >>- Some ]

        let inline ofLetterOptionJob letterOptionJob : _ LetterCmd = [ letterOptionJob ]

        let inline ofJobAny anyJob : _ LetterCmd = anyJob >>-. None |> ofLetterOptionJob

        let inline batch (letterCmds: #seq<'Letter LetterCmd>) : 'Letter LetterCmd = List.concat letterCmds

        let runWith run (letterCmd: _ LetterCmd) =
            letterCmd
            |> Seq.map (Job.bind (Option.map run >> Option.defaultWith Job.unit))
            |> Job.conIgnore
            |> Job.queue

    let create agent =
        let mailbox = Mailbox ()
        let stopIVar = IVar ()
        let inline takeMsg () = stopIVar ^-> Stop <|> Mailbox.take mailbox ^-> Letter
        let inline sendMsg msg = Mailbox.send mailbox (msg, None)

        (takeMsg, sendMsg) ||> agent |> Promise.start
        >>- fun stopped -> MailboxAgent (mailbox, stopped, stopIVar)

    module WithUpdate =

        let create init update stop =
            create
            <| fun take send ->
                let rec loop state =
                    take ()
                    >>= fun msg ->
                        match msg with
                        | Stop token -> stop token
                        | Letter (body, nackOption) ->
                            let state', cmds = update nackOption body state

                            cmds
                            |> Seq.map (Job.bind send)
                            |> Job.conIgnore
                            |> Job.queue
                            >>= fun () -> loop state'

                init () >>= loop

    module WithIntents =

        let create init update mapIntents stop =
            create
            <| fun take send ->
                let rec loop state =
                    take ()
                    >>= fun msg ->
                        match msg with
                        | Stop token -> stop token
                        | Letter (body, nackOption) ->
                            let state', cmd = update nackOption body state

                            LetterCmd.runWith send cmd >>=. loop state'

                init () >>= loop

    let trySend (MailboxAgent (mailbox, _, stop)) msg =
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

    let trySendAndAwaitReply (MailboxAgent (mailbox, _, stop)) msgBuilder =
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

    let sendStop (MailboxAgent (_, _, stopIVar)) v = IVar.tryFill stopIVar v

    let sendStopAndAwait (MailboxAgent (_, stopped, stopIVar)) v = IVar.tryFill stopIVar v >>-. stopped |> Alt.prepare

    let stopped (MailboxAgent (_, stopped, _)) = stopped

    let stopping (MailboxAgent (_, _, stopping)) = stopping

    let isStopped (MailboxAgent (_, stopped, _)) = stopped ^->. true <|> Alt.always false

    let isStopping (MailboxAgent (_, _, stopping)) = stopping ^->. true <|> Alt.always false
