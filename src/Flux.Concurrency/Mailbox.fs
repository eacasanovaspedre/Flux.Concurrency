namespace Flux.Concurrency

open Hopac
open Hopac.Infixes

[<Struct>]
type MailboxAgent<'T, 'StoppedToken> =
    private | MailboxAgent of Mailbox: Mailbox<'T * unit Promise option> * Stopped: 'StoppedToken Alt

module MailboxAgent =

    let create agent =
        let mailbox = Mailbox ()
        let inline takeMsg () = Mailbox.take mailbox
        let inline sendMsg msg = Mailbox.send mailbox (msg, None)

        (takeMsg, sendMsg) ||> agent |> Promise.start
        >>- fun stopped -> MailboxAgent (mailbox, stopped)

    let send (MailboxAgent (mailbox, _)) msg = Mailbox.send mailbox (msg, None)

    let sendAndAwaitReply (MailboxAgent (mailbox, stopped)) msgBuilder =
        Alt.withNackJob
        <| fun nack ->
            let replyIVar = IVar ()

            (msgBuilder replyIVar, Some nack) |> Mailbox.send mailbox >>-. replyIVar

    let stopped (MailboxAgent (_, stopped)) = stopped

    let isStopped (MailboxAgent (_, stopped)) = stopped ^->. true <|> Alt.always false

[<Struct>]
type MailboxAgentStop<'T, 'StoppedToken, 'StopToken> =
    private | MailboxAgentStop of
        Mailbox: Mailbox<'T * unit Promise option> *
        Stopped: 'StoppedToken Alt *
        Stop: 'StopToken IVar

module MailboxAgentStop =

    type CouldNotSendLetter<'StopToken> = AgentStopping of 'StopToken

    exception CouldNotSendLetterException of Error: obj CouldNotSendLetter

    type LetterOrStop<'T, 'S> =
        | Stop of 'S
        | Msg of 'T

    let create agent =
        let mailbox = Mailbox ()
        let stopIVar = IVar ()
        let inline takeMsg () = stopIVar ^-> Stop <|> Mailbox.take mailbox ^-> Msg
        let inline sendMsg msg = Mailbox.send mailbox (msg, None)

        (takeMsg, sendMsg) ||> agent |> Promise.start
        >>- fun stopped -> MailboxAgentStop (mailbox, stopped, stopIVar)

    let trySend (MailboxAgentStop (mailbox, _, stop)) msg =
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

    let trySendAndAwaitReply (MailboxAgentStop (mailbox, _, stop)) msgBuilder =
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

    let sendStop (MailboxAgentStop (_, _, stopIVar)) v = IVar.tryFill stopIVar v

    let sendStopAndAwait (MailboxAgentStop (_, stopped, stopIVar)) v =
        IVar.tryFill stopIVar v >>-. stopped |> Alt.prepare

    let stopped (MailboxAgentStop (_, stopped, _)) = stopped

    let stopping (MailboxAgentStop (_, _, stopping)) = stopping

    let isStopped (MailboxAgentStop (_, stopped, _)) = stopped ^->. true <|> Alt.always false

    let isStopping (MailboxAgentStop (_, _, stopping)) = stopping ^->. true <|> Alt.always false
