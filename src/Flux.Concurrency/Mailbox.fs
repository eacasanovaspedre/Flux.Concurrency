namespace Flux.Concurrency

open Hopac
open Hopac.Infixes

[<Struct>]
type MailboxAgent<'T, 'Stopped> = private MailboxAgent of Mailbox: 'T Mailbox * Stopped: 'Stopped Alt

[<Struct>]
type MailboxAgentStop<'T, 'Stopped, 'Stop> =
    private | MailboxAgentStop of Mailbox: 'T Mailbox * Stopped: 'Stopped Alt * Stop: 'Stop IVar

exception CannotSendToMailboxProcessorStoppingException of StopValue: obj

type MsgOrStop<'T, 'S> =
    | Stop of 'S
    | Msg of 'T

module MailboxAgent =

    let create agent =
        let mailbox = Mailbox ()
        let inline takeMsg () = Mailbox.take mailbox

        takeMsg |> agent |> Promise.start
        >>- fun stopped -> MailboxAgent (mailbox, stopped)

    let send (MailboxAgent (mailbox, _)) msg = Mailbox.send mailbox msg

    let sendAndAwaitReply (MailboxAgent (mailbox, _)) msgBuilder =
        Alt.prepareJob
        <| fun _ ->
            let replyIVar = IVar ()

            replyIVar |> msgBuilder |> Mailbox.send mailbox >>-. IVar.read replyIVar

    let stopped (MailboxAgent (_, stopped)) = stopped

module MailboxAgentStop =

    let create agent =
        let mailbox = Mailbox ()
        let stopIVar = IVar ()

        let inline takeMsg () = (stopIVar ^-> Stop) <|> (Mailbox.take mailbox ^-> Msg)

        takeMsg |> agent |> Promise.start
        >>- fun stopped -> MailboxAgentStop (mailbox, stopped, stopIVar)

    let trySend (MailboxAgentStop (mailbox, _, stop)) msg =
        (stop ^-> Error)
        <|> (Alt.prepare <| (Mailbox.send mailbox msg >>-. Alt.always (Ok ())))

    let maybeSend mailboxProcessor msg =
        trySend mailboxProcessor msg
        >>- function
            | Ok _ -> Some ()
            | Error _ -> None

    let send mailboxProcessor msg =
        trySend mailboxProcessor msg
        >>- function
            | Ok _ -> ()
            | Error x -> raise (CannotSendToMailboxProcessorStoppingException x)

    let trySendAndAwaitReply (MailboxAgentStop (mailbox, _, stop)) msgBuilder =
        (stop ^-> Error)
        <|> (Alt.prepareJob
             <| fun _ ->
                 let replyIVar = IVar ()

                 replyIVar |> msgBuilder |> Mailbox.send mailbox >>-. IVar.read replyIVar ^-> Ok)

    let maybeSendAndAwaitReply mailboxProcessor msgBuilder =
        trySendAndAwaitReply mailboxProcessor msgBuilder
        >>- function
            | Ok x -> Some x
            | Error x -> None

    let sendAndAwaitReply mailboxProcessor msgBuilder =
        trySendAndAwaitReply mailboxProcessor msgBuilder
        >>- function
            | Ok x -> x
            | Error x -> raise (CannotSendToMailboxProcessorStoppingException x)

    let sendStop (MailboxAgentStop (_, _, stopIVar)) v = IVar.tryFill stopIVar v

    let sendStopAndAwait (MailboxAgentStop (_, stopped, stopIVar)) v =
        IVar.tryFill stopIVar v >>-. stopped |> Alt.prepare

    let stopped (MailboxAgentStop (_, stopped, _)) = stopped
