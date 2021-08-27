namespace Flux.Concurrency

open Hopac
open Hopac.Infixes

[<Struct>]
type 'T MailboxProcessor = private MailboxProcessor of Mailbox: 'T Mailbox

type MailboxProcessorStop<'T, 'Stop> = private MailboxProcessorStop of Mailbox: 'T Mailbox * Stopped: 'Stop IVar

exception CannotSendToMailboxProcessorStoppingException of StopValue: obj

type MsgOrStop<'T, 'S> =
    | Stop of 'S
    | Msg of 'T

module MailboxProcessor =

    let create agent =
        let mailbox = Mailbox()
        let inline takeMsg () = Mailbox.take mailbox

        takeMsg |> agent |> Promise.start
        >>- fun stopped ->
                {| Mailbox = MailboxProcessor mailbox
                   Stopped = stopped |}

    let send (MailboxProcessor mailbox) msg = Mailbox.send mailbox msg

    let sendAndAwaitReply (MailboxProcessor mailbox) msgBuilder =
        Alt.prepareJob
        <| fun _ ->
            let replyIVar = IVar()

            replyIVar |> msgBuilder |> Mailbox.send mailbox
            >>-. IVar.read replyIVar

module MailboxProcessorStop =

    let create agent =
        let mailbox = Mailbox()
        let stopIVar = IVar()

        let inline takeMsg () =
            (stopIVar ^-> Stop)
            <|> (Mailbox.take mailbox ^-> Msg)

        takeMsg |> agent |> Promise.start
        >>- fun stopped ->
                {| Mailbox = MailboxProcessorStop(mailbox, stopIVar)
                   Stopped = stopped |}

    let trySend (MailboxProcessorStop (mailbox, stop)) msg =
        (stop ^-> Error)
        <|> (Alt.prepare
             <| (Mailbox.send mailbox msg >>-. Alt.always (Ok())))

    let maybeSend mailboxProcessor msg =
        trySend mailboxProcessor msg
        >>- function
            | Ok _ -> Some()
            | Error _ -> None

    let send mailboxProcessor msg =
        trySend mailboxProcessor msg
        >>- function
            | Ok _ -> ()
            | Error x -> raise (CannotSendToMailboxProcessorStoppingException x)

    let trySendAndAwaitReply (MailboxProcessorStop (mailbox, stop)) msgBuilder =
        (stop ^-> Error)
        <|> (Alt.prepareJob
             <| fun _ ->
                 let replyIVar = IVar()

                 replyIVar |> msgBuilder |> Mailbox.send mailbox
                 >>-. IVar.read replyIVar ^-> Ok)

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

    let stop (MailboxProcessorStop (_, stopIVar)) v = IVar.tryFill stopIVar v
