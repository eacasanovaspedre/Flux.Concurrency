namespace Flux.Concurrency

open Hopac
open Hopac.Infixes

[<Struct>]
type 'T MailboxProcessor = private MailboxProcessor of 'T Mailbox

[<Struct>]
type MailboxProcessorStop<'T, 'S> = private MailboxProcessorStop of 'T Mailbox * 'S IVar

type MsgOrStop<'T,'S> =
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
            (Mailbox.take mailbox ^-> Msg)
            <|> (stopIVar ^-> Stop)

        takeMsg |> agent |> Promise.start
        >>- fun stopped ->
                {| Mailbox = MailboxProcessorStop(mailbox, stopIVar)
                   Stopped = stopped |}

    let send (MailboxProcessorStop (mailbox, _)) msg = Mailbox.send mailbox msg

    let sendAndAwaitReply (MailboxProcessorStop (mailbox, _)) msgBuilder =
        Alt.prepareJob
        <| fun _ ->
            let replyIVar = IVar()

            replyIVar |> msgBuilder |> Mailbox.send mailbox
            >>-. IVar.read replyIVar

    let stop (MailboxProcessorStop (_, stopIVar)) v = IVar.tryFill stopIVar v
