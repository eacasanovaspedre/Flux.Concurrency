namespace Flux.Concurrency

open System
open Flux.Collections
open Hopac
open Hopac.Infixes

type private Cached<'Value> =
    { Value: 'Value
      AgePolicyCancellation: unit IVar option
      UsagePolicyCancellation: unit IVar MVar option }

type private Letter<'Key, 'Value, 'Error when 'Key: equality> =
    | FindAndReply of 'Key * IVar<Result<'Value, 'Error>>
    | LoadAndReply of 'Key * IVar<Result<'Value, 'Error>>
    | Cache of 'Key * Cached<'Value>
    | ResetKeyAfterLoadFailed of 'Key
    | Replace of 'Key * 'Value * AgePolicyCancellation: unit IVar
    | ExpireOldItem of 'Key
    | ExpireInactiveItem of 'Key

[<Struct>]
type LoadingCache<'Key, 'Value, 'Error, 'StopToken, 'StoppedToken when 'Key: equality> =
    private | LoadingCache of MailboxAgent<Letter<'Key, 'Value, 'Error>, 'StopToken, 'StoppedToken>

module LoadingCache =

    type RefreshFailurePolicy =
        | ExpireItem
        | KeepItem

    type ItemAgePolicy =
        | NoAgePolicy
        | ExpireIfOlderThan of TimeSpan
        | RefreshIfOlderThan of TimeSpan * FailurePolicy: RefreshFailurePolicy

    type ItemUsagePolicy =
        | NoUsagePolicy
        | ExpireIfNotReadFor of TimeSpan

    type TimeOut = internal TimeOut of (TimeSpan -> Alt<unit>)

    type Config =
        { ItemAgePolicy: ItemAgePolicy
          ItemUsagePolicy: ItemUsagePolicy
          TimeOut: TimeOut }

    module RefreshFailurePolicy =
        let default' = ExpireItem

    module ItemAgePolicy =
        let default' = NoAgePolicy

    module ItemUsagePolicy =
        let default' = NoUsagePolicy

    module TimeOut =
        let default' = TimeOut timeOut

    module Config =
        let default' =
            { ItemAgePolicy = ItemAgePolicy.default'
              ItemUsagePolicy = ItemUsagePolicy.default'
              TimeOut = TimeOut.default' }

    [<AutoOpen>]
    module private Impl =

        type Item<'Value, 'Error> =
            | Cached of Cached<'Value>
            | Loading of IVar<Result<'Value, 'Error>>

        module Policy =

            let inline private triggerCancellationOfCurrentAndSetNewOne cancellationMVar =
                let newCancellation = IVar ()

                MVar.tryMutateJob
                    (fun currentCancellation -> IVar.tryFill currentCancellation () >>-. newCancellation)
                    cancellationMVar
                >>-. newCancellation

            let private runJobAfterTimeoutWithCancellation
                waitFor
                timeToWaitBeforeRunningJob
                currentCancellationMVarOption
                commit
                job
                =
                let cancelCurrentAndSetNewCancellation =
                    currentCancellationMVarOption
                    |> Option.map triggerCancellationOfCurrentAndSetNewOne
                    |> Option.defaultWith (fun () -> Job.result (IVar ()))

                let startRunJobOnTimeout newCancellation =
                    newCancellation ^->. Job.unit ()
                    <|> waitFor timeToWaitBeforeRunningJob ^->. (asAlt commit >>=. job)
                    >>= id
                    |> Job.start
                    >>-. newCancellation

                cancelCurrentAndSetNewCancellation >>= startRunJobOnTimeout

            module ItemUsage =

                let inline private setUpExpirationIfNotReadFor
                    waitFor
                    timeToExpire
                    sendMsg
                    currentCancellationMVar
                    commit
                    key
                    =
                    runJobAfterTimeoutWithCancellation waitFor timeToExpire currentCancellationMVar commit
                    <| sendMsg (ExpireInactiveItem key)

                let runItemUsagePolicy waitFor itemUsagePolicy currentCancellation sendMsg commit key =
                    match itemUsagePolicy, currentCancellation with
                    | ExpireIfNotReadFor timeToWait, currentCancellationMVarOpt ->
                        setUpExpirationIfNotReadFor waitFor timeToWait sendMsg currentCancellationMVarOpt commit key
                        >>- Some
                    | NoUsagePolicy, None -> Job.result None
                    | NoUsagePolicy, Some _ -> failwith "ERROR"

            module ItemAge =

                let inline private setUpAgeExpiration waitFor timeToExpire sendMsg commit key =
                    runJobAfterTimeoutWithCancellation waitFor timeToExpire None commit
                    <| sendMsg (ExpireOldItem key)

                let rec private setUpAutoRefresh
                    waitFor
                    tryLoadFromSource
                    timeToRefresh
                    refreshFailurePolicy
                    sendMsg
                    commit
                    key
                    =
                    let refreshJob =
                        tryLoadFromSource key
                        >>= fun result ->
                            match result, refreshFailurePolicy with
                            | Ok value, _ ->
                                let commit = IVar ()

                                setUpAutoRefresh
                                    waitFor
                                    tryLoadFromSource
                                    timeToRefresh
                                    refreshFailurePolicy
                                    sendMsg
                                    commit
                                    key
                                >>= fun agePolicyCancellation ->
                                    Replace (key, value, agePolicyCancellation) |> sendMsg >>=. IVar.fill commit ()
                            | Error _, ExpireItem -> ExpireOldItem key |> sendMsg
                            | Error _, KeepItem -> Job.unit ()

                    runJobAfterTimeoutWithCancellation waitFor timeToRefresh None commit refreshJob

                let runItemAgePolicy waitFor tryLoadFromSource itemAgePolicy sendMsg commit key =
                    match itemAgePolicy with
                    | NoAgePolicy -> Job.result None
                    | ExpireIfOlderThan timeToWait -> setUpAgeExpiration waitFor timeToWait sendMsg commit key >>- Some
                    | RefreshIfOlderThan (timeToWait, refreshFailurePolicy) ->
                        setUpAutoRefresh waitFor tryLoadFromSource timeToWait refreshFailurePolicy sendMsg commit key
                        >>- Some

        let findAndReply waitFor itemUsagePolicy sendMsg key replyIVar map =
            Job.thunk <| fun _ -> HamtMap.maybeFind key map
            >>= function
                | Some (Cached { Value = value
                                 UsagePolicyCancellation = usagePolicyCancellation }) ->
                    replyIVar *<= Ok value
                    >>=. Policy.ItemUsage.runItemUsagePolicy
                        waitFor
                        itemUsagePolicy
                        usagePolicyCancellation
                        sendMsg
                        (Alt.unit ())
                        key
                    |> Job.Ignore
                | Some (Loading originalReplyIVar) -> originalReplyIVar >>= ( *<= ) replyIVar
                | None -> sendMsg (LoadAndReply (key, replyIVar))
            |> Job.start
            >>-. map

        let loadAndReply waitFor tryLoadFromSource itemUsagePolicy itemAgePolicy sendMsg key replyIVar map =
            match HamtMap.maybeFind key map with
            | Some (Loading originalReplyIVar) -> originalReplyIVar >>= ( *<= ) replyIVar |> Job.start >>-. map
            | None ->
                tryLoadFromSource key
                >>= fun result -> replyIVar *<= result >>-. result
                >>= function
                    | Ok value ->
                        let commit = IVar ()

                        Policy.ItemUsage.runItemUsagePolicy waitFor itemUsagePolicy None sendMsg commit key
                        <*> Policy.ItemAge.runItemAgePolicy waitFor tryLoadFromSource itemAgePolicy sendMsg commit key
                        >>= fun (usagePolicyCancellation, agePolicyCancellation) ->
                            let cached =
                                { Value = value
                                  UsagePolicyCancellation = usagePolicyCancellation |> Option.map MVar
                                  AgePolicyCancellation = agePolicyCancellation }

                            Cache (key, cached) |> sendMsg >>=. IVar.fill commit ()
                    | Error _ -> sendMsg (ResetKeyAfterLoadFailed key)
                |> Job.start
                >>-. HamtMap.add key (Loading replyIVar) map
            | Some (Cached _) ->
                IVar.tryFillFailure
                    replyIVar
                    (exn "Unexpected error, there seems to be a bug in the LoadingCache mechanism")
                >>-. map

        let cache key cached map = HamtMap.put key (Cached cached) map

        let loadFailedAndReply key map = HamtMap.remove key map

        let expireOldItem key map =
            match HamtMap.maybeFind key map with
            | Some (Cached { UsagePolicyCancellation = usagePolicyCancellationMVarOption
                             AgePolicyCancellation = Some agePolicyCancellation }) ->
                agePolicyCancellation ^->. true <|> Alt.always false
                >>= function
                    | true -> Job.result map
                    | false ->
                        let cancelUsagePolicy =
                            usagePolicyCancellationMVarOption
                            |> Option.map (MVar.take >=> fun cancelledIVar -> IVar.tryFill cancelledIVar ())
                            |> Option.defaultWith Job.unit

                        let cancelAgePolicy = IVar.tryFill agePolicyCancellation ()

                        cancelUsagePolicy <*> cancelAgePolicy >>-. HamtMap.remove key map
            | _ -> failwith "ERROR"

        let expireInactiveItem key map =
            match HamtMap.maybeFind key map with
            | Some (Cached { UsagePolicyCancellation = Some usagePolicyCancellationMVar
                             AgePolicyCancellation = agePolicyCancellation }) ->
                MVar.take usagePolicyCancellationMVar
                >>= fun usagePolicyCancellation ->
                    usagePolicyCancellation ^->. true <|> Alt.always false
                    >>= function
                        | true -> Job.result map
                        | false ->
                            let cancelAgePolicy =
                                agePolicyCancellation
                                |> Option.map (fun cancellationIVar -> IVar.tryFill cancellationIVar ())
                                |> Option.defaultWith Job.unit

                            let cancelUsagePolicy = IVar.tryFill usagePolicyCancellation ()

                            cancelUsagePolicy <*> cancelAgePolicy >>-. HamtMap.remove key map
            | _ -> failwith "ERROR"

        let replace key value agePolicyCancellation map =
            HamtMap.change key
            <| function
                | Some (Cached x) ->
                    Some (
                        Cached
                            { x with
                                Value = value
                                AgePolicyCancellation = Some agePolicyCancellation }
                    )
                | _ -> failwith "ERROR"
            <| map

        let agentFun waitFor itemAgePolicy itemUsagePolicy tryLoadFromSource receiveLetter sendLetter =
            let rec loop map =
                receiveLetter ()
                >>= function
                    | Stop stopToken -> Job.result stopToken
                    | Envelope letter ->
                        match letter with
                        | FindAndReply (key, replyIVar) ->
                            findAndReply waitFor itemUsagePolicy sendLetter key replyIVar map >>= loop
                        | LoadAndReply (key, replyIVar) ->
                            loadAndReply
                                waitFor
                                tryLoadFromSource
                                itemUsagePolicy
                                itemAgePolicy
                                sendLetter
                                key
                                replyIVar
                                map
                            >>= loop
                        | Cache (key, cached) -> cache key cached map |> loop
                        | ResetKeyAfterLoadFailed key -> loadFailedAndReply key map |> loop
                        | ExpireOldItem key -> expireOldItem key map >>= loop
                        | ExpireInactiveItem key -> expireInactiveItem key map >>= loop
                        | Replace (key, value, agePolicyCancellation) ->
                            replace key value agePolicyCancellation map |> loop

            loop HamtMap.empty

    let create
        { ItemAgePolicy = itemAgePolicy
          ItemUsagePolicy = itemUsagePolicy
          TimeOut = TimeOut waitFor }
        tryLoadFromSource
        =
        agentFun waitFor itemAgePolicy itemUsagePolicy tryLoadFromSource
        |> MailboxAgent.ofAgentFun
        >>- LoadingCache

    type CacheFindError<'StoppedToken, 'StopToken, 'LoadingError> =
        | CacheStopping of 'StopToken
        | CacheStopped of 'StoppedToken
        | CouldNotLoadValue of 'LoadingError
        
    exception CacheFindException of Error: CacheFindError<obj, obj, obj>

    [<RequireQualifiedAccess>]
    module Try =
        let find key (LoadingCache mailboxAgent) =
            MailboxAgent.Try.sendAndAwaitReply mailboxAgent (fun replyIVar -> Job.result (FindAndReply (key, replyIVar)))
            ^-> function
                | Ok (Ok value) -> Ok value
                | Ok (Error error) -> Error (CouldNotLoadValue error)
                | Error (MailboxAgent.AgentStopped token) -> Error (CacheStopped token)
                | Error (MailboxAgent.AgentStopping token) -> Error (CacheStopping token)
                
    let find key (LoadingCache mailboxAgent) =
        MailboxAgent.Try.sendAndAwaitReply mailboxAgent (fun replyIVar -> Job.result (FindAndReply (key, replyIVar)))
        ^-> function
            | Ok (Ok value) -> value
            | Ok (Error error) -> raise (CacheFindException (CouldNotLoadValue error))
            | Error (MailboxAgent.AgentStopped token) -> raise (CacheFindException (CacheStopped token))
            | Error (MailboxAgent.AgentStopping token) -> raise (CacheFindException (CacheStopping token))
            
    let sendStop token (LoadingCache mailboxAgent) =
        MailboxAgent.sendStop mailboxAgent token
        
    let sendStopAndAwait token (LoadingCache mailboxAgent) =
        MailboxAgent.sendStopAndAwait mailboxAgent token
        
    let stopping (LoadingCache mailboxAgent) =
        MailboxAgent.stopping mailboxAgent
        
    let stopped (LoadingCache mailboxAgent) =
        MailboxAgent.stopped mailboxAgent

    type Builder() =
        member inline x.Yield(_: unit) = ()

        [<CustomOperation "expireIfOlderThan">]
        member inline x.ExpireIfOlderThan((), duration) = ExpireIfOlderThan duration

        [<CustomOperation "expireIfOlderThan">]
        member inline x.ExpireIfOlderThan(current, duration) =
            { ItemUsagePolicy = current
              ItemAgePolicy = ExpireIfOlderThan duration
              TimeOut = TimeOut.default' }

        [<CustomOperation "refreshIfOlderThan">]
        member inline x.RefreshIfOlderThan((), duration, expireIfFailedToRefresh) =
            RefreshIfOlderThan (duration, expireIfFailedToRefresh)

        [<CustomOperation "refreshIfOlderThan">]
        member inline x.RefreshIfOlderThan(current, duration, expireIfFailedToRefresh) =
            { ItemUsagePolicy = current
              ItemAgePolicy = RefreshIfOlderThan (duration, expireIfFailedToRefresh)
              TimeOut = TimeOut.default' }

        [<CustomOperation "expireIfNotReadFor">]
        member inline x.ExpireIfNotReadFor((), duration) = ExpireIfNotReadFor duration

        [<CustomOperation "expireIfNotReadFor">]
        member inline x.ExpireIfNotReadFor(current, duration) =
            { ItemUsagePolicy = ExpireIfNotReadFor duration
              ItemAgePolicy = current
              TimeOut = TimeOut.default' }

        [<CustomOperation "withTimeOut">]
        member x.WithTimeOut((), f) =
            { Config.default' with
                TimeOut = TimeOut f }

        [<CustomOperation "withTimeOut">]
        member x.WithTimeOut(current, f) =
            { ItemUsagePolicy = current
              ItemAgePolicy = ItemAgePolicy.default'
              TimeOut = TimeOut f }

        [<CustomOperation "withTimeOut">]
        member x.WithTimeOut(current, f) =
            { ItemUsagePolicy = ItemUsagePolicy.default'
              ItemAgePolicy = current
              TimeOut = TimeOut f }

        [<CustomOperation "withTimeOut">]
        member x.WithTimeOut(current, f) = { current with TimeOut = TimeOut f }

        [<CustomOperation "loadWith">]
        member x.LoadWith((), f) = create Config.default' f

        [<CustomOperation "loadWith">]
        member x.LoadWith(final, f) =
            create
                { ItemUsagePolicy = ItemUsagePolicy.default'
                  ItemAgePolicy = final
                  TimeOut = TimeOut.default' }
                f

        [<CustomOperation "loadWith">]
        member x.LoadWith(final, f) =
            create
                { ItemUsagePolicy = final
                  ItemAgePolicy = ItemAgePolicy.default'
                  TimeOut = TimeOut.default' }
                f

        [<CustomOperation "loadWith">]
        member x.LoadWith(final, f) = create final f

[<AutoOpen>]
module LoadingCache' =

    let loadingCache = LoadingCache.Builder ()
