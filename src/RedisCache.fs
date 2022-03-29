namespace Mimir.Data.Redis

open System
open System.Threading.Tasks
open FSharp.Control
open Mimir.Jsonic
open Mimir.Jsonic.Net
open StackExchange.Redis


module RedisCache =
    let inline redisKey (key:string) = RedisKey.op_Implicit(key)
    let inline redisValue (value:string) = RedisValue.op_Implicit(value)

    type Entry<'value> =
        { CreatedAt: DateTimeOffset
          Value: 'value
        }

    let inline entry value =
        { CreatedAt = DateTimeOffset.UtcNow
          Value = value
        }

    let tryDecodeRedisValue entryCodec (value:RedisValue) =
        let (stringValue:string) = RedisValue.op_Implicit value

        Option.ofObj stringValue
        |> Option.bind(Codec.decodeString entryCodec >> Result.toOption)

    let entryCodec(valueCodec:Codec<'value>) : Codec<Entry<'value>> =
        Codec.object(fun createdAt value ->
            { CreatedAt = createdAt
              Value = value
            }
        )
        |> Codec.field "createdAt" (fun v -> v.CreatedAt) Codec.timestamp
        |> Codec.field "value" (fun v -> v.Value) valueCodec
        |> Codec.buildObject



open RedisCache
open System.Threading
open System.Diagnostics

type RedisCache<'key, 'value>( redisConfiguration:string
                             , hashTableName: string
                             , keyCodec: Codec<'key>
                             , valueCodec: Codec<'value>
                             , maxEntryAge: TimeSpan
                             , ?gcInterval: TimeSpan
                             ) =

    let hashKey = redisKey hashTableName
    let entryCodec = entryCodec valueCodec
    let redis = Lazy<_>.Create(fun _ -> ConnectionMultiplexer.Connect(redisConfiguration))

    let mkKey =
        Codec.encodeToString false keyCodec
        >> redisValue

    let tryDecodeKey (key:RedisValue) =
        if key.IsNullOrEmpty then 
            None 

        else 
            Codec.decodeString keyCodec (RedisValue.op_Implicit key)
            |> Result.toOption

    let mkKeys (firstKey:'key, moreKeys:'key list) =
        firstKey :: moreKeys
        |> List.map mkKey
        |> List.toArray

    let mkEntries(firstKey:'key, firstValue:'value, more:('key * 'value) list) =
        (firstKey, firstValue) :: more
        |> List.map(fun (key, value) ->
            let value = Codec.encodeToString false entryCodec (entry value)
            HashEntry(name = mkKey key, value = redisValue value))
        |> List.toArray

    let tryDecodeEntry (entry:RedisValue) =
        if entry.IsNullOrEmpty then 
            None 

        else 
            Codec.decodeString entryCodec (RedisValue.op_Implicit entry)
            |> Result.toOption

    let cancellationToken = new CancellationTokenSource()
    let startGarbageCollectorLoop() = 
        let gcInterval = defaultArg gcInterval (TimeSpan.FromSeconds 1.0)

        task {
            while not cancellationToken.IsCancellationRequested do 
                try 
                    let mutable numCollected = 0
                    let sw = Stopwatch.StartNew()
                    let db = redis.Value.GetDatabase()
                    let! allEntries = db.HashGetAllAsync(hashKey)

                    for entry in allEntries do 
                        match tryDecodeKey entry.Name, tryDecodeEntry entry.Value with
                        | Some _, Some entry when entry.CreatedAt + maxEntryAge > DateTimeOffset.UtcNow -> 
                            () // Key decodes, Entry decodes and expiry is still future, so do nothing

                        | _ -> 
                            // Either the key or entry don't decode, or it is expired, so delete.
                            let! _ = db.HashDeleteAsync(hashKey, entry.Name)
                            numCollected <- numCollected + 1

                    if numCollected > 0 then
                        printfn $"RedisCache {hashTableName}: Collected {numCollected} keys in {sw.Elapsed.TotalSeconds} sec."

                with e -> 
                    printfn $"RedisCache {hashTableName}: Collection failed: {e.Message}"

                do! Async.Sleep(int gcInterval.TotalMilliseconds)
        }
        |> ignore // Tasks are hot so it is already started.

    do startGarbageCollectorLoop()


    interface IDisposable with 
        member __.Dispose() =
            cancellationToken.Cancel()
            cancellationToken.Dispose()
            

    member __.Set(firstKey:'key, firstValue:'value, ?more:('key * 'value) list)  =
        let entries = mkEntries(firstKey, firstValue, defaultArg more [])
        let db = redis.Value.GetDatabase()
        db.HashSet(hashKey, entries)


    member __.SetAsync(firstKey:'key, firstValue:'value, ?more:('key * 'value) list)  =
        let entries = mkEntries(firstKey, firstValue, defaultArg more [])
        let db = redis.Value.GetDatabase()
        db.HashSetAsync(hashKey, entries)


    member __.Delete(firstKey:'key, ?moreKeys:'key list) =
        let keys = mkKeys(firstKey, defaultArg moreKeys [])
        let db = redis.Value.GetDatabase()
        db.HashDelete(hashKey, keys)


    member __.DeleteAsync(firstKey:'key, ?moreKeys:'key list) =
        let keys = mkKeys(firstKey, defaultArg moreKeys [])
        let db = redis.Value.GetDatabase()
        db.HashDeleteAsync(hashKey, keys)


    member this.TryGet(key:'key, ?maxAge:TimeSpan) =
        let maxAge = maxAge |> Option.defaultValue maxEntryAge

        let db = redis.Value.GetDatabase()
        let value = db.HashGet(hashKey, mkKey key)

        match value |> tryDecodeRedisValue entryCodec with
        | None -> 
            // If the value isn't null but it fails to decode, then delete!
            if value.IsNullOrEmpty then this.Delete(key) |> ignore 
            None

        | Some entry ->
            if entry.CreatedAt + maxAge <= DateTimeOffset.UtcNow then 
                this.Delete(key) |> ignore // Expired
                None
            else 
                Some (entry.Value)


    member this.TryGetAsync(key:'key, ?maxAge:TimeSpan) =
        task {
            let maxAge = maxAge |> Option.defaultValue maxEntryAge

            let db = redis.Value.GetDatabase()
            let! value = db.HashGetAsync(hashKey, mkKey key)

            match value |> tryDecodeRedisValue entryCodec with
            | None -> 
                // If the value isn't null but it fails to decode, then delete!
                if value.IsNullOrEmpty then 
                    let! _ = this.DeleteAsync(key)
                    ()

                return None

            | Some entry ->
                if entry.CreatedAt + maxAge <= DateTimeOffset.UtcNow then 
                    this.Delete(key) |> ignore // Expired
                    return None
                else 
                    return Some (entry.Value)
        }


    member this.GetOrSet(key:'key, getFromSource:_ -> _ -> Result<_, 'error>, ?maxAge) =
        try
            match this.TryGet(key, ?maxAge=maxAge) with
            | Some existing ->
                {| IsLive = false
                   Value = existing
                |}
                |> Ok

            | None ->
                match getFromSource key None with
                | Ok value ->
                    this.Set(key, value)
                    Ok {|
                        IsLive = true
                        Value = value
                    |}

                | Error e ->
                    Error e

        with e ->
            match getFromSource key (Some e) with
            | Ok existing ->
                Ok {|
                    IsLive = true
                    Value = existing
                |}

            | Error e ->
                Error e


    member this.GetOrSetAsync(key:'key, getFromSourceAsync:_ -> _ -> Task<Result<_, 'error>>) =
        task {
            try
                match! this.TryGetAsync(key) with
                | Some existing ->
                    return Ok
                        {| IsLive = false
                           Value = existing
                        |}

                | None ->
                    match! getFromSourceAsync key None with
                    | Ok value ->
                        do! this.SetAsync(key, value)
                        return Ok
                            {| IsLive = true
                               Value = value
                            |}

                    | Error e ->
                        return Error e

            with e ->
                match! getFromSourceAsync key (Some e) with
                | Ok existing ->
                    return Ok
                        {| IsLive = true
                           Value = existing
                        |}

                | Error e ->
                    return Error e
        }

