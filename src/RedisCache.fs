namespace Mimir.Data.Redis

open System
open System.Threading.Tasks
open FSharp.Control.Tasks
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

type RedisCache<'key, 'value>( redisConfiguration:string
                             , hashTableName: string
                             , keyCodec: Codec<'key>
                             , valueCodec: Codec<'value>
                             , maxEntryAge: TimeSpan
                             ) =

    let hashKey = redisKey hashTableName
    let entryCodec = entryCodec valueCodec
    let redis = Lazy<_>.Create(fun _ -> ConnectionMultiplexer.Connect(redisConfiguration))

    let mkKey =
        Codec.encodeToString false keyCodec
        >> redisValue

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


    member __.TryGet(key:'key, ?maxAge:TimeSpan) =
        let age = maxAge |> Option.defaultValue maxEntryAge

        let db = redis.Value.GetDatabase()
        db.HashGet(hashKey, mkKey key)
        |> tryDecodeRedisValue entryCodec
        |> Option.filter(fun entry -> DateTimeOffset.UtcNow - entry.CreatedAt <= age)
        |> Option.map(fun entry -> entry.Value)


    member __.TryGetAsync(key:'key, ?maxAge:TimeSpan) =
        task {
            let age = maxAge |> Option.defaultValue maxEntryAge

            let db = redis.Value.GetDatabase()
            let! redisValue = db.HashGetAsync(hashKey, mkKey key)

            return
                redisValue
                |> tryDecodeRedisValue entryCodec
                |> Option.filter(fun entry -> DateTimeOffset.UtcNow - entry.CreatedAt <= age)
                |> Option.map(fun entry -> entry.Value)
        }


    member this.GetOrSet(key:'key, getFromSource:Result<'key,exn> -> Result<_, 'error>, ?maxAge) =
        try
            match this.TryGet(key, ?maxAge=maxAge) with
            | Some existing ->
                {| IsLive = true
                   Value = existing
                |}
                |> Ok

            | None ->
                match getFromSource(Ok key) with
                | Ok value ->
                    this.Set(key, value)
                    Ok {|
                        IsLive = false
                        Value = value
                    |}

                | Error e ->
                    Error e

        with e ->
            match getFromSource(Error e) with
            | Ok existing ->
                Ok {|
                    IsLive = true
                    Value = existing
                |}

            | Error e ->
                Error e


    member this.GetOrSetAsync(key:'key, getFromSourceAsync:Result<'key, exn> -> Task<Result<_, 'error>>) =
        task {
            try
                match! this.TryGetAsync(key) with
                | Some existing ->
                    return Ok
                        {| IsLive = true
                           Value = existing
                        |}

                | None ->
                    match! getFromSourceAsync(Ok key) with
                    | Ok value ->
                        do! this.SetAsync(key, value)
                        return Ok
                            {| IsLive = false
                               Value = value
                            |}

                    | Error e ->
                        return Error e

            with e ->
                match! getFromSourceAsync(Error e) with
                | Ok existing ->
                    return Ok
                        {| IsLive = true
                           Value = existing
                        |}

                | Error e ->
                    return Error e
        }

