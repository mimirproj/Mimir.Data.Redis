namespace Mimir.Data.Redis

open System
open System.Threading.Tasks
open FSharp.Control
open Mimir.Jsonic
open Mimir.Jsonic.Net
open StackExchange.Redis


module RedisMap =
    let inline redisKey (key:string) = RedisKey.op_Implicit(key)
    let inline redisValue (value:string) = RedisValue.op_Implicit(value)

    type Entry<'value> =
        { Value: 'value
          ExpiresAt: DateTimeOffset option
        }
        with
            member entry.IsAlive =
                entry.ExpiresAt
                |> Option.mapBoth (fun expiry -> expiry > DateTimeOffset.UtcNow ) (konst true)


    let inline entry value anyExpiry =
        { Value = value
          ExpiresAt = anyExpiry
        }

    let tryDecodeRedisValue entryCodec (value:RedisValue) =
        let (stringValue:string) = RedisValue.op_Implicit value

        Option.ofObj stringValue
        |> Option.bind(Codec.decodeString entryCodec >> Result.toOption)

    let entryCodec(valueCodec:Codec<'value>) : Codec<Entry<'value>> =
        Codec.object(fun value expiry ->
            { Value = value
              ExpiresAt = expiry
            }
        )
        |> Codec.field "value" (fun v -> v.Value) valueCodec
        |> Codec.field "expiresAt" (fun v -> v.ExpiresAt) (Codec.option Codec.timestamp)
        |> Codec.buildObject


open RedisMap

type RedisMap<'key, 'value>( redisConfiguration:string
                           , hashTableName: string
                           , keyCodec: Codec<'key>
                           , valueCodec: Codec<'value>
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

    let mkEntries(firstKey:'key, firstValue:'value, expiry, more:('key * 'value) list) =
        (firstKey, firstValue) :: more
        |> List.map(fun (key, value) ->
            let value = Codec.encodeToString false entryCodec (entry value expiry)
            HashEntry(name = mkKey key, value = redisValue value))
        |> List.toArray


    member __.Add(firstKey:'key, firstValue:'value, ?expiresIn, ?more:('key * 'value) list) =
        let anyExpiry = expiresIn |> Option.map(fun ts -> DateTimeOffset.UtcNow + ts)
        let entries = mkEntries(firstKey, firstValue, anyExpiry, defaultArg more [])
        let db = redis.Value.GetDatabase()
        db.HashSet(hashKey, entries)


    member __.AddAsync(firstKey:'key, firstValue:'value, ?expiresIn, ?more:('key * 'value) list)  =
        let anyExpiry = expiresIn |> Option.map(fun ts -> DateTimeOffset.UtcNow + ts)
        let entries = mkEntries(firstKey, firstValue, anyExpiry, defaultArg more [])
        let db = redis.Value.GetDatabase()
        db.HashSetAsync(hashKey, entries)


    member __.Remove(firstKey:'key, ?moreKeys:'key list) =
        let keys = mkKeys(firstKey, defaultArg moreKeys [])
        let db = redis.Value.GetDatabase()
        db.HashDelete(hashKey, keys)


    member __.RemoveAsync(firstKey:'key, ?moreKeys:'key list) =
        let keys = mkKeys(firstKey, defaultArg moreKeys [])
        let db = redis.Value.GetDatabase()
        db.HashDeleteAsync(hashKey, keys)


    member __.TryGet(key:'key) =
        let db = redis.Value.GetDatabase()

        let anyItem =
            db.HashGet(hashKey, mkKey key)
            |> tryDecodeRedisValue entryCodec

        // Delete if expired
        match anyItem with
        | Some item when not item.IsAlive ->
            __.Remove(key) |> ignore

        | _ -> ()


        anyItem
        |> Option.filter(fun entry -> entry.IsAlive)
        |> Option.map(fun entry -> entry.Value)


    member __.TryGetAsync(key:'key) =
        task {
            let db = redis.Value.GetDatabase()
            let! redisValue = db.HashGetAsync(hashKey, mkKey key)

            let anyItem =
                redisValue
                |> tryDecodeRedisValue entryCodec

            // Delete if expired
            match anyItem with
            | Some item when not item.IsAlive ->
                let! __ = __.RemoveAsync(key)
                ()

            | _ -> ()

            return
                anyItem
                |> Option.filter(fun entry -> entry.IsAlive)
                |> Option.map(fun entry -> entry.Value)
        }


    member this.Update(key, updater, ?expiresIn) =

        let anyUpdate = this.TryGet(key) |> updater

        match anyUpdate with
        | None -> ()

        | Some value ->
            this.Add(key, value, ?expiresIn=expiresIn)

        anyUpdate



    member this.UpdateAsync(key, updater, ?expiresIn) =
        task {
            let! anyValue = this.TryGetAsync(key)
            let anyUpdate = updater anyValue

            match anyUpdate with
            | None -> ()

            | Some value ->
                do! this.AddAsync(key, value, ?expiresIn=expiresIn)

            return anyUpdate
        }