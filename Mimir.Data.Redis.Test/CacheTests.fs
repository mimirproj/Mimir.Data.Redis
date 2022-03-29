module CacheTests

open System.Threading
open Mimir.Jsonic
open Mimir.Jsonic.Net
open Mimir.Data.Redis

open System
open NUnit.Framework

let cacheKeyTtl = TimeSpan.FromSeconds 0.5
let cache = new RedisCache<Guid, string>( "localhost,abortConnect=false"
                                        , "test"
                                        , Codec.uuid
                                        , Codec.string
                                        , cacheKeyTtl
                                        )

[<Test>]
let ``Text Key Expiry`` () =
    let key = Guid.NewGuid()
    cache.Set(key, "Test Value")
    
    Thread.Sleep(int (cacheKeyTtl.TotalMilliseconds * 2.0))
    
    match cache.TryGet key with
    | None -> Assert.Pass()
    | Some _ -> Assert.Fail()
    
[<OneTimeTearDown>]
let Teardown () =
    (cache :> IDisposable).Dispose()