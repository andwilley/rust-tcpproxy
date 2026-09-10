[Work in Progress] TCP Proxy
============================

A project designed to learn async Rust. Not intended for reuse.

```sh
cargo run -- --help
```

TODO
----

-	Testing
	-	Mock traits for testing
	-	Unit tests
	-	Manual end-to-end test with fake backends
	-	Load tests, performance benchmarks
-	Listener error handling
	-	The entire proxy should not die for a failed accept.
	-	Track which ports fail to listen, die if they all fail.
	-	Handle failed accepts and log gracefully
-	Documentation comments throughout
-	Metrics/telemetry
-	Check file descriptor limits
-	Remove ArcSwap types from public APIs (TargetState)
-	Improve the configuration structure
-	Configurable queue wait time
-	Configurable copy buffer sizes
	-	8 KB default buffer size for `copy_bidirectional` not configurable
	-	My solution would have to handle backpressure as well (sender fast than receiver e.g.)
-	Pathological cases
	-	All backends fail to connect at the same time
	-	DNS outage
	-	Malicious clients (slow loris, retry storm, etc)
-	Configurable overall connection timeout

Future features
---------------

-	Dynamic backend discovery
	-	On demand backend drain/undrain
	-	Change backend mapping
	-	Would have to support draining now unmapped backends and undraining new ones
-	Denylist for IPs or ranges
-	Background DNS resolution

Design decisions
----------------

### Static JSON config

This is simple and quick to get started with. The config shape is adapted from the Fly.io challenge (this project is not a submission for this challenge). Dynamic discovery of this mapping would be ideal.

### DNS resolution

We use hickory rather than OS DNS resolution. This gets us caching, but misses still result in synchronous lookups. Background lookups could improve performance as long as it doesn't compete with connections. A task to refresh resolution at TTLs could be first step.

### Backpressure

We limit number of connections and queue size via flags strictly as a way to control total memory usage. This could be made more fair by enforcing per port quotas, but that seems fraught with assumptions about which traffic is critical for the client. We could also strictly bound the queue wait time to avoid long delays by waiting in the queue, but for now this can be tuned via the relative sizes of max connections.

### Datastructure for backend status

We need some way to keep track of backend state. This will be checked for every connection and shared across tasks, so we should consider what the right tradeoffs should be based on expected and potentially worst case performance. In the normal operating case, I expect for the need to update these backend statuses to be rare. In most cases they'll be up, and if not we should mark them and potentially monitor them for cool down. In a pathological case where all backends or most are failing, the latency induced by trying these backends probably outweighs some of the more nuanced performance downsides of our chosen concurrency solution.

The data model was designed to support this "minimal mutation required" approach. We have a 2 value enum where a backend is either serving traffic, cooling down, or drained. To represent cooldown we use a simple `Instant`, where if that is in the future, the backend is still cooling down. Once this cooldown expires it doesn't need to be updated. There are cases where we'll try to connect to cooling backends and if successful, we do remove the cooldown, but typically this shouldn't be necessary. This should make an assumption that we are almost always reading and very rarely writing a valid one.

A mutex per backend would be safe, but pays a context switching tax, as does reader/writer locks. Even though critical sections are short, both serve to put a kind of ceiling on concurrency which can be roughly translated to throughput of the proxy.

Lock-free with atomics is an option, but would require losing some information by being a bit creative with integer types.

ArcSwap allows us to optimize for the read path without dealing with the cache bounce penalty for rwlocks or mutexes. The obvious downside is that if many backends start failing, performance suffers a bit as writes to the target state map will be more common. In a situation like that though, it's probable that the penalty for writing / swapping heap refs is overshadowed by the latency of failing backends.

### Cooldown handling is intentionally loose

We don't require that we get 100% consistency. The cooldown state of a backend can be stepped on by another attempt that had success, e.g. This is generally fine. Handling that would require much more concurrency control, more blocking and contention, etc. If one task could connect and the other couldn't, they're probably both right at some level.

### Generics over dynamic dispatch

I wanted to avoid `async-trait` mostly to learn using the newer native support for async functions in traits. This proxy doesn't have a need yet for anything other than statically defined types and trait implementations. There are some performance benefits as well, but I suspect they aren't significant.
