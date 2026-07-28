# TCP Proxy Design and Implementation Notes

* First step, just get a proxy working between two ports on loopback
* test this with netcat (run a simple server that prints when it gets traffic)
* Then support the config
* Parse json
* round robin using unsigned atomics and mod (relaxed ordering)
    * address non-divisible by num backends and imperfect unsigned overflow
* make sure the round robin is an interface that can be swapped out
* make sure backpressure is handled, if clients are reading slowly, don't continue to write
* make sure connections timeout or clean up. no inifinite connections
* do better than the OS timeout for connections
* SO KEEP ALIVE
* set up careful telemetry and logs
* look at error table and make sure they're handled

## Production considerations

* retry storms
* OS resource limits (like file descriptors)
* hot reload for config
    * or a better config, done via some service callable by the control plane
* denylist for IPs or ranges
* discuss round robin is intentionally imperfect, we don't need to be for this use case
* on shutdown drain active connections before termination

# TODOs

## DNS resolution

* we'll do a simple cache implementation
* targets are resolved on demand
* if many connections need this backend, they all wait on one resolution
* we only have to lock during updates
* we could do this with a dashmap / channel implementation
* or with moka, a library based on caffeiene
* more complicated would be to also refresh in the background based on demand.
* this would have to make sure not to compete with active connection resolution
* we could also base our resolve-ahead strategy on the load balancing implementation if we can
* wouldn't want to be resolving all the time if there isn't the traffic to support it

## backpressure

We need to limit the number of connecitons based on available memory. Should we do this per port?

Make sure file descriptor limit is high enough at the OS level.

Make sure we shed load rather than queuing unbounded.

Should there be some max number of ports in a config? how many should we listen on at once?

## buffer sizes

copybidir uses 8KB by default and its not configurable. it does implement backpressure when one side writes way faster than the other. To make that configurable, we'd have to roll out own.

## Trasient failure tolerance

* improve cooldown logic for backends
* handle DNS lookup transient errors better

## Logging and metrics

printing errors doesn't scale. we'd need a logging / telemetry framework for monitoring the state and history of the proxy, error logging, alerting, etc

## global semaphore

I use a this to manage memory ceiling. This could be combined with a "fairness" quota per port as well if thats needed.

## arcswap justification

The enum was designed to need minimal updates and we expect backend failures to be rare relative to normal operation. ArcSwap allows us to optimize for the read path without dealing with the cache bounce penalty for rwlocks or mutexes. The obvious downside is that if many backends start failing, we suffer a big as writes to the target state map will be more common, but in a situation like that, its probably that the penalty for writing / swapping heap refs is overshadowed by the failing backends.
