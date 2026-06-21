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
