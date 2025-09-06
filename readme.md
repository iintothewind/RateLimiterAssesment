# RateLimiter

## Requirements

Coding: (Expected Time: 1-2 hours) Submit a zipped project, written in Java, with source code
only (no compiled class files) that solves the following problem. Suppose that you are required
to write a specific rate limiter that will run in the context of a fleet of servers for a web server
application.
1. Existing Code: For the purpose of the problem, you may assume that there is an existing
   data access layer class that you do not have to write the implementation for and may reference
   in your code, DistributedKeyValueStore with the method signature public
   CompletableFuture<Integer> incrementByAndExpire(String key, int delta, int
   expirationSeconds) throws Exception; . That is, you may reference an instance of a
   class DistributedKeyValueStore and call the incrementByAndExpire method to increment
   a given key by some count and set the duration that the counter should persist. At the end of
   the specified duration, the counter is automatically deleted. If a key does not exist, it will be
   initialized to zero for you prior to incrementing the value. The expiration is only set when the
   counter is initialized; that is, the expirationSeconds parameter is ignored after a key is
   incremented until it expires, at which time the it may be set again.
2. Your Code Requirement: The rate limiter you create should be
   called DistributedHighThroughputRateLimiter and contain a method with the
   signature public CompletableFuture<Boolean> isAllowed(String key, int limit) .
3. Example Usage: An example of how your rate limiter would be used is if there is a
   ClientId xyz making a request and their configured limit is 500 , your method would be
   invoked with isAllowed("xyz", 500) . If the client xyz has made fewer than "approximately"
   500 requests in the last 60 seconds across all hosts in the fleet, it should return true , else it
   should return false .
4. Key Constraints are:
    1. The method must make use of the DistributedKeyValueStore to track a total count across
       all server instances in the fleet, no matter which server instance receives the request. Server
       counts may vary due to autoscaling and requests are not guaranteed to be evenly distributed
       across servers.
    2. The rate limit period can be assumed to be fixed at 60 seconds and will never change.
    3. Exact accuracy for the count is not required. Clients must be able to make "at least" the
       amount of calls specified in their configured limit, but may temporarily make a little more.
    4. Throughput is expected to be high. Your class should be able to support a total of 100 million
       calls per minute to the same key. Your class should handle any problems that may result from
       that, such as hot partitions or hot keys - the DistributedKeyValueStore will not handle those
       issues for you and will simply store and set an expiration on the exact key you specify. Every
       call to DistributedKeyValueStore will result in a network call - therefore, you should avoid
       calling it on each and every request and only call it occasionally with a batched delta.
    5. Your class and method may be accessed concurrently by multiple threads and should handle
       concurrency safely.
    6. Consider edge cases, error handling, add comments where appropriate, include unit tests,
       and consider maintainability and extensibility. Part of the evaluation will be based on the
       organization of your code. Be prepared to discuss your code and trade-offs

## Analysis

- `DistributedHighThroughputRateLimiter` is suppoed to be running in distributed system, and it should be thread-safe and handle high concurrent situation.
- `DistributedKeyValueStore` should be served as a dependent service of `DistributedHighThroughputRateLimiter`, helping to retrieve and update the total number of requests for each key.
- In regard to the expected throughput, 100 million requests per minute (about 1.67 million requests per second)
- Since every call to DistributedKeyValueStore will result in a network call, which makes it impossible to update the total number of requests after each request, A batch update of request number delta for multiple keys should be used to avoid network overhead.
- The accuracy of total number of requests per key is not guaranteed since request number delta is updated via a scheduler job periodically.
- Downstream developer should be able to call `shardKey()` to create multiple sharding keys in KV store. The sharding will be a logical sharding at the key level, not actual server placement.
- By using logical sharding key, we should gain the following benefits:
  - avoid single-key contention
  - reduce lock contention
  - support higher parallelism
- Changing a key to shard key should be done manually
- Downstream user should be able to call `shardKey()` while application startup or in the runtime to change an existing key to a shard key.

## Implementation

### DistributedKeyValueStore

`DistributedKeyValueStore` depends on `redis` to keep all the number of requests for each key

### DistributedHighThroughputRateLimiter

`DistributedHighThroughputRateLimiter` update the number of request each time in its local map `reqDeltas`
`DistributedHighThroughputRateLimiter` batch update request deltas for all entries in local map `reqDeltas` periodically after startup.


## Running and testing

### Start up redis

user following commands to start up redis docker container

```bash
cd docker
docker compose -f redis.yml up -d
```

### Run tests

- right-click the code of `DistributedHighThroughputRateLimiterTest`
- choose `run DistributedHighThroughputRateLimiterTest` or `debug DistributedHighThroughputRateLimiterTest`