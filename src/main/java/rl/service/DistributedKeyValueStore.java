package rl.service;

import lombok.extern.slf4j.Slf4j;
import rl.util.RedisClient;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

@Slf4j
public class DistributedKeyValueStore {
    public final static int defaultTimeout = 60;

    public DistributedKeyValueStore() {

    }


    /**
     *
     *  to increment a given key by some count and set the duration that the counter should persist. At the end of
     *  the specified duration, the counter is automatically deleted. If a key does not exist, it will be
     *  initialized to zero for you prior to incrementing the value. The expiration is only set when the
     *  counter is initialized; that is, the expirationSeconds parameter is ignored after a key is
     *  incremented until it expires, at which time the it may be set again
     * @param key key of the request
     * @param delta the delta for the total number of requests
     * @param expirationSeconds timeout for key-numOfRequest entry, it is only applied when entry is created, and ignored if entry is existing
     * @return a CompletableFuture which wraps the number of total request for a key or subkey after increment
     * @throws Exception
     */
    public CompletableFuture<Integer> incrementByAndExpire(String key, int delta, int expirationSeconds) throws Exception {
        log.info("incrementByAndExpire, key: {}, delta: {}", key, delta);
        return CompletableFuture
                .supplyAsync(() -> RedisClient.init().incrBy(key, (long) delta, expirationSeconds))
                .handle(
                        (number, t) -> {
                            if (Objects.nonNull(t)) {
                                log.error("failed to execute request number increment, key: {}, delta: {}", key, delta, t);
                                return 0;
                            } else {
                                return number.intValue();
                            }
                        }
                );
    }

    /**
     * get the total number of requests for a key,
     * return the sum of number of requests for all sub-keys if a given key is a shard key
     * @param key the key of requests
     * @return the total number of requests for a key
     */
    public Long getNumberOfRequests(final String key) {
        return RedisClient.init().sumOfRequests(key);
    }
}
