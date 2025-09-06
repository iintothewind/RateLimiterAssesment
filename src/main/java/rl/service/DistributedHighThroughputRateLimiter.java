package rl.service;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.vavr.control.Try;
import lombok.extern.slf4j.Slf4j;
import rl.util.RedisClient;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class DistributedHighThroughputRateLimiter implements AutoCloseable {
    public final static long reportInterval = 2000L;
    private final DistributedKeyValueStore kvStore;
    private final ConcurrentHashMap<String, Long> reqDeltas;
    private final ReentrantLock lock;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService executor;
    private final LoadingCache<String, Long> requestNumCache;

    public DistributedHighThroughputRateLimiter(final DistributedKeyValueStore kvStore) {
        this.kvStore = kvStore;
        this.reqDeltas = new ConcurrentHashMap<>();
        this.lock = new ReentrantLock();
        this.scheduler = Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory());
        this.scheduler.scheduleAtFixedRate(this::publishDeltas, 0, reportInterval, TimeUnit.MILLISECONDS);
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.requestNumCache = CacheBuilder.newBuilder()
                .expireAfterWrite(Duration.ofMillis(reportInterval))
                .build(new CacheLoader<String, Long>() {
                    @Override
                    public Long load(final String key) throws Exception {
                        final Long numberOfRequests = kvStore.getNumberOfRequests(key);
                        return numberOfRequests;
                    }
                });
    }

    @Override
    public void close() {
        this.scheduler.shutdown();
        this.executor.shutdown();
    }

    /**
     * return a random subkey if the given key is a shard key
     * or return the key itself
     *
     * @param key
     * @return
     */
    private String loadKey(final String key) {
        final boolean isShardKey = RedisClient.init().isShardKey(key);
        if (isShardKey) {
            final int numOfShards = RedisClient.init().getNumberOfShards(key);
            final int randomShardIndex = ThreadLocalRandom.current().nextInt(0, numOfShards);
            final String randomSubKey = String.format("%s:%s", key, randomShardIndex);
            return randomSubKey;
        }
        return key;
    }

    /**
     * increase the total number of request per each key in reqDeltas by delta, and then clear all entries of reqDeltas after.
     */
    private void publishDeltas() {
        try {
            if (lock.tryLock(300L, TimeUnit.MILLISECONDS)) {
                final Map<String, Long> snapshot = Map.copyOf(reqDeltas);
                reqDeltas.clear();
                snapshot.forEach((key, delta) -> Try.run(() -> kvStore
                        .incrementByAndExpire(loadKey(key), delta.intValue(), DistributedKeyValueStore.defaultTimeout)
                        .exceptionally(t -> {
                            log.error("failed to publishDelta for key: {}", key, t);
                            return 0;
                        })));
            }
        } catch (InterruptedException e) {
            log.error("failed to publishDeltas", e);
            throw new IllegalStateException(e);
        } finally {
            if (lock.isHeldByCurrentThread()) {
                Try.run(lock::unlock);
            }
        }
    }

    /**
     * increate the delta by 1 per given key in reqDeltas
     *
     * @param key
     */
    private void updateDelta(final String key) {
        reqDeltas.merge(key, 1L, Long::sum);
    }

    /**
     * update delta for each key in reqDeltas,
     * then retrieve the total number of request for given key, compare it with input limit.
     *
     * @param key
     * @param limit
     * @return A CompletableFuture of comparison result
     */
    public CompletableFuture<Boolean> isAllowed(String key, int limit) {
        return CompletableFuture.supplyAsync(() -> {
            this.updateDelta(key);
            final Long numberOfRequests = Try.of(() -> this.requestNumCache.get(key)).getOrElse(0L);
            return numberOfRequests < limit;
        }, executor);
    }


}
