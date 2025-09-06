package rl.util;

import com.google.common.base.Strings;
import io.vavr.control.Try;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

public class RedisClient {

    public final static String host = "localhost";
    public final static int port = 16379;

    private JedisPool pool = null;

    private RedisClient(final String host, final Integer port) {
        this.pool = new JedisPool(host, port);
    }

    private RedisClient() {
        this.pool = new JedisPool(host, port);
    }

    private static class InstanceHolder {
        private static final RedisClient instance = new RedisClient();
    }

    public static RedisClient init() {
        return InstanceHolder.instance;
    }


    public static long getTimeout(int expirationSeconds) {
        final int expiration = Math.max(expirationSeconds, 60);
        final LocalDateTime now = LocalDateTime.now();
        final int seconds = now.getSecond(); // seconds part of current minute
        final int remaining = expiration - seconds;
        return remaining > 1 ? remaining:expiration;
    }

    public void delEntries(final String pattern) {
        try (final Jedis jedis = pool.getResource()) {
            final Set<String> keys = jedis.keys(pattern);
            if (Objects.nonNull(keys) && !keys.isEmpty()) {
                jedis.del(keys.toArray(new String[0]));
            }
        }
    }

    public Long incrBy(final String key, final Long delta, int expirationSeconds) {
        final int expiration = Math.max(expirationSeconds, 60);
        if (!Strings.isNullOrEmpty(key) && delta > 0) {
            try (final Jedis jedis = pool.getResource()) {
                final boolean keyExisting = jedis.exists(key);
                if (keyExisting) {
                    return jedis.incrBy(key, delta);
                } else {
                    final long rate = jedis.incrBy(key, delta);
                    jedis.expire(key, getTimeout(expiration));
                    return rate;
                }
            }
        }
        return 0L;
    }

    public void shardKey(final String key, final Integer numOfShards) {
        try (final Jedis jedis = pool.getResource()) {
            final boolean isKeyExisting = jedis.exists(key);
            if (!isShardKey(key) && numOfShards > 1) {
                final String shardConfigKey = String.format("%s:numOfShards", key);
                jedis.set(shardConfigKey, numOfShards.toString());
                if (isKeyExisting) {
                    jedis.rename(key, String.format("%s:0", key));
                }
            }
        }
    }

    public void disableSharding(final String key) {
        if (isShardKey(key)) {
            final String shardConfigKey = String.format("%s:numOfShards", key);
            try (final Jedis jedis = pool.getResource()) {
                jedis.del(shardConfigKey);
                final int numOfShards = RedisClient.init().getNumberOfShards(key);
                final List<String> subKeys = IntStream.range(0, numOfShards).mapToObj(i -> String.format("%s:%s", key, i)).toList();
                if (!subKeys.isEmpty()) {
                    jedis.del(subKeys.toArray(new String[0]));
                }
            }
        }
    }

    public boolean isShardKey(final String key) {
        final String shardConfigKey = String.format("%s:numOfShards", key);
        try (final Jedis jedis = pool.getResource()) {
            final boolean isKeyExisting = jedis.exists(shardConfigKey);
            return isKeyExisting;
        }
    }

    public int getNumberOfShards(final String key) {
        final String shardConfigKey = String.format("%s:numOfShards", key);
        try (final Jedis jedis = pool.getResource()) {
            return Try.of(() -> jedis.get(shardConfigKey))
                    .filter(Objects::nonNull)
                    .mapTry(Integer::parseInt)
                    .getOrElse(0);
        }
    }

    public Long getLong(final String key) {
        try (final Jedis jedis = pool.getResource()) {
            return Try.of(() -> jedis.get(key))
                    .filter(Objects::nonNull)
                    .mapTry(Long::parseLong)
                    .getOrElse(0L);
        }
    }

    public Long sumLong(final List<String> keys) {
        final long sum = Optional.ofNullable(keys)
                .orElse(List.of())
                .stream()
                .mapToLong(this::getLong)
                .sum();
        return sum;
    }
}
