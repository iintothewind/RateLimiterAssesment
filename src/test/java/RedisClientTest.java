import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import rl.util.RedisClient;

import java.time.LocalDateTime;

@Slf4j
public class RedisClientTest {

    private JedisPool pool = null;

    @BeforeAll
    public static void beforeAll() {
    }

    @BeforeEach
    public void setUp() {
        pool = new JedisPool("localhost", 16379);
        RedisClient.init().delEntries("rl:*");
    }

    @AfterEach
    public void tearDown() {
    }


    @Test
    public void testSet01() {
        try (final Jedis jedis = pool.getResource()) {
            jedis.setex("rl:key001", 60L, "0");
        }
    }

    public static long getTimeout() {
        final LocalDateTime now = LocalDateTime.now();
        final int seconds = now.getSecond(); // seconds part of current minute
        final int remaining = 60 - seconds;
        return remaining > 1L ? remaining:60L;
    }

    public void incr() {
        try (final Jedis jedis = pool.getResource()) {
            final String key = "rl:key001";
            final boolean keyExisting = jedis.exists(key);
            if (keyExisting) {
                jedis.incrBy("rl:key001", 1L);
            } else {
                jedis.incrBy("rl:key001", 1L);
                jedis.expire(key, getTimeout());
            }
        }
    }
}
