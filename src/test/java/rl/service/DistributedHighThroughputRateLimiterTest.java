package rl.service;

import io.vavr.control.Try;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rl.util.RedisClient;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@Slf4j
public class DistributedHighThroughputRateLimiterTest {
    private DistributedKeyValueStore kvStore = null;
    private DistributedHighThroughputRateLimiter rateLimiter = null;

    @BeforeEach
    public void setUp() {
        kvStore = new DistributedKeyValueStore();
        rateLimiter = new DistributedHighThroughputRateLimiter(kvStore);
        RedisClient.init().delEntries("rl:*");
    }

    @AfterEach
    public void tearDown() {
    }


    @Test
    @SneakyThrows
    public void testAllowed01() {
        final String svcKey = "rl:svc01";
        RedisClient.init().disableSharding(svcKey);
        rateLimiter.isAllowed(svcKey, 20)
                .whenComplete((r, t) -> {
                    if (Objects.nonNull(t)) {
                        log.info("isAllowed() error: ", t);
                    } else {
                        log.info("svcKey: {}, isAllowed: {}", svcKey, r);
                        Assertions.assertThat(r).isTrue();
                    }
                });
        TimeUnit.SECONDS.sleep(6L);
        final Long requestNum = kvStore.getNumberOfRequests(svcKey);
        Assertions.assertThat(requestNum).isEqualTo(1L);
    }

    @Test
    @SneakyThrows
    public void testAllowed02() {
        final String svcKey = "rl:svc01";
        RedisClient.init().disableSharding(svcKey);
        RedisClient.init().shardKey(svcKey, 5);
        rateLimiter.isAllowed(svcKey, 20)
                .whenComplete((r, t) -> {
                    if (Objects.nonNull(t)) {
                        log.info("isAllowed() error: ", t);
                    } else {
                        log.info("svcKey: {}, isAllowed: {}", svcKey, r);
                        Assertions.assertThat(r).isTrue();
                    }
                });
        TimeUnit.SECONDS.sleep(6L);
        final Long requestNum = kvStore.getNumberOfRequests(svcKey);
        Assertions.assertThat(requestNum).isEqualTo(1L);
    }

    @Test
    @SneakyThrows
    public void testAllowed03() {
        final String svcKey = "rl:svc01";
        RedisClient.init().disableSharding(svcKey);
        IntStream.range(0, 20).forEach(i -> {
            Try.run(() -> TimeUnit.SECONDS.sleep(1L));
            rateLimiter.isAllowed(svcKey, 10)
                    .whenComplete((r, t) -> {
                        if (Objects.nonNull(t)) {
                            log.info("isAllowed() error: ", t);
                        } else {
                            if (i < 10) {
                                Assertions.assertThat(r).isTrue();
                            }
                        }
                    });
        });

        TimeUnit.SECONDS.sleep(20L);
        final Long requestNum = kvStore.getNumberOfRequests(svcKey);
        Assertions.assertThat(requestNum).isEqualTo(20L);
    }


    @Test
    @SneakyThrows
    public void testAllowed04() {
        final String svcKey = "rl:svc01";
        RedisClient.init().disableSharding(svcKey);
        IntStream.range(0, 20).forEach(i -> {
            Try.run(() -> TimeUnit.SECONDS.sleep(1L));
            rateLimiter.isAllowed(svcKey, 5)
                    .whenComplete((r, t) -> {
                        if (Objects.nonNull(t)) {
                            log.info("isAllowed() error: ", t);
                        } else {
                            log.info("svcKey: {}, isAllowed: {}", svcKey, r);
                            if (i > 10) {
                                Assertions.assertThat(r).isFalse();
                            }
                        }
                    });
        });

        TimeUnit.SECONDS.sleep(20L);
    }

    @Test
    @SneakyThrows
    public void testAllowed05() {
        IntStream.range(1, 6)
                .mapToObj(n -> String.format("rl:svc0%s", n))
                .parallel()
                .forEach(svcKey -> {
                    IntStream.range(0, 20).forEach(i -> {
                        Try.run(() -> TimeUnit.SECONDS.sleep(1L));
                        rateLimiter.isAllowed(svcKey, 5)
                                .whenComplete((r, t) -> {
                                    if (Objects.nonNull(t)) {
                                        log.info("isAllowed() error: ", t);
                                    } else {
                                        log.info("svcKey: {}, isAllowed: {}", svcKey, r);
                                        if (i > 10) {
                                            Assertions.assertThat(r).isFalse();
                                        }
                                    }
                                });
                    });
                });


        TimeUnit.SECONDS.sleep(20L);
    }

    @Test
    @SneakyThrows
    public void testAllowed06() {
        RedisClient.init().shardKey("rl:svc03", 5);
        IntStream.range(1, 6)
                .mapToObj(n -> String.format("rl:svc0%s", n))
                .parallel()
                .forEach(svcKey -> {
                    IntStream.range(0, 20).forEach(i -> {
                        Try.run(() -> TimeUnit.SECONDS.sleep(1L));
                        rateLimiter.isAllowed(svcKey, 5)
                                .whenComplete((r, t) -> {
                                    if (Objects.nonNull(t)) {
                                        log.info("isAllowed() error: ", t);
                                    } else {
                                        log.info("svcKey: {}, isAllowed: {}", svcKey, r);
                                        if (i > 10) {
                                            Assertions.assertThat(r).isFalse();
                                        }
                                    }
                                });
                    });
                });


        TimeUnit.SECONDS.sleep(20L);
    }

    @Test
    @SneakyThrows
    public void testAllowed07() {
        final String svcKey = "rl:svc01";
        RedisClient.init().disableSharding(svcKey);
        IntStream.range(0, 500).forEach(i -> {
            rateLimiter.isAllowed(svcKey, 500)
                    .whenComplete((r, t) -> {
                        if (Objects.nonNull(t)) {
                            log.info("isAllowed() error: ", t);
                        } else {
                            log.info("svcKey: {}, isAllowed: {}", svcKey, r);
                            Assertions.assertThat(r).isTrue();
                        }
                    });
        });

        TimeUnit.SECONDS.sleep(5L);

        rateLimiter.isAllowed(svcKey, 500)
                .whenComplete((r, t) -> {
                    if (Objects.nonNull(t)) {
                        log.info("isAllowed() error: ", t);
                    } else {
                        final Long total = RedisClient.init().sumOfRequests(svcKey);
                        log.info("key: {}, total: {}, isAllowed: {}", svcKey, total, r);
                        Assertions.assertThat(r).isFalse();
                    }
                });

        TimeUnit.SECONDS.sleep(5L);
    }

    @Test
    @SneakyThrows
    public void testAllowed08() {
        RedisClient.init().shardKey("rl:svc03", 5);
        RedisClient.init().shardKey("rl:svc07", 5);
        IntStream.range(1, 9)
                .mapToObj(n -> String.format("rl:svc0%s", n))
                .parallel()
                .forEach(svcKey -> {
                    IntStream.range(0, 9999).forEach(i -> {
                        rateLimiter.isAllowed(svcKey, 500)
                                .whenComplete((r, t) -> {
                                    if (Objects.nonNull(t)) {
                                        log.info("isAllowed() error: ", t);
                                    } else {
                                        log.info("svcKey: {}, isAllowed: {}", svcKey, r);
                                        Assertions.assertThat(r).isTrue();
                                    }
                                });
                    });
                });


        TimeUnit.SECONDS.sleep(9);

        IntStream.range(1, 9)
                .mapToObj(n -> String.format("rl:svc0%s", n))
                .forEach(svcKey -> {
                    rateLimiter.isAllowed(svcKey, 500)
                            .whenComplete((r, t) -> {
                                if (Objects.nonNull(t)) {
                                    log.info("isAllowed() error: ", t);
                                } else {
                                    final Long total = RedisClient.init().sumOfRequests(svcKey);
                                    log.info("key: {}, total: {}, isAllowed: {}", svcKey, total, r);
                                    Assertions.assertThat(r).isFalse();
                                }
                            });
                });

        TimeUnit.SECONDS.sleep(5L);
    }

}
