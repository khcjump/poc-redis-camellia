package com.example.sync.app.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.JedisPooled;

/**
 * Redis client used by the REST console API. It points at the LOCAL Camellia proxy port
 * ({@code camellia-redis-proxy.config.port}), NOT the upstream Redis, so that every write
 * performed by the console flows through the proxy and therefore triggers the
 * {@code MqMultiWriteProducerProxyPlugin} cross-region replication.
 */
@Configuration
public class RedisClientConfig {

    @Bean
    public JedisPooled jedisPooled(
            @Value("${sync.local-proxy.host:127.0.0.1}") String host,
            @Value("${sync.local-proxy.port:6380}") int port) {
        return new JedisPooled(host, port);
    }
}
