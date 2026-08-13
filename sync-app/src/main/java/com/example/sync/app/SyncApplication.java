package com.example.sync.app;

import com.example.sync.common.config.AppProperties;
import com.example.sync.common.config.SyncProperties;
import com.netease.nim.camellia.redis.proxy.springboot.EnableCamelliaRedisProxyServer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Single runnable program. The same JAR switches operating mode via {@code app.role}
 * ({@link com.example.sync.common.model.AppRole}) and region via {@code app.location}
 * ({@link com.example.sync.common.model.Location}).
 *
 * <p>Camellia Redis proxy (Netty) is embedded through the official Spring Boot starter.
 * The REST/console API is served by Tomcat on {@code server.port}; the Redis listener is
 * separated onto {@code camellia-redis-proxy.config.port} to avoid a port collision.</p>
 */
@SpringBootApplication
@EnableCamelliaRedisProxyServer
@EnableConfigurationProperties({AppProperties.class, SyncProperties.class})
public class SyncApplication {

    public static void main(String[] args) {
        SpringApplication.run(SyncApplication.class, args);
    }
}
