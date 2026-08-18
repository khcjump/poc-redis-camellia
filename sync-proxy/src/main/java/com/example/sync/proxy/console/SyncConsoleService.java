package com.example.sync.proxy.console;

import com.example.sync.common.config.AppProperties;
import com.example.sync.common.config.SyncProperties;
import com.example.sync.common.metrics.QueueMetrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netease.nim.camellia.http.console.ConsoleResult;
import com.netease.nim.camellia.redis.proxy.console.ConsoleServiceAdaptor;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 自訂 ConsoleService：在 {@code /custom} 端點輸出 MQ 同步指標（補足 Camellia 缺 MQ 指標的缺口）。
 *
 * <p>ConsoleResult 只接受 String payload，故以 Jackson 序列化 JSON。</p>
 */
public class SyncConsoleService extends ConsoleServiceAdaptor {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final AppProperties appProperties;
    private final SyncProperties syncProperties;
    private final QueueMetrics metrics;

    public SyncConsoleService(AppProperties appProperties, SyncProperties syncProperties, QueueMetrics metrics) {
        this.appProperties = appProperties;
        this.syncProperties = syncProperties;
        this.metrics = metrics;
    }

    @Override
    public ConsoleResult custom(Map<String, List<String>> params) {
        try {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("role", appProperties.getRole().name());
            payload.put("location", appProperties.getLocation().name());
            payload.put("queueType", syncProperties.getQueueType().name());
            payload.put("producerQueueType", syncProperties.getProducerQueueType().name());
            payload.put("consumerQueueType", syncProperties.getConsumerQueueType().name());
            payload.put("redisMode", syncProperties.getRedisMode().name());
            payload.put("metrics", metrics.snapshot());
            return ConsoleResult.success(MAPPER.writeValueAsString(payload));
        } catch (Exception e) {
            return ConsoleResult.error(e.getMessage());
        }
    }
}
