package com.example.sync.app.controller;

import com.example.sync.app.service.SwitchService;
import com.example.sync.app.service.SyncService;
import com.example.sync.common.config.AppProperties;
import com.example.sync.common.config.SyncProperties;
import com.example.sync.common.metrics.QueueMetrics;
import com.example.sync.common.model.Location;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Console REST API backing the Web UI ("操作 + 檢查"). Each region (cloud-app / onprem-app)
 * exposes the same endpoints; the Web UI aggregates both.
 */
@RestController
@RequestMapping("/api")
public class ConsoleApiController {

    private final SyncService syncService;
    private final SwitchService switchService;
    private final AppProperties appProperties;
    private final SyncProperties syncProperties;
    private final QueueMetrics queueMetrics;

    public ConsoleApiController(SyncService syncService,
                                SwitchService switchService,
                                AppProperties appProperties,
                                SyncProperties syncProperties,
                                QueueMetrics queueMetrics) {
        this.syncService = syncService;
        this.switchService = switchService;
        this.appProperties = appProperties;
        this.syncProperties = syncProperties;
        this.queueMetrics = queueMetrics;
    }

    /** 寫入測試 session（可附 TTL） */
    @PostMapping("/session")
    public Map<String, Object> writeSession(@RequestBody WriteSessionRequest req) {
        return syncService.writeSession(req.key(), req.value(), req.ttlSeconds());
    }

    /** 兩端一致性比對 + 同步延遲檢視 */
    @GetMapping("/compare")
    public Map<String, Object> compare(@RequestParam("key") String key) {
        return syncService.readKey(key);
    }

    /** 大小網切換：查詢目前 primary */
    @GetMapping("/switch")
    public Map<String, Object> switchStatus() {
        return switchService.current();
    }

    /** 大小網切換：切換 primary（含 10 秒防抖） */
    @PostMapping("/switch")
    public ResponseEntity<?> switchPrimary(@RequestParam("to") String to) {
        try {
            Location target = Location.valueOf(to);
            return ResponseEntity.ok(switchService.switchTo(target));
        } catch (SwitchService.TooFrequentSwitchException e) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .body(Map.of("error", e.getMessage()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", "unknown location: " + to));
        }
    }

    /** 調整參數：查詢目前設定 */
    @GetMapping("/params")
    public Map<String, Object> params() {
        return paramsSnapshot();
    }

    /** 調整參數：runtime 可調 TTL 與切換防抖 */
    @PostMapping("/params")
    public Map<String, Object> updateParams(@RequestBody ParamsRequest req) {
        if (req.queueTtlSeconds() != null) {
            syncProperties.setQueueTtlSeconds(req.queueTtlSeconds());
        }
        if (req.minIntervalSeconds() != null) {
            syncProperties.getSwtch().setMinIntervalSeconds(req.minIntervalSeconds());
        }
        return paramsSnapshot();
    }

    /** 佇列積壓與消費狀態 */
    @GetMapping("/queue-status")
    public Map<String, Object> queueStatus() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("location", appProperties.getLocation().name());
        result.put("role", appProperties.getRole().name());
        result.put("metrics", queueMetrics.snapshot());
        return result;
    }

    /** 元件健康 */
    @GetMapping("/health")
    public Map<String, Object> health() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "UP");
        result.put("role", appProperties.getRole().name());
        result.put("location", appProperties.getLocation().name());
        result.put("queueType", syncProperties.getQueueType().name());
        result.put("producerQueueType", syncProperties.getProducerQueueType().name());
        result.put("consumerQueueType", syncProperties.getConsumerQueueType().name());
        result.put("redisMode", syncProperties.getRedisMode().name());
        return result;
    }

    private Map<String, Object> paramsSnapshot() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("role", appProperties.getRole().name());
        result.put("location", appProperties.getLocation().name());
        result.put("queueType", syncProperties.getQueueType().name());
        result.put("producerQueueType", syncProperties.getProducerQueueType().name());
        result.put("consumerQueueType", syncProperties.getConsumerQueueType().name());
        result.put("redisMode", syncProperties.getRedisMode().name());
        result.put("queueTtlSeconds", syncProperties.getQueueTtlSeconds());
        result.put("minIntervalSeconds", syncProperties.getSwtch().getMinIntervalSeconds());
        return result;
    }

    public record WriteSessionRequest(String key, String value, Long ttlSeconds) {}

    public record ParamsRequest(Long queueTtlSeconds, Long minIntervalSeconds) {}

    public record ReplayRequest(String payload) {}

    public record WriteHashRequest(String key, String field, String value) {}

    public record WriteZSetRequest(String key, String member, Double score) {}

    /** 寫入 Hash */
    @PostMapping("/hash")
    public Map<String, Object> writeHash(@RequestBody WriteHashRequest req) {
        return syncService.writeHash(req.key(), req.field(), req.value());
    }

    /** 讀取 / 比對 Hash */
    @GetMapping("/hash")
    public Map<String, Object> readHash(@RequestParam("key") String key) {
        return syncService.readHash(key);
    }

    /** 寫入 Sorted Set (ZSet) */
    @PostMapping("/zset")
    public Map<String, Object> writeZSet(@RequestBody WriteZSetRequest req) {
        double score = req.score() != null ? req.score() : 0.0;
        return syncService.writeZSet(req.key(), req.member(), score);
    }

    /** 讀取 / 比對 Sorted Set (ZSet) */
    @GetMapping("/zset")
    public Map<String, Object> readZSet(@RequestParam("key") String key) {
        return syncService.readZSet(key);
    }

    /** 跨區 REST API 重放端點：接收 MqPack payload 並重放至本地 Redis */
    @PostMapping("/replay")
    public ResponseEntity<?> replay(@RequestBody ReplayRequest req) {
        if (req.payload() == null || req.payload().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "payload is required"));
        }
        try {
            byte[] bytes = java.util.Base64.getDecoder().decode(req.payload());
            com.netease.nim.camellia.redis.proxy.mq.common.MqPack pack =
                    com.netease.nim.camellia.redis.proxy.mq.common.MqPackSerializer.deserialize(bytes);
            com.example.sync.proxy.plugin.MqPackReplayer.replayLocal(pack, queueMetrics);
            return ResponseEntity.ok(Map.of("status", "ok"));
        } catch (Exception e) {
            queueMetrics.recordReplayFail();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "failed to replay: " + e.getMessage()));
        }
    }
}
