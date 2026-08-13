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
        result.put("producerQueueType", syncProperties.getProducerQueueType().name());
        result.put("consumerQueueType", syncProperties.getConsumerQueueType().name());
        result.put("redisMode", syncProperties.getRedisMode().name());
        return result;
    }

    private Map<String, Object> paramsSnapshot() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("role", appProperties.getRole().name());
        result.put("location", appProperties.getLocation().name());
        result.put("producerQueueType", syncProperties.getProducerQueueType().name());
        result.put("consumerQueueType", syncProperties.getConsumerQueueType().name());
        result.put("redisMode", syncProperties.getRedisMode().name());
        result.put("queueTtlSeconds", syncProperties.getQueueTtlSeconds());
        result.put("minIntervalSeconds", syncProperties.getSwtch().getMinIntervalSeconds());
        return result;
    }

    public record WriteSessionRequest(String key, String value, Long ttlSeconds) {}

    public record ParamsRequest(Long queueTtlSeconds, Long minIntervalSeconds) {}
}
