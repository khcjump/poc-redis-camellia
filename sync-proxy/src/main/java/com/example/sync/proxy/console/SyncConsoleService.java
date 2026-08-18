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
 * Camellia Console 自訂服務適配器（SyncConsoleService）：
 * 
 * <h3>資料流程與架構腳色：</h3>
 * <ul>
 *   <li><b>Console 端點監控</b>： 擴充 Camellia 內建 console 的 HTTP REST 端點。</li>
 *   <li><b>指標快照與 JSON 輸出</b>： 當調用 {@code GET /custom} 時，快照當前的 {@link QueueMetrics}
 *       與全域組態資訊（角色、地區、佇列型態、Redis 模式）並回傳 JSON 格式字串。
 *   </li>
 * </ul>
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
            payload.put("redisMode", syncProperties.getRedisMode().name());
            payload.put("metrics", metrics.snapshot());
            return ConsoleResult.success(MAPPER.writeValueAsString(payload));
        } catch (Exception e) {
            return ConsoleResult.error(e.getMessage());
        }
    }
}
