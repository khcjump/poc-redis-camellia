package com.example.sync.proxy.plugin;

import com.example.sync.common.metrics.QueueMetrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netease.nim.camellia.redis.proxy.command.Command;
import com.netease.nim.camellia.redis.proxy.conf.GlobalRedisProxyEnv;
import com.netease.nim.camellia.redis.proxy.monitor.CommandFailMonitor;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPack;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPackSerializer;
import com.netease.nim.camellia.redis.proxy.reply.ErrorReply;
import com.netease.nim.camellia.redis.proxy.reply.Reply;
import com.netease.nim.camellia.redis.proxy.upstream.IUpstreamClientTemplate;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * 跨區重放器（MqPack Replayer）：
 * 
 * <h3>核心職責與防迴圈設計：</h3>
 * <ul>
 *   <li><b>跨區 REST API 轉發 ({@link #replay(MqPack, QueueMetrics, String)})</b>：
 *       當本地 Consumer 收到本區佇列訊息時（例如 Cloud 消費 GCP Pub/Sub），將 {@link MqPack} 序列化並編碼為 Base64 JSON，
 *       透過 HTTP REST API 發送至遠端 App（如 OnPrem App 的 {@code POST /api/replay}），以觸發跨區重放。
 *   </li>
 *   <li><b>本地 Redis 執行 ({@link #replayLocal(MqPack, QueueMetrics)})</b>：
 *       目標區域接收到 API 請求後，透過 {@link GlobalRedisProxyEnv#getClientTemplateFactory()}
 *       取得與對應 bid/bgroup 綁定的直連 Upstream 模板（{@link IUpstreamClientTemplate}），
 *       直接對原始 Redis 主庫發送 {@link IUpstreamClientTemplate#sendCommand} 指令。
 *   </li>
 *   <li><b>關鍵防無窮迴圈機制 (Anti-Infinite Loop Design)</b>：
 *       直連 Upstream 模板繞過了 Camellia Proxy 插件層（ProxyPlugin），因此該重放動作<b>不會</b>再次觸發
 *       {@code MqMultiWriteProducerProxyPlugin} 發送佇列訊息，防止 A→B→A 的 Ping-Pong 無窮循環發送！
 *   </li>
 *   <li><b>HTTP 連線池 (Apache HttpClient 5 PoolingHttpClientConnectionManager)</b>：
 *       使用執行緒安全的連線池共享 CloseableHttpClient，避免每次請求都建立新 TCP 連線帶來的延遲與資源浪費。
 *       連線池參數（最大連線數、每路由最大連線數、TTL 等）透過靜態初始化區塊設定。
 *   </li>
 * </ul>
 */
public final class MqPackReplayer {

    private static final Logger log = LoggerFactory.getLogger(MqPackReplayer.class);

    /** 使用共享 ObjectMapper 建構 JSON，避免手工拼接字串造成格式破損 */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ── HTTP 連線池設定 ─────────────────────────────────────────────────────
    /**
     * 連線池管理器：
     * <ul>
     *   <li>{@code maxTotal}：全局最大連線數（跨所有 route 加總上限）</li>
     *   <li>{@code maxPerRoute}：同一 remoteAppUrl host 的最大並行連線數</li>
     *   <li>{@code timeToLive}：連線最大存活時間；到期後自動回收，避免 stale 連線積累</li>
     * </ul>
     */
    private static final PoolingHttpClientConnectionManager CONN_MANAGER;

    /**
     * 單例 CloseableHttpClient（執行緒安全），底層共用 {@link #CONN_MANAGER}。
     * <p>RequestConfig 設定：
     * <ul>
     *   <li>{@code connectionRequestTimeout}：從連線池等候可用連線的超時時間</li>
     *   <li>{@code connectTimeout}：TCP 三向握手超時</li>
     *   <li>{@code responseTimeout}：等待回應第一個 byte 的超時（等同 socket read timeout）</li>
     * </ul>
     */
    private static final CloseableHttpClient HTTP_CLIENT;

    static {
        CONN_MANAGER = new PoolingHttpClientConnectionManager(
                60, TimeUnit.SECONDS   // timeToLive：連線存活上限 60 秒
        );
        CONN_MANAGER.setMaxTotal(50);          // 全局連線池上限
        CONN_MANAGER.setDefaultMaxPerRoute(20); // 同一 host 的最大連線數

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(Timeout.ofSeconds(3))  // 等候連線池空位超時
                .setConnectTimeout(Timeout.ofSeconds(3))            // TCP 連線建立超時
                .setResponseTimeout(Timeout.ofSeconds(10))          // 等待 HTTP response 超時
                .build();

        HTTP_CLIENT = HttpClients.custom()
                .setConnectionManager(CONN_MANAGER)
                .setDefaultRequestConfig(requestConfig)
                // 每隔 30 秒清理 expired 或超過 idle 時間的連線，防止連線池中積累 stale 連線
                .evictExpiredConnections()
                .evictIdleConnections(TimeValue.ofSeconds(30))
                .build();

        log.info("[MqPackReplayer] HTTP 連線池初始化完成：maxTotal=50, maxPerRoute=20, connTTL=60s");
    }

    /** skipRegExp 的編譯快取（key 為原始正則字串），避免每筆訊息重複編譯 */
    private static volatile String cachedSkipRegExp = "";
    private static volatile Pattern cachedSkipPattern;

    private MqPackReplayer() {
    }

    /**
     * 重放 MqPack（可依 skipRegExp 跳過）。
     * 若 {@code skipRegExp} 非空白且 {@code pack} 內任一 key 符合該正則式（{@code find()} 語意），
     * 則直接跳過重放並靜默回傳：不計入 success/fail，訊息仍會由呼叫方 ACK，避免無限重投遞。
     *
     * @param pack         要重放的 Redis 指令包
     * @param metrics      佇列指標統計器
     * @param remoteAppUrl 對端 App REST API 基礎網址 (如 http://onprem-app:8081)
     * @param skipRegExp   跳過重放的 key 正則式，空白表示不跳過
     */
    public static void replay(MqPack pack, QueueMetrics metrics, String remoteAppUrl, String skipRegExp) {
        if (shouldSkip(pack, skipRegExp)) {
            return;
        }
        replay(pack, metrics, remoteAppUrl);
    }

    /**
     * 重放 MqPack。
     * 若指定 {@code remoteAppUrl} 則透過 HTTP 連線池（Apache HttpClient 5）
     * 發送至遠端 App 的 {@code POST /api/replay} 端點；
     * 否則直接在本地呼叫 {@link #replayLocal} 執行。
     *
     * @param pack          要重放的 Redis 指令包
     * @param metrics       佇列指標統計器
     * @param remoteAppUrl  對端 App REST API 基礎網址 (如 http://onprem-app:8081)
     */
    public static void replay(MqPack pack, QueueMetrics metrics, String remoteAppUrl) {
        if (remoteAppUrl == null || remoteAppUrl.isBlank()) {
            replayLocal(pack, metrics);
            return;
        }
        try {
            // 1. 將 MqPack 序列化並編碼為 Base64
            byte[] bytes = MqPackSerializer.serialize(pack);
            String base64Payload = Base64.getEncoder().encodeToString(bytes);

            // 2. 使用 ObjectMapper 安全構建 JSON body（避免手工拼接導致特殊字元破壞 JSON）
            ObjectNode bodyNode = MAPPER.createObjectNode();
            bodyNode.put("payload", base64Payload);
            String jsonBody = MAPPER.writeValueAsString(bodyNode);

            // 3. 構建 HTTP POST 請求，目標 URL = remoteAppUrl/api/replay
            String targetUrl = remoteAppUrl.replaceAll("/+$", "") + "/api/replay";
            HttpPost httpPost = new HttpPost(targetUrl);
            httpPost.setEntity(new StringEntity(jsonBody, ContentType.APPLICATION_JSON));

            // 4. 透過連線池 CloseableHttpClient 發送請求（自動從池中取/還連線）
            int statusCode = HTTP_CLIENT.execute(httpPost, response -> response.getCode());
            if (statusCode >= 200 && statusCode < 300) {
                metrics.recordReplaySuccess();
            } else {
                log.warn("[MqPackReplayer] 遠端 replay 回傳非 2xx: {} -> status={}", targetUrl, statusCode);
                metrics.recordReplayFail();
            }
        } catch (Exception e) {
            log.warn("[MqPackReplayer] 遠端 replay 請求失敗: {}", remoteAppUrl, e);
            metrics.recordReplayFail();
        }
    }

    public static void replay(MqPack pack, QueueMetrics metrics) {
        replay(pack, metrics, null);
    }

    private static boolean shouldSkip(MqPack pack, String skipRegExp) {
        if (skipRegExp == null || skipRegExp.isBlank()) {
            return false;
        }
        try {
            Pattern pattern = compileSkipPattern(skipRegExp);
            if (pattern == null) {
                return false;
            }
            Command command = pack.getCommand();
            if (command == null) {
                return false;
            }
            List<byte[]> keys = command.getKeys();
            if (keys == null || keys.isEmpty()) {
                return false;
            }
            for (byte[] key : keys) {
                if (key == null) {
                    continue;
                }
                if (pattern.matcher(new String(key, StandardCharsets.UTF_8)).find()) {
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            log.warn("[MqPackReplayer] 判斷 skipRegExp 失敗，改為不跳過", e);
            return false;
        }
    }

    private static Pattern compileSkipPattern(String skipRegExp) {
        if (skipRegExp.equals(cachedSkipRegExp)) {
            return cachedSkipPattern;
        }
        Pattern compiled;
        try {
            compiled = Pattern.compile(skipRegExp);
        } catch (PatternSyntaxException e) {
            log.warn("[MqPackReplayer] skipRegExp 正則式無效，停用 skip 功能: {}", skipRegExp, e);
            compiled = null;
        }
        cachedSkipRegExp = skipRegExp;
        cachedSkipPattern = compiled;
        return compiled;
    }

    /**
     * 直接將 MqPack 中的原 Redis 命令發送至 Upstream Redis 主庫。
     * 
     * <p>注意：此處直接透過 UpstreamClientTemplate 寫入庫，繞過了 Proxy Plugin 層，
     * 保障重放寫入不會再度被 MQ Producer 攔截造成死迴圈。</p>
     *
     * @param pack    Redis 指令包
     * @param metrics 佇列指標統計器
     */
    public static void replayLocal(MqPack pack, QueueMetrics metrics) {
        try {
            // 1. 取得標的路由項目的直連 Upstream 客戶端模板
            IUpstreamClientTemplate template = GlobalRedisProxyEnv.getClientTemplateFactory()
                    .getOrInitialize(pack.getBid(), pack.getBgroup());

            // 2. 對 Upstream Redis 發送單筆原始指令 (如 SET/HSET/ZADD)
            List<CompletableFuture<Reply>> futures =
                    template.sendCommand(pack.getDb(), Collections.singletonList(pack.getCommand()));

            // 3. 處理重放執行結果並更新指標
            for (CompletableFuture<Reply> future : futures) {
                future.whenComplete((reply, ex) -> {
                    if (ex != null) {
                        metrics.recordReplayFail();
                    } else if (reply instanceof ErrorReply) {
                        CommandFailMonitor.incr(((ErrorReply) reply).getError());
                        metrics.recordReplayFail();
                    } else {
                        metrics.recordReplaySuccess();
                    }
                });
            }
        } catch (Exception e) {
            metrics.recordReplayFail();
        }
    }
}
