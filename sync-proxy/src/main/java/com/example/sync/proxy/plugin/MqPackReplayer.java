package com.example.sync.proxy.plugin;

import com.example.sync.common.metrics.QueueMetrics;
import com.netease.nim.camellia.redis.proxy.conf.GlobalRedisProxyEnv;
import com.netease.nim.camellia.redis.proxy.monitor.CommandFailMonitor;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPack;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPackSerializer;
import com.netease.nim.camellia.redis.proxy.reply.ErrorReply;
import com.netease.nim.camellia.redis.proxy.reply.Reply;
import com.netease.nim.camellia.redis.proxy.upstream.IUpstreamClientTemplate;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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
 * </ul>
 */
public final class MqPackReplayer {

    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    private MqPackReplayer() {
    }

    /**
     * 重放 MqPack。
     * 若指定 {@code remoteAppUrl} 則透過 HTTP REST API 傳送至遠端 App 的 {@code /api/replay} 端點，
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
            String jsonBody = "{\"payload\":\"" + base64Payload + "\"}";

            // 2. 構建 HTTP POST /api/replay 請求
            String targetUrl = remoteAppUrl.replaceAll("/+$", "") + "/api/replay";
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(targetUrl))
                    .timeout(Duration.ofSeconds(5))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                    .build();

            // 3. 非同步/同步發送至遠端 App API
            HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                metrics.recordReplaySuccess();
            } else {
                metrics.recordReplayFail();
            }
        } catch (Exception e) {
            metrics.recordReplayFail();
        }
    }

    public static void replay(MqPack pack, QueueMetrics metrics) {
        replay(pack, metrics, null);
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
