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
 * 跨區重放（replay）：把對岸的 MqPack 指令重放至 Redis 後端。
 *
 * <p>支援直接重放至本地 Redis（{@link #replayLocal}），以及跨區叫用對端 REST API（{@link #replay}）以實現雲地跨區同步。</p>
 */
public final class MqPackReplayer {

    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    private MqPackReplayer() {
    }

    /**
     * 重放 MqPack。若指定 remoteAppUrl 則透過 REST API 叫用遠端 App 重放，否則直接重放至本地 Redis。
     */
    public static void replay(MqPack pack, QueueMetrics metrics, String remoteAppUrl) {
        if (remoteAppUrl == null || remoteAppUrl.isBlank()) {
            replayLocal(pack, metrics);
            return;
        }

        try {
            byte[] bytes = MqPackSerializer.serialize(pack);
            String base64Payload = Base64.getEncoder().encodeToString(bytes);
            String jsonBody = "{\"payload\":\"" + base64Payload + "\"}";

            String targetUrl = remoteAppUrl.replaceAll("/+$", "") + "/api/replay";
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(targetUrl))
                    .timeout(Duration.ofSeconds(5))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                    .build();

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
     * 直接將 MqPack 指令重放至本地 Redis 後端。
     */
    public static void replayLocal(MqPack pack, QueueMetrics metrics) {
        try {
            IUpstreamClientTemplate template = GlobalRedisProxyEnv.getClientTemplateFactory()
                    .getOrInitialize(pack.getBid(), pack.getBgroup());
            List<CompletableFuture<Reply>> futures =
                    template.sendCommand(pack.getDb(), Collections.singletonList(pack.getCommand()));
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
