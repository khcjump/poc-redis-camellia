package com.example.sync.proxy.plugin;

import com.example.sync.common.metrics.QueueMetrics;
import com.netease.nim.camellia.redis.proxy.conf.GlobalRedisProxyEnv;
import com.netease.nim.camellia.redis.proxy.monitor.CommandFailMonitor;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPack;
import com.netease.nim.camellia.redis.proxy.reply.ErrorReply;
import com.netease.nim.camellia.redis.proxy.reply.Reply;
import com.netease.nim.camellia.redis.proxy.upstream.IUpstreamClientTemplate;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 跨區重放（replay）：把對岸的 MqPack 指令重放至本地 Redis 後端。
 *
 * <p>複製 Camellia {@code KafkaMqPackConsumerProxyPlugin} 的重放 pattern：經由
 * {@link GlobalRedisProxyEnv#getClientTemplateFactory()} 取得 upstream template，再
 * {@code sendCommand(db, commands)} 送原指令。bid/bgroup 直接透傳（非多租戶時工廠回預設 template）。</p>
 */
public final class MqPackReplayer {

    private MqPackReplayer() {
    }

    public static void replay(MqPack pack, QueueMetrics metrics) {
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
