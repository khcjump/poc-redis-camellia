package com.example.sync.proxy.plugin;

import com.example.sync.common.metrics.QueueMetrics;
import com.example.sync.proxy.mq.RedisPubSubBroker;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPack;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPackSerializer;
import com.netease.nim.camellia.redis.proxy.plugin.ProxyBeanFactory;
import com.netease.nim.camellia.redis.proxy.plugin.ProxyPlugin;

/**
 * Redis Pub/Sub 入站 consumer：訂閱對岸出站 channel → 重放本地 Redis。
 *
 * <p>純 pub/sub 語意（無持久化、無 ack）：訊息在訂閱前已發出的會遺失，屬短期保存功能特性。</p>
 */
public class RedisPubSubConsumerProxyPlugin implements ProxyPlugin {

    private final RedisPubSubBroker broker;
    private final QueueMetrics metrics;

    private volatile boolean running = false;
    private Thread thread;

    public RedisPubSubConsumerProxyPlugin(RedisPubSubBroker broker, QueueMetrics metrics) {
        this.broker = broker;
        this.metrics = metrics;
    }

    @Override
    public void init(ProxyBeanFactory factory) {
        if (running) {
            return;
        }
        running = true;
        thread = new Thread(() -> broker.subscribe(this::onMessage), "redis-pubsub-consumer");
        thread.setDaemon(true);
        thread.start();
    }

    private void onMessage(byte[] data) {
        try {
            MqPack pack = MqPackSerializer.deserialize(data);
            metrics.recordConsumed();
            MqPackReplayer.replay(pack, metrics);
        } catch (Exception e) {
            metrics.recordReplayFail();
        }
    }
}
