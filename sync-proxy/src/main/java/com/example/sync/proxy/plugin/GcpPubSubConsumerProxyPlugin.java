package com.example.sync.proxy.plugin;

import com.example.sync.common.config.SyncProperties;
import com.example.sync.common.metrics.QueueMetrics;
import com.example.sync.proxy.mq.PubSubClient;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPack;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPackSerializer;
import com.netease.nim.camellia.redis.proxy.plugin.ProxyBeanFactory;
import com.netease.nim.camellia.redis.proxy.plugin.ProxyPlugin;

import java.util.ArrayList;
import java.util.List;

/**
 * GCP Pub/Sub 入站 consumer：拉取雲端出站佇列 → 重放本地 Redis。
 *
 * <p>註冊為 Spring bean（無空建構子）；FQCN 由 {@code proxy.plugin.list} 指向。
 * {@link #init(ProxyBeanFactory)} 起 daemon thread，並以 {@code running} 旗標防熱載重複起。</p>
 */
public class GcpPubSubConsumerProxyPlugin implements ProxyPlugin {

    private final PubSubClient client;
    private final String topic;
    private final String subscription;
    private final int pollMaxMessages;
    private final long pollIntervalMs;
    private final QueueMetrics metrics;

    private volatile boolean running = false;
    private Thread thread;

    public GcpPubSubConsumerProxyPlugin(PubSubClient client, SyncProperties.PubSubConfig config, QueueMetrics metrics) {
        this.client = client;
        this.topic = config.getTopic();
        this.subscription = config.getSubscription();
        this.pollMaxMessages = config.getPollMaxMessages();
        this.pollIntervalMs = config.getPollIntervalMs();
        this.metrics = metrics;
    }

    @Override
    public void init(ProxyBeanFactory factory) {
        if (running) {
            return;
        }
        running = true;
        thread = new Thread(this::consumeLoop, "gcp-pubsub-consumer");
        thread.setDaemon(true);
        thread.start();
    }

    private void consumeLoop() {
        try {
            client.ensureTopic(topic);
            client.ensureSubscription(topic, subscription);
        } catch (Exception e) {
            return;
        }
        while (running) {
            try {
                List<PubSubClient.PubSubMessage> messages = client.pull(subscription, pollMaxMessages);
                List<String> ackIds = new ArrayList<>();
                for (PubSubClient.PubSubMessage message : messages) {
                    if (message.ackId() != null && !message.ackId().isEmpty()) {
                        ackIds.add(message.ackId());
                    }
                    try {
                        MqPack pack = MqPackSerializer.deserialize(message.data());
                        metrics.recordConsumed();
                        MqPackReplayer.replay(pack, metrics);
                    } catch (Exception e) {
                        metrics.recordReplayFail();
                    }
                }
                if (ackIds.isEmpty()) {
                    sleep(pollIntervalMs);
                } else {
                    client.acknowledge(subscription, ackIds);
                }
            } catch (Exception e) {
                sleep(pollIntervalMs);
            }
        }
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}
