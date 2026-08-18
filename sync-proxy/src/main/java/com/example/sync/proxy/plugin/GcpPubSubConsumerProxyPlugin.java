package com.example.sync.proxy.plugin;

import com.example.sync.common.config.SyncProperties;
import com.example.sync.common.metrics.QueueMetrics;
import com.example.sync.proxy.mq.PubSubClient;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPack;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPackSerializer;
import com.netease.nim.camellia.redis.proxy.plugin.ProxyBeanFactory;
import com.netease.nim.camellia.redis.proxy.plugin.ProxyPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * GCP Pub/Sub 入站消費者插件（Consumer Proxy Plugin）：
 * 
 * <h3>資料流程與架構腳色：</h3>
 * <ol>
 *   <li><b>啟動佇列消費</b>： Camellia Redis Proxy 初始化時調用 {@link #init(ProxyBeanFactory)}，啟動後台 Daemon 執行緒。</li>
 *   <li><b>訊息拉取 (Pull)</b>： 定期透過 {@link PubSubClient#pull} 向 GCP Pub/Sub 訂閱（Subscription）拉取 `MqPack` 訊息。</li>
 *   <li><b>反序列化與紀錄</b>： 將 Message Data 反序列化為 {@link MqPack}，並累加 {@link QueueMetrics#recordConsumed()}。</li>
 *   <li><b>跨區重放 (Replay)</b>： 調用 {@link MqPackReplayer#replay(MqPack, QueueMetrics, String)}：
 *       <ul>
 *         <li>若配置了 {@code remoteAppUrl}，則經由 REST API HTTP POST 傳至遠端 App（如 Cloud → OnPrem）。</li>
 *         <li>否則直接呼叫本地 Redis 重放（{@link MqPackReplayer#replayLocal}）。</li>
 *       </ul>
 *   </li>
 *   <li><b>確認（ACK）</b>： 重放完成後向 Pub/Sub 發送 Acknowledge (ACK)，完成此筆訊息消費。</li>
 * </ol>
 *
 * <p>註冊為 Spring Bean；全名 (FQCN) 經由 {@code camellia-redis-proxy.config.proxy.plugin.list} 配置。</p>
 */
public class GcpPubSubConsumerProxyPlugin implements ProxyPlugin {

    private static final Logger log = LoggerFactory.getLogger(GcpPubSubConsumerProxyPlugin.class);

    private final PubSubClient client;
    private final String topic;
    private final String subscription;
    private final int pollMaxMessages;
    private final long pollIntervalMs;
    private final QueueMetrics metrics;
    private final String remoteAppUrl;
    private final String skipRegExp;

    private volatile boolean running = false;
    private Thread thread;

    public GcpPubSubConsumerProxyPlugin(PubSubClient client, SyncProperties.PubSubConfig config, QueueMetrics metrics, String remoteAppUrl, String skipRegExp) {
        this.client = client;
        this.topic = config.getTopic();
        this.subscription = config.getSubscription();
        this.pollMaxMessages = config.getPollMaxMessages();
        this.pollIntervalMs = config.getPollIntervalMs();
        this.metrics = metrics;
        this.remoteAppUrl = remoteAppUrl;
        this.skipRegExp = skipRegExp;
    }

    public GcpPubSubConsumerProxyPlugin(PubSubClient client, SyncProperties.PubSubConfig config, QueueMetrics metrics, String remoteAppUrl) {
        this(client, config, metrics, remoteAppUrl, null);
    }

    public GcpPubSubConsumerProxyPlugin(PubSubClient client, SyncProperties.PubSubConfig config, QueueMetrics metrics) {
        this(client, config, metrics, null, null);
    }

    @Override
    public void init(ProxyBeanFactory factory) {
        // 防止熱加載（Hot Reload）時重複啟動消費執行緒
        if (running) {
            return;
        }
        running = true;
        thread = new Thread(this::consumeLoop, "gcp-pubsub-consumer");
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * 後台消費主迴圈：自動建立 Topic/Subscription，拉取訊息並呼叫 Replayer 重放。
     */
    private void consumeLoop() {
        try {
            // 確保目標 Topic 與 Subscription 已建立
            client.ensureTopic(topic);
            client.ensureSubscription(topic, subscription);
        } catch (Exception e) {
            // 初始化失敗記錄 log，消費執行緒停止
            log.error("[GcpPubSubConsumer] 初始化 Topic/Subscription 失敗，消費執行緒停止: topic={}, sub={}", topic, subscription, e);
            running = false;
            return;
        }
        while (running) {
            try {
                // 1. 從 GCP Pub/Sub 批次拉取訊息
                List<PubSubClient.PubSubMessage> messages = client.pull(subscription, pollMaxMessages);
                List<String> ackIds = new ArrayList<>();
                for (PubSubClient.PubSubMessage message : messages) {
                    try {
                        // 2. 反序列化 MqPack（包含原 Redis 命令物件）
                        MqPack pack = MqPackSerializer.deserialize(message.data());
                        metrics.recordConsumed();

                        // 3. 呼叫重放器發送至遠端 App API 或本地 Redis 主庫
                        MqPackReplayer.replay(pack, metrics, remoteAppUrl, skipRegExp);

                        // 4. 重放成功後才加入 ACK 清單（失敗的訊息不 ACK，讓 Pub/Sub 重新投遞）
                        if (message.ackId() != null && !message.ackId().isEmpty()) {
                            ackIds.add(message.ackId());
                        }
                    } catch (Exception e) {
                        // 重放失敗：不加入 ackIds，讓該訊息被 Pub/Sub nack 後重投遞
                        metrics.recordReplayFail();
                        log.warn("[GcpPubSubConsumer] 訊息重放失敗，將由 Pub/Sub 重新投遞", e);
                    }
                }

                // 5. 若無訊息則休眠，否則批次 ACK 成功訊息
                if (messages.isEmpty()) {
                    sleep(pollIntervalMs);
                } else if (!ackIds.isEmpty()) {
                    client.acknowledge(subscription, ackIds);
                }
            } catch (Exception e) {
                log.warn("[GcpPubSubConsumer] Pull 異常，等待後重試", e);
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
