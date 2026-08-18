package com.example.sync.proxy.plugin;

import com.example.sync.common.metrics.QueueMetrics;
import com.example.sync.proxy.mq.RedisPubSubBroker;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPack;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPackSerializer;
import com.netease.nim.camellia.redis.proxy.plugin.ProxyBeanFactory;
import com.netease.nim.camellia.redis.proxy.plugin.ProxyPlugin;

/**
 * Redis Pub/Sub 入站消費者插件（Consumer Proxy Plugin）：
 * 
 * <h3>資料流程與架構腳色：</h3>
 * <ol>
 *   <li><b>訂閱頻道 (SUBSCRIBE)</b>： Camellia Proxy 啟動時透過 {@link #init} 啟動 Daemon 執行緒訂閱 Redis Pub/Sub 頻道。</li>
 *   <li><b>接收訊息並解碼</b>： 收到頻道廣播廣播之訊息 Data 後，解碼為 {@link MqPack} 並累加消費計數。</li>
 *   <li><b>跨區重放</b>： 呼叫 {@link MqPackReplayer#replay} 將指令發送至遠端 App REST API 或本地 Redis。</li>
 * </ol>
 *
 * <p>註冊為 Spring Bean；全名 (FQCN) 經由 {@code camellia-redis-proxy.config.proxy.plugin.list} 配置。</p>
 */
public class RedisPubSubConsumerProxyPlugin implements ProxyPlugin {

    private final RedisPubSubBroker broker;
    private final QueueMetrics metrics;
    private final String remoteAppUrl;
    private final String skipRegExp;

    private volatile boolean running = false;
    private Thread thread;

    public RedisPubSubConsumerProxyPlugin(RedisPubSubBroker broker, QueueMetrics metrics, String remoteAppUrl, String skipRegExp) {
        this.broker = broker;
        this.metrics = metrics;
        this.remoteAppUrl = remoteAppUrl;
        this.skipRegExp = skipRegExp;
    }

    public RedisPubSubConsumerProxyPlugin(RedisPubSubBroker broker, QueueMetrics metrics, String remoteAppUrl) {
        this(broker, metrics, remoteAppUrl, null);
    }

    public RedisPubSubConsumerProxyPlugin(RedisPubSubBroker broker, QueueMetrics metrics) {
        this(broker, metrics, null, null);
    }

    @Override
    public void init(ProxyBeanFactory factory) {
        // 防止熱加載（Hot Reload）時重複啟動消費執行緒
        if (running) {
            return;
        }
        running = true;
        thread = new Thread(() -> broker.subscribe(this::onMessage), "redis-pubsub-consumer");
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * 收到 Redis Pub/Sub 廣播時觸發的回調
     */
    private void onMessage(byte[] data) {
        try {
            // 1. 反序列化 MqPack
            MqPack pack = MqPackSerializer.deserialize(data);
            metrics.recordConsumed();

            // 2. 重放指令至遠端 App API 或本地 Redis
            MqPackReplayer.replay(pack, metrics, remoteAppUrl, skipRegExp);
        } catch (Exception e) {
            metrics.recordReplayFail();
        }
    }
}
