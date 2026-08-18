package com.example.sync.proxy.plugin;

import com.example.sync.common.config.SyncProperties;
import com.example.sync.common.metrics.QueueMetrics;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPack;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPackSerializer;
import com.netease.nim.camellia.redis.proxy.plugin.ProxyBeanFactory;
import com.netease.nim.camellia.redis.proxy.plugin.ProxyPlugin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Kafka 入站消費者插件（Consumer Proxy Plugin）：
 * 
 * <h3>資料流程與架構腳色：</h3>
 * <ol>
 *   <li><b>啟動佇列消費</b>： Camellia Redis Proxy 初始化時調用 {@link #init(ProxyBeanFactory)}，啟動獨立 Daemon 執行緒。</li>
 *   <li><b>訊息拉取 (Poll)</b>： 定期透過 {@link KafkaConsumer#poll(Duration)} 訂閱並拉取 Kafka Topic 內的 `MqPack` 位元組。</li>
 *   <li><b>反序列化與指標紀錄</b>： 將 `record.value()` 序列化位元組解碼為 {@link MqPack}，並累加 {@link QueueMetrics#recordConsumed()}。</li>
 *   <li><b>跨區重放 (Replay)</b>： 調用 {@link MqPackReplayer#replay(MqPack, QueueMetrics, String)}：
 *       <ul>
 *         <li>若配置了 {@code remoteAppUrl}，則經由 REST API HTTP POST 傳至遠端 App（如 OnPrem → Cloud）。</li>
 *         <li>否則直接呼叫本地 Redis 重放（{@link MqPackReplayer#replayLocal}）。</li>
 *       </ul>
 *   </li>
 *   <li><b>Auto Commit</b>： 採用單執行緒保序 Poll + Auto Commit 確保消費與重發順序。</li>
 * </ol>
 *
 * <p>註冊為 Spring Bean；全名 (FQCN) 經由 {@code camellia-redis-proxy.config.proxy.plugin.list} 配置。</p>
 */
public class KafkaConsumerProxyPlugin implements ProxyPlugin {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerProxyPlugin.class);

    private final SyncProperties.KafkaConfig config;
    private final QueueMetrics metrics;
    private final String remoteAppUrl;
    private final String skipRegExp;

    private volatile boolean running = false;
    private volatile KafkaConsumer<String, byte[]> consumer;
    private Thread thread;

    public KafkaConsumerProxyPlugin(SyncProperties.KafkaConfig config, QueueMetrics metrics, String remoteAppUrl, String skipRegExp) {
        this.config = config;
        this.metrics = metrics;
        this.remoteAppUrl = remoteAppUrl;
        this.skipRegExp = skipRegExp;
    }

    public KafkaConsumerProxyPlugin(SyncProperties.KafkaConfig config, QueueMetrics metrics, String remoteAppUrl) {
        this(config, metrics, remoteAppUrl, null);
    }

    public KafkaConsumerProxyPlugin(SyncProperties.KafkaConfig config, QueueMetrics metrics) {
        this(config, metrics, null, null);
    }

    @Override
    public void init(ProxyBeanFactory factory) {
        // 防止熱加載（Hot Reload）時重複啟動消費執行緒
        if (running) {
            return;
        }
        running = true;
        thread = new Thread(this::consumeLoop, "kafka-consumer");
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * 後台 Kafka 消費主迴圈：設定 Kafka Consumer、訂閱 Topic 並持續 Poll 訊息重放。
     */
    private void consumeLoop() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<>(props);
        try {
            consumer.subscribe(Collections.singletonList(config.getTopic()));
            while (running) {
                // 1. 從 Kafka 批次 Poll 訊息
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, byte[]> record : records) {
                    try {
                        // 2. 反序列化 MqPack（包含原 Redis 命令物件）
                        MqPack pack = MqPackSerializer.deserialize(record.value());
                        metrics.recordConsumed();

                        // 3. 呼叫重放器發送至遠端 App API 或本地 Redis 主庫
                        MqPackReplayer.replay(pack, metrics, remoteAppUrl, skipRegExp);
                    } catch (Exception e) {
                        metrics.recordReplayFail();
                        log.warn("[KafkaConsumer] 訊息重放失敗", e);
                    }
                }
            }
        } catch (WakeupException e) {
            // 正常關機信號（由 stop() 呼叫 wakeup() 觸發）
            log.info("[KafkaConsumer] 收到 wakeup 信號，執行緒正常停止");
        } finally {
            consumer.close();
            log.info("[KafkaConsumer] KafkaConsumer 已關閉");
        }
    }

    /** 停止消費（如需外部呼叫）。 */
    public void stop() {
        running = false;
        KafkaConsumer<String, byte[]> c = consumer;
        if (c != null) {
            c.wakeup();
        }
    }
}
