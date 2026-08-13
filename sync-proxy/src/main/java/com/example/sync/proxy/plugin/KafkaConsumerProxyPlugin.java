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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * 自建 Kafka 入站 consumer：poll 地端出站 topic → 重放本地 Redis。
 *
 * <p>單執行緒逐則重放（保序）；auto-commit。補足 Camellia 內建 consumer 缺 MQ 指標的缺口。</p>
 */
public class KafkaConsumerProxyPlugin implements ProxyPlugin {

    private final SyncProperties.KafkaConfig config;
    private final QueueMetrics metrics;

    private volatile boolean running = false;
    private Thread thread;

    public KafkaConsumerProxyPlugin(SyncProperties.KafkaConfig config, QueueMetrics metrics) {
        this.config = config;
        this.metrics = metrics;
    }

    @Override
    public void init(ProxyBeanFactory factory) {
        if (running) {
            return;
        }
        running = true;
        thread = new Thread(this::consumeLoop, "kafka-consumer");
        thread.setDaemon(true);
        thread.start();
    }

    private void consumeLoop() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(config.getTopic()));
            while (running) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, byte[]> record : records) {
                    try {
                        MqPack pack = MqPackSerializer.deserialize(record.value());
                        metrics.recordConsumed();
                        MqPackReplayer.replay(pack, metrics);
                    } catch (Exception e) {
                        metrics.recordReplayFail();
                    }
                }
            }
        }
    }
}
