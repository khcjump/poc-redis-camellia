package com.example.sync.proxy.mq;

import com.example.sync.common.config.SyncProperties;
import com.example.sync.common.metrics.QueueMetrics;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPack;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPackSender;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPackSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Kafka 出站發送器（Outbound MqPackSender）：
 * 
 * <h3>資料流程與架構腳色：</h3>
 * <ol>
 *   <li><b>寫入攔截 (Write Interception)</b>：
 *       Camellia Redis Proxy 收到用戶 Redis 寫入命令後，將 {@link MqPack} 送至此發送器。
 *   </li>
 *   <li><b>Partition Key 提取（保序）</b>：
 *       提取命令第一個 Redis Key 當作 Kafka Message Key（{@code pack.getCommand().getKeys().get(0)}），
 *       確保**同一 Redis Key 的所有修改都會分發至同一個 Kafka Partition**，保障強順序性。
 *   </li>
 *   <li><b>非同步發送與 Callback</b>：
 *       呼叫 {@link KafkaProducer#send} 非同步發送，並在 Completion Callback 中更新 In-Flight 數量與 {@link QueueMetrics}。
 *   </li>
 * </ol>
 *
 * <p>註冊為 Spring Bean；全名 (FQCN) 經由 {@code camellia-redis-proxy.config.mq.multi.write.sender.class.name} 配置。</p>
 */
public class KafkaMqPackSender implements MqPackSender {

    private final KafkaProducer<String, byte[]> producer;
    private final String topic;
    private final QueueMetrics metrics;
    private final AtomicInteger inFlight = new AtomicInteger();

    public KafkaMqPackSender(SyncProperties.KafkaConfig config, QueueMetrics metrics) {
        this.topic = config.getTopic();
        this.metrics = metrics;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "3000");
        this.producer = new KafkaProducer<>(props);
    }

    /** in-flight 供應者（供 gauge 掛載）。 */
    public long inFlightCount() {
        return inFlight.get();
    }

    @Override
    public boolean send(MqPack pack) throws Exception {
        // 1. 序列化 MqPack
        byte[] data = MqPackSerializer.serialize(pack);

        // 2. 提取第一個 Redis Key 作為 Kafka Partition Key (保序)
        String key = null;
        List<byte[]> keys = pack.getCommand().getKeys();
        if (keys != null && !keys.isEmpty()) {
            key = new String(keys.get(0));
        }

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, data);
        inFlight.incrementAndGet();

        // 3. 非同步發送至 Kafka 并設定 Completion Callback
        producer.send(record, (metadata, ex) -> {
            inFlight.decrementAndGet();
            if (ex != null) {
                metrics.recordSendFail();
            } else {
                metrics.recordSent();
            }
        });
        return true;
    }

    public void close() {
        producer.close();
    }
}
