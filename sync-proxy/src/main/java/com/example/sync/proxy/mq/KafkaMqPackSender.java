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
 * 自建 Kafka MqPackSender：以 kafka-clients 3.9 直接送，補足 Camellia 內建 sender 缺 MQ 指標的缺口。
 *
 * <p>以命令首 key 當分區 key 保序（同 key 同分區）；非阻塞 {@code send} + callback 更新 in-flight 與計數。</p>
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
        byte[] data = MqPackSerializer.serialize(pack);
        String key = null;
        List<byte[]> keys = pack.getCommand().getKeys();
        if (keys != null && !keys.isEmpty()) {
            key = new String(keys.get(0));
        }
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, data);
        inFlight.incrementAndGet();
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
