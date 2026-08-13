package com.example.sync.proxy.mq;

import com.example.sync.common.config.SyncProperties;
import com.example.sync.common.metrics.QueueMetrics;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPack;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPackSender;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPackSerializer;

/**
 * 雲端出站 MqPackSender：serialize → GCP Pub/Sub publish。
 *
 * <p>註冊為 Spring bean（無空建構子，須經 SpringProxyBeanFactory.getBean 解析），
 * FQCN 由 {@code mq.multi.write.sender.class.name} 指向。</p>
 */
public class GcpPubSubMqPackSender implements MqPackSender {

    private final PubSubClient client;
    private final String topic;
    private final QueueMetrics metrics;

    public GcpPubSubMqPackSender(PubSubClient client, SyncProperties.PubSubConfig config, QueueMetrics metrics) {
        this.client = client;
        this.topic = config.getTopic();
        this.metrics = metrics;
    }

    @Override
    public boolean send(MqPack pack) throws Exception {
        byte[] data = MqPackSerializer.serialize(pack);
        try {
            client.publish(topic, data);
            metrics.recordSent();
            return true;
        } catch (Exception e) {
            metrics.recordSendFail();
            throw e;
        }
    }
}
