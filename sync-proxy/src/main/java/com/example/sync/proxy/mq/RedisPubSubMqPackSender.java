package com.example.sync.proxy.mq;

import com.example.sync.common.metrics.QueueMetrics;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPack;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPackSender;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPackSerializer;

/**
 * 地端出站 MqPackSender：serialize → Redis Pub/Sub publish（純 pub/sub 語意、無持久化）。
 */
public class RedisPubSubMqPackSender implements MqPackSender {

    private final RedisPubSubBroker broker;
    private final QueueMetrics metrics;

    public RedisPubSubMqPackSender(RedisPubSubBroker broker, QueueMetrics metrics) {
        this.broker = broker;
        this.metrics = metrics;
    }

    @Override
    public boolean send(MqPack pack) throws Exception {
        byte[] data = MqPackSerializer.serialize(pack);
        try {
            broker.publish(data);
            metrics.recordSent();
            return true;
        } catch (Exception e) {
            metrics.recordSendFail();
            throw e;
        }
    }
}
