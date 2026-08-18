package com.example.sync.proxy.mq;

import com.example.sync.common.metrics.QueueMetrics;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPack;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPackSender;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPackSerializer;

/**
 * Redis Pub/Sub 出站發送器（Outbound MqPackSender）：
 * 
 * <h3>資料流程與架構腳色：</h3>
 * <ol>
 *   <li><b>寫入攔截</b>： 攔截 Redis 寫入命令並包裝為 {@link MqPack}。</li>
 *   <li><b>廣播發送 (PUBLISH)</b>： 序列化位元組並透過 {@link RedisPubSubBroker#publish} 發送至指定頻道。</li>
 *   <li><b>指標紀錄</b>： 成功紀錄發送數，失敗紀錄失敗數。</li>
 * </ol>
 *
 * <p>註冊為 Spring Bean；全名 (FQCN) 經由 {@code camellia-redis-proxy.config.mq.multi.write.sender.class.name} 配置。</p>
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
        // 1. 序列化 MqPack
        byte[] data = MqPackSerializer.serialize(pack);
        try {
            // 2. 發送至 Redis Pub/Sub 頻道
            broker.publish(data);
            metrics.recordSent();
            return true;
        } catch (Exception e) {
            metrics.recordSendFail();
            throw e;
        }
    }
}
