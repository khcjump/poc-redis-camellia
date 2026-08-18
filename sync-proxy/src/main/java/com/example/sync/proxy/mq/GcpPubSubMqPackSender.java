package com.example.sync.proxy.mq;

import com.example.sync.common.config.SyncProperties;
import com.example.sync.common.metrics.QueueMetrics;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPack;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPackSender;
import com.netease.nim.camellia.redis.proxy.mq.common.MqPackSerializer;

/**
 * GCP Pub/Sub 出站發送器（Outbound MqPackSender）：
 * 
 * <h3>資料流程與架構腳色：</h3>
 * <ol>
 *   <li><b>寫入攔截 (Write Interception)</b>：
 *       當用戶對 Camellia Redis Proxy 發送寫入命令（SET/HSET/ZADD）時，
 *       Proxy 插件層 {@code MqMultiWriteProducerProxyPlugin} 攔截該 Command 並封裝為 {@link MqPack}。
 *   </li>
 *   <li><b>序列化與發送 (Serialize & Publish)</b>：
 *       將 {@link MqPack} 序列化為位元組陣列，調用 {@link PubSubClient#publish(String, byte[])} 發送至 GCP Pub/Sub。
 *   </li>
 *   <li><b>指標紀錄 (Metrics)</b>：
 *       成功時呼叫 {@link QueueMetrics#recordSent()} 累加發送數；失敗時累加 {@link QueueMetrics#recordSendFail()} 並拋出例外。
 *   </li>
 * </ol>
 *
 * <p>註冊為 Spring Bean；全名 (FQCN) 經由 {@code camellia-redis-proxy.config.mq.multi.write.sender.class.name} 配置。</p>
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
        // 1. 序列化指令包
        byte[] data = MqPackSerializer.serialize(pack);
        try {
            // 2. 發送至 GCP Pub/Sub 出站 Topic
            client.publish(topic, data);
            metrics.recordSent();
            return true;
        } catch (Exception e) {
            metrics.recordSendFail();
            throw e;
        }
    }
}
