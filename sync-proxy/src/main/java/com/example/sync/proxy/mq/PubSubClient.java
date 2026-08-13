package com.example.sync.proxy.mq;

import java.util.List;

/**
 * GCP Pub/Sub 抽象（demo 對準 emulator REST，避免 grpc-netty 與 Camellia netty 4.2 衝突）。
 *
 * <p>抽象化成介面，使 {@code GcpPubSubMqPackSender} 與 {@code GcpPubSubConsumerProxyPlugin}
 * 皆可替換為真實 GCP Pub/Sub 實作（僅需換 {@link PubSubClient} 實作）。</p>
 */
public interface PubSubClient {

    /** 確保 topic 存在（emulator PUT 冪等） */
    void ensureTopic(String topic) throws Exception;

    /** 確保 subscription 存在並綁定 topic（emulator PUT 冪等） */
    void ensureSubscription(String topic, String subscription) throws Exception;

    /** 發送一則二進位訊息至 topic */
    void publish(String topic, byte[] data) throws Exception;

    /** 拉取最多 maxMessages 則（returnImmediately，不回空阻塞） */
    List<PubSubMessage> pull(String subscription, int maxMessages) throws Exception;

    /** 確認（ack）訊息 */
    void acknowledge(String subscription, List<String> ackIds) throws Exception;

    /** 釋放資源 */
    void close();

    /** 拉取到的訊息：data = 二進位 payload，ackId = 確認憑證 */
    record PubSubMessage(byte[] data, String ackId) {
    }
}
