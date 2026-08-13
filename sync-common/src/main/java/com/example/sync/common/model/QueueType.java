package com.example.sync.common.model;

/**
 * 訊息佇列方案（出站/入站佇列的實作類型）。
 */
public enum QueueType {
    /** 雲端 GCP Pub/Sub（demo 對準 emulator） */
    GcpPubSub,
    /** Redis Pub/Sub（純 pub/sub 語意、無持久化） */
    RedisPubSub,
    /** Apache Kafka（Camellia 已附 producer/consumer） */
    Kafka
}
