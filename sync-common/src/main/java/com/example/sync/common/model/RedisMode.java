package com.example.sync.common.model;

/**
 * Redis 後端拓樸。
 */
public enum RedisMode {
    /** 單一 Redis 節點 */
    Single,
    /** Sentinel 高可用（地端） */
    Sentinel,
    /** 讀寫分離雙 IP（雲端 Memorystore：1 讀 1 寫） */
    ReadWrite
}
