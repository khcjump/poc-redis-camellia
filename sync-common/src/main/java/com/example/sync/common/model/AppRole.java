package com.example.sync.common.model;

/**
 * 單一程式運作方式（相同程式、不同角色）。
 */
public enum AppRole {
    /** Redis 閘道：主寫本地 + 發本地出站佇列（掛 producer plugin） */
    RedisGateway,
    /** 同步 worker：消費對岸出站佇列 + 重放本地（掛 consumer plugin） */
    SyncWorker,
    /** 控制台：僅 REST API 與檢查功能 */
    Console,
    /** 全部：producer + consumer + console 共存於同一 JVM */
    All
}
