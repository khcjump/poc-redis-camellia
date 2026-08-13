# mq-adapters Specification

## Purpose
讓同步機制可依設定在 GCP PubSub、Redis PubSub 與 Kafka 三種訊息佇列之間切換，實現「不同訊息佇列方案」需求。
## Requirements
### Requirement: QueueType 可插拔
系統 SHALL 支援 GcpPubSub、RedisPubSub、Kafka 三種佇列後端，並以 QueueType 參數於啟動時選擇。

#### Scenario: 切換佇列後端
- **WHEN** 以不同 QueueType 啟動系統
- **THEN** 同步變更訊息改經由對應的佇列後端傳遞，同步行為不變

### Requirement: 跨區佇列拓樸
每個 region SHALL 擁有一個出站佇列，並由對岸 region 的 SyncWorker 跨區消費。

#### Scenario: 雲端發佈、地端消費
- **WHEN** 雲端 Gateway 發佈變更至雲端出站佇列
- **THEN** 地端 SyncWorker 從雲端出站佇列消費該變更

#### Scenario: 地端發佈、雲端消費
- **WHEN** 地端 Gateway 發佈變更至地端出站佇列
- **THEN** 雲端 SyncWorker 從地端出站佇列消費該變更

### Requirement: 訊息攜帶原始指令
佇列訊息 SHALL 攜帶原始 Redis 指令（RESP 位元組）與租戶識別（bid/bgroup），使消費端可原樣重放。

#### Scenario: 指令原樣重放
- **WHEN** 消費端收到一筆訊息
- **THEN** 消費端可將訊息中的原始指令完整重放至目標 Redis，無需額外轉譯

