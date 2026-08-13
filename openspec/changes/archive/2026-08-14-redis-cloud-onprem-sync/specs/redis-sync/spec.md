## Purpose

提供雲端與地端之間的雙向 Redis session 資料同步，讓雙活 Session 服務在進線切換時兩端資料保持一致。

## ADDED Requirements

### Requirement: Gateway 主寫與輔發
當系統以 RedisGateway 角色運行時，SHALL 將寫入指令以主路徑寫入本地 Redis，並以輔路徑將相同變更發佈到本地 region 的出站訊息佇列。

#### Scenario: 主寫並發佈變更
- **WHEN** Gateway 收到一筆 session 的寫入指令（例如 SET）
- **THEN** 該指令被寫入本地 Redis，且一筆攜帶相同指令的訊息被發佈到本地出站佇列

### Requirement: SyncWorker 跨區重放
當系統以 SyncWorker 角色運行時，SHALL 消費對岸 region 出站佇列中的訊息，並將訊息中的指令重放到本地 Redis。

#### Scenario: 消費對岸變更
- **WHEN** 對岸佇列出現一筆變更訊息
- **THEN** SyncWorker 將該指令套用到本地 Redis，使本地資料與對岸一致

### Requirement: 雙向同步
雲端與地端 SHALL 彼此雙向同步變更，使任一端的寫入最終出現在另一端。

#### Scenario: 雲端寫入同步至地端
- **WHEN** 雲端寫入一筆 session key
- **THEN** 該 key 於一定延遲內出現在地端 Redis

#### Scenario: 地端寫入同步至雲端
- **WHEN** 地端寫入一筆 session key
- **THEN** 該 key 於一定延遲內出現在雲端 Redis

### Requirement: 衝突政策（LWW）
當同一 key 在兩端同時被寫入不同值時，SHALL 以最後寫入者勝出（LWW），不進行衝突調解。

#### Scenario: 並行寫入衝突
- **WHEN** 同一 key 在雲端與地端幾乎同時被寫入不同值
- **THEN** 兩端最終收斂至最後完成的寫入值

### Requirement: EXPIRE/SETEX 轉送
EXPIRE 與 SETEX 等帶有存活時間的指令 SHALL 被正確轉送至對岸，使 key 的 TTL 語意被保留；少量轉送時間差可被忽略。

#### Scenario: SETEX 同步
- **WHEN** 一端執行 SETEX 設定 key 與 TTL
- **THEN** 對岸出現相同 key，且其剩餘 TTL 與來源一致（允許少量時間差）

### Requirement: 佇列保留 TTL 可調
佇列訊息的保留時間（TTL）SHALL 為可調參數，定性為短期保存特性。

#### Scenario: 調整 TTL
- **WHEN** 管理端調整佇列保留 TTL 參數
- **THEN** 佇列訊息按新 TTL 保留，逾期的訊息不再被消費
