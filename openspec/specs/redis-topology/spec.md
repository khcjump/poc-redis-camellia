# redis-topology Specification

## Purpose
讓系統依 RedisMode 連接到不同拓樸的 Redis 後端：單機、Sentinel 高可用與雲端讀寫分離。
## Requirements
### Requirement: Single 模式
RedisMode=Single SHALL 使系統連接到單一 Redis 實例。

#### Scenario: 連接單機
- **WHEN** 以 RedisMode=Single 啟動並提供單一 Redis 位址
- **THEN** 所有讀寫均導向該實例

### Requirement: Sentinel 模式
RedisMode=Sentinel SHALL 使系統透過 Redis Sentinel 連接到 master/slave 拓樸，並支援故障轉移。

#### Scenario: 經由 Sentinel 連線
- **WHEN** 以 RedisMode=Sentinel 啟動並提供 sentinel 位址
- **THEN** 寫入導向現任 master，讀取依設定導向 master 或 slave

### Requirement: ReadWrite 讀寫分離模式
RedisMode=ReadWrite SHALL 使系統使用獨立的讀端與寫端位址（如雲端 Memorystore 的讀/寫兩 IP），讀操作導向讀端、寫操作導向寫端。

#### Scenario: 讀寫分離路由
- **WHEN** 以 RedisMode=ReadWrite 啟動並提供讀 IP 與寫 IP
- **THEN** 寫指令導向寫 IP，讀指令導向讀 IP

