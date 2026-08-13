## Why

需要一套雲端（GCP Memorystore）↔ 地端（Redis Sentinel）的 Redis 資料同步機制，讓雙活（active-active）Session 服務在「大小網」進線切換時，兩端的 Session 資料保持一致。以 Camellia（NetEase Redis proxy）作為技術棧核心，讓「單一程式專案」依參數（Role / Location / QueueType / RedisMode）運行不同角色，避免分別維護多套程式。

## What Changes

建立一個全新的 Java + Spring Boot 專案（greenfield），單一可執行檔依啟動參數切換「運作方式」：

- **單一程式、多角色**：同一支 Spring Boot 程式以參數切換 `RedisGateway`（主寫本地 Redis + 輔發變更到訊息佇列）與 `SyncWorker`（消費佇列 + 跨區重放到對岸 Redis）；支援 `All` 模式在同一個 JVM 同時掛 producer + consumer。
- **整合 Camellia 作為同步引擎**：以 Camellia 為實際依賴，擴充其 MQ 雙寫擴充點（`MqPackSender` / consumer plugin），不重寫核心代理邏輯。
- **可插拔訊息佇列**：實作三種 `MqPackSender` 轉接 — `GcpPubSub` / `RedisPubSub` / `Kafka`，以 `QueueType` 參數切換。
- **Redis 拓樸路由**：以 Camellia `ResourceTable` 支援 `Single` / `Sentinel`（地端）/ `ReadWrite`（雲端 Memorystore，讀寫分離 2 IP）。
- **一致性政策**：雙向同步採 LWW（last-write-wins），忽略衝突調解；必須正確轉送 `EXPIRE` / `SETEX` 指令（原始 RESP 重放自然取得）；佇列 TTL 為可調參數（定性為「短期保存特性」）。
- **docker-compose.yml**：以 `cloud-` / `onprem-` 前綴命名 instance，一鍵起雲地兩端。
- **Web UI（demo）**：Node 前端 + Spring Boot REST 後端，提供「操作」（寫入 session、切換大小網、調參數）與「檢查」（兩端一致性比對、同步延遲、佇列積壓、元件健康）。

## Capabilities

### New Capabilities

- `redis-sync`: 雲地雙向 Redis session 同步核心 — Gateway（主寫 + 發佇列）、SyncWorker（消費 + 跨區重放）、LWW 衝突政策、EXPIRE/SETEX 轉送、TTL 參數化。
- `mq-adapters`: 可插拔訊息佇列轉接 — `MqPackSender` 介面之 `GcpPubSub` / `RedisPubSub` / `Kafka` 三種實作，每 region 各自擁有一個出站佇列、跨區消費。
- `redis-topology`: Redis 存取拓樸路由 — `Single` / `Sentinel`（地端）/ `ReadWrite`（雲端 Memorystore 讀寫分離）三種 `RedisMode`。
- `console-ui`: demo 用 Web UI — 操作（寫入 session、切換大小網、調 QueueType/TTL）與檢查（兩端一致性、同步延遲、佇列積壓、元件健康）。

### Modified Capabilities

（無 — 全新專案，無既有 capability。）

## Impact

- **程式碼**：全新專案，無既有程式碼需修改。
- **依賴**：`netease-im/camellia`（Java, server 需 Java 21）、Spring Boot、Jedis（Camellia 底層）、Kafka/Redpanda、GCP PubSub emulator、Redis（含 sentinel 組態）、Node.js（前端建置）。
- **部署**：Docker + docker-compose（`cloud-` / `onprem-` 前綴）。
- **系統**：雲端 GCP Memorystore（讀寫分離 2 IP，或本地模擬替身）、地端 Redis Sentinel。

### 待確認的假設（已採用推薦預設，可在 review 時更正）

1. **Memorystore**：預設「本地模擬」（docker 起 `cloud-redis-write` + `cloud-redis-read` 兩 IP 當替身），真實 GCP Memorystore 連線以環境變數可切換。
2. **佇列拓撲**：每個 region 各自擁有一個出站佇列（雲端 PubSub、地端 Kafka），對岸 SyncWorker 跨區消費。
3. **預設 QueueType**：雲端 = PubSub、地端 = Kafka（同時演示「不同訊息佇列方案」）。
