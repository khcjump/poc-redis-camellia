# camellia-sync — 雲地 Redis 資料同步 demo

以 [NetEase Camellia](https://github.com/netease-im/camellia)（Redis proxy）為核心的雲端 ⇄ 地端
（Cloud ⇄ OnPrem）Redis 資料同步示範專案。單一 Spring Boot 程式，透過啟動參數切換不同 **role**、
不同 **message queue** 方案、不同 **位置定位**，並附 Node.js Web UI 做「操作 + 檢查」的 demo 介面。

## 架構

```
        ┌───────────────────────────── Cloud ─────────────────────────────┐
        │  cloud-app (role=All)                                           │
        │   ├─ RedisGateway  → 主寫 cloud-redis  + 發佈 cloud-pubsub        │
        │   ├─ SyncWorker    → 消費 onprem-kafka → 重放至 cloud-redis        │
        │   └─ Console       → REST API (:8080)                           │
        │  後端: cloud-redis-write / cloud-redis-read (ReadWrite 讀寫分離)    │
        └──────────────────────────────┬──────────────────────────────────┘
                                        │  (跨區: GcpPubSub ⇄ Kafka)
        ┌──────────────────────────────┴──────────────────────────────────┐
        │  onprem-app (role=All)                                          │
        │   ├─ RedisGateway  → 主寫 onprem-redis + 發佈 onprem-kafka        │
        │   ├─ SyncWorker    → 消費 cloud-pubsub → 重放至 onprem-redis       │
        │   └─ Console       → REST API (:8081)                           │
        │  後端: onprem-redis master/replica + 3×sentinel (Sentinel 模式)    │
        └─────────────────────────────────────────────────────────────────┘
                                        │
                                 web-ui (Node) :3000
```

- **雙活 (active-active)**：兩區各自主寫本地 Redis，並把變更發佈到本地出站佇列；對端的
  SyncWorker 消費後重放，達成雙向同步。
- **每 region 一個出站佇列、跨區消費**：Cloud 出站用 **GcpPubSub**、OnPrem 出站用 **Kafka**，
  同時示範兩種 message queue 方案。
- **單一 jar 多 role**：`APP_ROLE` 切換 `RedisGateway | SyncWorker | Console | All`。
- **Camellia 擴充點掛接**（不重寫核心）：
  - `MqPackSender`（出站發送）×3 → `GcpPubSubMqPackSender` / `RedisPubSubMqPackSender` / `KafkaMqPackSender`
  - `ProxyPlugin`（入站消費）×3 → `GcpPubSubConsumerProxyPlugin` / `RedisPubSubConsumerProxyPlugin` / `KafkaConsumerProxyPlugin`
  - `RouteConfProvider` → `RedisModeRouteConfProvider`（Single / Sentinel / ReadWrite）
  - `ConsoleService` → `SyncConsoleService`（自訂 `/custom` 輸出 MQ 指標）

## 模組結構

```
camellia-sync/
├── sync-common/   共用模型(enum) + 設定(AppProperties/SyncProperties) + 指標(QueueMetrics)
├── sync-proxy/    Camellia 擴充點實作：3×sender、3×consumer plugin、RouteConfProvider、ConsoleService
├── sync-app/      Spring Boot 主程式 + REST API + role/queue 注入(SyncEnvironmentPostProcessor)
├── web-ui/        Node.js (Vite + React + Express) 前端，反代 /cloud、/onprem
├── Dockerfile     multi-stage：Maven 21 建置 → temurin 21-jre
└── docker-compose.yml
```

## 快速啟動

```bash
docker compose up -d --build
```

| 服務 | 位址 | 說明 |
|---|---|---|
| web-ui | http://localhost:3000 | 操作 + 檢查介面 |
| cloud-app | http://localhost:8080 | Cloud region REST API（Redis proxy :6380、console :16379） |
| onprem-app | http://localhost:8086 | OnPrem region REST API（容器內 8081，宿主映射 8086） |
| onprem-kafka | localhost:9092 | 地端出站佇列（KRaft） |
| cloud-pubsub | localhost:8085 | 雲端出站佇列（GCP Pub/Sub emulator） |
| cloud-redis-write/read | localhost:6401 / 6402 | 雲端 Memorystore 模擬（讀寫分離） |
| onprem-redis-master/replica | localhost:6403 / 6404 | 地端 master/replica（sentinel 監控 mymaster） |

> 埠注意：宿主若已有其他服務佔用 8080/8081 等，請改 `docker-compose.yml` 的宿主映射。
> 本 repo 已將 onprem-app 宿主映射設為 **8086**（避開既有 session 服務的 8081）。

## 組態參數（env）

| 環境變數 | 預設 | 說明 |
|---|---|---|
| `APP_ROLE` | `All` | `RedisGateway` / `SyncWorker` / `Console` / `All` |
| `APP_LOCATION` | `Cloud` | `Cloud` / `OnPrem`（寫入 envelope 的 origin） |
| `WEB_PORT` | `8080` | Web REST / Tomcat port |
| `SYNC_PRODUCER_QUEUE_TYPE` | `Kafka` | 出站佇列 `GcpPubSub` / `RedisPubSub` / `Kafka` |
| `SYNC_CONSUMER_QUEUE_TYPE` | `Kafka` | 入站佇列（同上三選一） |
| `REDIS_MODE` | `Single` | `Single` / `Sentinel` / `ReadWrite` |
| `REDIS_PROXY_PORT` | `6380` | Camellia Redis proxy port（`camellia-redis-proxy.config.port`） |
| `CONSOLE_PORT` | `16379` | Camellia console port |
| `QUEUE_TTL_SECONDS` | `30` | 佇列訊息 TTL（短期保存特性，可調參數） |
| `SWITCH_MIN_INTERVAL_SECONDS` | `10` | 大小網切換防抖間隔 |
| `REDIS_SINGLE_HOST`/`PORT` | `127.0.0.1`/`6379` | Single 模式後端 |
| `REDIS_SENTINEL_NODES`/`MASTER` | — | Sentinel 模式（`host:port,...`） |
| `REDIS_READ_HOST`/`WRITE_HOST` | — | ReadWrite 模式（2 IP 讀寫分離） |
| `PUBSUB_HOST`/`PROJECT_ID`/`TOPIC`/`SUBSCRIPTION` | — | GCP Pub/Sub emulator 端點 |
| `KAFKA_BOOTSTRAP_SERVERS`/`TOPIC`/`GROUP_ID` | — | Kafka 端點 |

## REST API

兩區同一組端點（Web UI 聚合兩端）：

| 方法 | 路徑 | 說明 |
|---|---|---|
| `POST` | `/api/session` | 寫入測試 session `{key, value, ttlSeconds?}`（TTL 走 SETEX） |
| `GET` | `/api/compare?key=` | 兩端一致性比對 + 延遲（回 value/origin/writtenAt/lagMs） |
| `GET` | `/api/switch` | 查詢目前 primary |
| `POST` | `/api/switch?to=Cloud\|OnPrem` | 大小網切換（10 秒防抖，過頻 429） |
| `GET`/`POST` | `/api/params` | 查詢 / 調整執行參數（queueTtlSeconds、minIntervalSeconds） |
| `GET` | `/api/queue-status` | 佇列積壓與消費狀態（sent/consumed/replaySuccess/replayFail/inFlight） |
| `GET` | `/api/health` | 元件健康（role/location/queue types/redis mode） |

## 驗證結果

`docker compose up` 後六項端到端驗證全數通過：

1. **cloud→onprem**（GcpPubSub 出站）：cloud 寫入 → onprem `compare` 見 `origin=Cloud`。
2. **onprem→cloud**（Kafka 出站）：onprem 寫入 → cloud `compare` 見 `origin=OnPrem`。
3. **EXPIRE/SETEX 轉送**：TTL=5 的 key 兩端同步倒數、同步過期。
4. **大小網切換防抖**：10 秒內二次切換回 429。
5. **佇列指標**：兩端 `replayFail=0`（零重放失敗）。
6. **web-ui**：:3000 HTTP 200。

## 技術棧

- **Java 21 + Spring Boot 3.5.9**（Maven 多模組）
- **Camellia 1.4.2**（`camellia-redis-proxy-spring-boot-starter` + `mq-common` + `mq-kafka`，內嵌 proxy）
- **Netty 4.2.10**（Camellia proxy 依賴，override Spring Boot 的 4.1）
- **Jedis 5.2.0**（REST 寫入經 proxy 觸發同步）
- **Kafka 3.9.0**（`apache/kafka` KRaft 單節點）
- **GCP Pub/Sub emulator**（`google/cloud-sdk:emulators`，走 REST API 非 gRPC）
- **Node.js 22 + Vite + React + Express**（web-ui，同源反代避免 CORS）

## 備註

- Camellia 啟動時會 **eager preheat 上游 Redis 連線**，故 `docker compose` 中 app 依賴 Redis
  先行（`depends_on`），Redis 未就緒時 app 啟動會失敗。
- Redis 7.4 Sentinel 預設不解析 hostname（`resolve_hostnames=0` → `AI_NUMERICHOST`），故
  sentinel config 已加 `sentinel resolve-hostnames yes`。
- 雙向寫入衝突採 **LWW（last-write-wins）**，無衝突調解（按需求「忽略不一致」）。
