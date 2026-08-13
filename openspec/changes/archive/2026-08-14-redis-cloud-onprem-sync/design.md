# Design: 雲地 Redis 同步（Camellia）

## Context

本專案是 greenfield Java + Spring Boot 專案，以 NetEase Camellia Redis proxy（`camellia-redis-proxy-spring-boot-starter:1.4.2`）為同步引擎，實作「單一程式、多角色」的雲地雙向 Redis session 同步。Camellia 本身已提供 MQ 雙寫的 producer/consumer 擴充點與路由（ResourceTable），本設計聚焦於「如何正確掛接這些擴充點」與「如何補足 Camellia 缺少的 MQ 可觀測性」。

Camellia 的關鍵事實（已驗證 master 與 1.4.2 一致）：
- 官方部署即「Spring Boot embed」：`@EnableCamelliaRedisProxyServer` 啟動內嵌 Netty Redis server，無「embed vs standalone」之分。
- 所有擴充點皆經 `ProxyBeanFactory.getBean(Class)` 解析：可注入 Spring `@Bean`（`applicationContext.getBean` 優先），也可 reflection 建立。
- producer 只需實作 `MqPackSender`（單一方法 `boolean send(MqPack pack)`），consumer 是 `ProxyPlugin`（`init()` 起執行緒）。
- Camellia **只附 Kafka 實作**（producer 與 consumer），PubSub / RedisPubSub 需自行實作；且 **MQ 無任何 lag/backlog 指標**。

## Goals / Non-Goals

**Goals**
- 單一 Spring Boot 可執行檔，依 `app.role` 切換 `RedisGateway` / `SyncWorker` / `Console` / `All`。
- 以 Camellia 為實際依賴，僅實作其擴充點（`MqPackSender` ×3、consumer plugin ×3、`RouteConfProvider`），不重寫代理核心。
- 三種 QueueType（GcpPubSub / RedisPubSub / Kafka）可插拔切換。
- 三種 RedisMode（Single / Sentinel / ReadWrite）以 Camellia ResourceTable 路由。
- 補足 MQ 積壓/消費指標，供 demo Web UI 檢查。
- docker-compose 以 `cloud-` / `onprem-` 前綴一鍵起雲地兩端。

**Non-Goals**
- 不做衝突調解（接受 LWW，雙向寫入衝突忽略）。
- 不做切換雙寫窗口的 fencing token（忽略）。
- 不重寫 Camellia 代理核心（連線管理、RESP 解析、指令轉送、熱載）。
- 不做真實 GCP Memorystore 連線（docker 以兩 IP 模擬，真實連線以 env 切換）。
- 不做生產級高可用/多租戶管理（demo 單一 bid/bgroup）。

## Decisions

### D1. 單一可執行檔、角色以 `app.role` 切換
一個 Spring Boot jar，用 `app.role`（`RedisGateway` | `SyncWorker` | `Console` | `All`）決定掛載哪些 bean/plugin。role 對 bean 的選擇以 `@ConditionalOnProperty(name="app.role", havingValue=...)` 實現；role 對 Camellia `proxy.plugin.list` 的選擇以 Spring profile 或 dynamic conf 覆寫實現。
- **Alternative**：多個獨立程式（gateway.jar / worker.jar / console.jar）。**Rejected**：違反「相同程式、不同運作方式」的核心需求。

### D2. 內嵌 Camellia：spring-boot-starter + `@EnableCamelliaRedisProxyServer`
使用 `camellia-redis-proxy-spring-boot-starter:1.4.2`，主類 `@SpringBootApplication` + `@EnableCamelliaRedisProxyServer`。Camellia 以內嵌 Netty server 直接開 Redis port。此即官方 standalone proxy 的啟動方式，無額外整合成本。
- **Alternative**：fork Camellia source 或自寫 RESP proxy。**Rejected**：違反「整合方式 = A（跑 stock proxy + 自訂 plugin）」。

### D3. Port 配置（避免 Tomcat / Netty 衝突）
Camellia proxy 讀 `@Value("${server.port:6379}")` 當 Redis port。若同 JVM 加 spring-web，Tomcat 會與 Netty 搶 port。故明確分離：
- `camellia-redis-proxy.config.port` = Redis port（如 `6380`，dynamic conf key `port` 優先）。
- `server.port` = Spring Web 管理/Console REST 埠（如 `8080`/`8081`）。
- `camellia-redis-proxy.config.console.port` = Camellia console server 埠（如 `16379`）。

### D4. 擴充點掛接對照（核心整合縫）
Camellia 的四個擴充點皆經 `ProxyBeanFactory.getBean(Class)`，本專案以 Spring `@Bean` 提供自訂實作，並把對應 class name 寫入 dynamic conf：

| 需求 | Camellia 擴充點 | 本專案實作 | 掛接 key |
|---|---|---|---|
| 出站佇列發送 | `MqPackSender` 介面 | `GcpPubSubMqPackSender` / `RedisPubSubMqPackSender` / `KafkaMqPackSender`（後者 Camellia 已附） | `mq.multi.write.sender.class.name` |
| 入站佇列消費（SyncWorker） | `ProxyPlugin`（consumer 型） | `GcpPubSubConsumerProxyPlugin` / `RedisPubSubConsumerProxyPlugin` / `KafkaMqPackConsumerProxyPlugin`（後者 Camellia 已附） | `proxy.plugin.list` |
| Redis 拓樸路由 | `RouteConfProvider` 抽象類 | `RedisModeRouteConfProvider`（依 RedisMode 產 route.conf） | `route.conf.provider` |
| 檢查/指標 | `ConsoleService`（覆寫 `custom()`）＋ `@RestController` | `SyncConsoleService` + `ConsoleApiController` | `@Bean ConsoleService` |

**Producer 端**：Camellia 的 `MqMultiWriteProducerProxyPlugin` 會讀 `mq.multi.write.sender.class.name`，只轉送 WRITE + FULL_SUPPORT + 非阻塞指令；`SETEX`/`EXPIRE` 是 WRITE 指令，故被以原始 RESP 轉送，重放時自然保留 TTL 語意（滿足「EXPIRE/SETEX 轉送」需求，無需自訂邏輯）。

**Consumer 端**：Camellia 僅附 `KafkaMqPackConsumerProxyPlugin`。為對稱與統一指標，本專案以相同 pattern 自建 `GcpPubSubConsumerProxyPlugin` 與 `RedisPubSubConsumerProxyPlugin`（`init()` 起消費執行緒 → poll → `MqPackSerializer` 反序列化 → 原始指令重放至本地 Redis）。

### D5. 佇列拓樸：每 region 一個出站佇列、跨區消費
- 雲端 `cloud-app`：Gateway 寫 `cloud-redis` 並發至 `cloud-pubsub`（雲端出站）；SyncWorker 消費 `onprem-kafka`（地端出站）重放至 `cloud-redis`。
- 地端 `onprem-app`：Gateway 寫 `onprem-redis` 並發至 `onprem-kafka`（地端出站）；SyncWorker 消費 `cloud-pubsub` 重放至 `onprem-redis`。
- 每 region 出站佇列類型不同（雲=PubSub、地=Kafka），恰好演示「不同訊息佇列方案」；Camellia producer + consumer 可共存於同一 JVM，故每 region 一個 instance 跑 `All`。
- 訊息 payload = `MqPack`（`Command` raw RESP + `bid` + `bgroup` + `ext`），語言中立、消費端原樣重放。demo 使用單一 bid/bgroup（預設值），不啟用多租戶。

### D6. 自訂 `MqPackSender` 三種實作（補 Camellia 缺口）
Camellia 只附 Kafka。`GcpPubSubMqPackSender` 與 `RedisPubSubMqPackSender`（`PUBLISH` 至本地 Redis 的 `__camellia-sync` channel）為 net-new，皆實作單一介面 `boolean send(MqPack pack) throws Exception`。
- **RedisPubSub = 純 Pub/Sub 語意（已決議）**：不持久化、無訊息 TTL（無 consumer 即丟）。「佇列 TTL 可調」僅套用於 PubSub（subscription retention）與 Kafka（broker retention），RedisPubSub 不適用。此定性即需求 2 的「短期保存特性」。
- **GcpPubSub = emulator 優先 + client 抽象化（已決議）**：先對準本地 docker 的 PubSub emulator（`cloud-pubsub`），實作時抽 `PubSubClient` 介面（send/pull/backlog），讓真實 GCP 以 env 切換 client 實作（emulator REST vs 正式 gRPC），程式不改。

### D7. 自訂 `RouteConfProvider` 支援三種 RedisMode
`RedisModeRouteConfProvider extends RouteConfProvider`，依 `RedisMode` 產出 Camellia route.conf 字串，經 `invokeUpdateResourceTable()` 推播（不手建 ResourceTable）：
- `Single`：單一 `redis://host:port`。
- `Sentinel`：`redis-sentinel://sentinel1,sentinel2,sentinel3@masterName`。
- `ReadWrite`：讀寫分離雙 IP（`{type: simple, operation: {read: ...readIp, write: ...writeIp}}` 或等價 read/write 分流 route.conf）。
Pin Camellia ≥1.4.0（`RouteConfProvider`/`ServerConf` 為 2026/2 重構後 API，舊 `ProxyRouteConfUpdater` 已過時）。

### D8. 補足 MQ 可觀測性（Camellia 缺口）
Camellia monitor 套件完全無 MQ 指標；`KafkaMqPackSender` 的內部佇列為 private 無 getter。本專案自建：
- sender：暴露 `queue.size()` / `remainingCapacity()` 為 Micrometer gauge。
- consumer：計數 consumed / replay-success / replay-fail。
- broker lag：Kafka 用 `AdminClient`（`listConsumerGroupOffsets` vs `endOffsets`）；PubSub 用訂閱 backlog；RedisPubSub 用輔助 list 長度。
- 輸出：經自訂 `ConsoleService`（`extends ConsoleServiceAdaptor` 覆寫 `custom()`）或 `@RestController`，供 Web UI 讀取。`monitor.enable` 設 `true`（預設 false）。

### D9. Console REST API + Node 前端
Spring Boot 僅出 REST API；前端為獨立 Node 專案（保留 `node_modules`）。API（cloud-app 與 onprem-app 皆提供）：
- `POST /api/session`（寫入測試 session，寫入目前主端）
- `GET  /api/compare?key=`（比對 key 於兩端之值）
- `POST /api/switch`（大小網切換，含防抖：最短切換間隔 10 秒，過於頻繁則拒絕）
- `GET/POST /api/params`（檢視/調整 QueueType 與 TTL）
- `GET  /api/latency`（同步延遲）
- `GET  /api/queue-status`（積壓/已消費）
- `GET  /api/health`（各元件 role/location 與存活）

Web UI（Node）同時連 `cloud-app` 與 `onprem-app` 的 REST API，以呈現「兩端一致性比對」與「元件健康」跨端視圖。

### D10. 專案結構（Maven 多模組 + Node 前端）
```
pom.xml                          # parent aggregator（Spring Boot 3.x, Java 21）
├── sync-common/                 # MqPack 模型、QueueType/RedisMode enum、共用設定模型、指標介面
├── sync-proxy/                  # Camellia embed：producer/consumer plugin、3×MqPackSender、RouteConfProvider、ConsoleService
├── sync-app/                    # Spring Boot 主程式：@EnableCamelliaRedisProxyServer、role 切換、REST controllers
└── web-ui/                      # Node 前端（demo 操作 + 檢查）
```
- **Alternative**：單一模組塞全部。**Rejected**：Camellia starter 與 REST/前端需乾淨分離，且 `sync-common` 的 `MqPack` 模型要獨立以利未來測試。

### D11. docker-compose 拓樸（`cloud-` / `onprem-` 前綴）
- 雲端側：`cloud-redis-write`、`cloud-redis-read`（Memorystore 讀寫分離替身）、`cloud-pubsub`（emulator）、`cloud-app`（8080）。
- 地端側：`onprem-redis-master`、`onprem-redis-replica`、`onprem-redis-sentinel`×3、`onprem-kafka`（或 Redpanda）、`onprem-app`（8081）。
- 共用：`web-ui`（Node，連兩端 REST）。
- 真實 GCP Memorystore / PubSub 以環境變數切換（預設本地模擬）。

## Risks / Trade-offs

| 風險 | 影響 | 緩解 |
|---|---|---|
| Camellia `RouteConfProvider`/`ServerConf` 為新 API（≥1.4.0），舊文獻誤導 | 整合走錯 API | 已 pin 1.4.2 並以 master 驗證；實作以 1.4.2 source 為準 |
| PubSub / RedisPubSub 需 net-new consumer + sender，無官方 reference | 自建實作易有 bug | 以 `KafkaMqPackConsumerProxyPlugin` / `KafkaMqPackSender` 為 pattern 對稱實作；payload 共用 `MqPackSerializer` |
| RedisPubSub 無持久化、訊息易丟 | 同步不可靠（定性短期保存） | 需求已接受「短期保存特性」；TTL 可調；Kafka 為可靠替代 |
| 雙向寫入衝突無調解 | LWW 收斂可能非預期值 | 需求已接受 LWW；UI 一致性比對可視化結果 |
| Tomcat 與 Netty port 衝突 | 啟動失敗 | D3 明確分離三 port |
| MQ 積壓指標缺失 | UI 無法顯示積壓 | D8 自建 gauge + AdminClient |

## Migration Plan

Greenfield，無資料遷移。部署階段：
1. 本地 docker-compose 起雲地兩端（模擬替身）→ 驗證雙向同步。
2. 以 env 切換真實 GCP Memorystore / PubSub（唯讀連線資訊變更，程式不變）。
3. 大小網切換演練：寫入測試 session → 切換 → 一致性比對 → 同步延遲檢視。

## Open Questions

（無 — 全部已於 propose 階段決議：RedisPubSub 採純 Pub/Sub 語意；切換防抖 10 秒；GcpPubSub emulator 優先 + client 抽象化。）
