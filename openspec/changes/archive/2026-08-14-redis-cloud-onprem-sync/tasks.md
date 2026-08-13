# Tasks: 雲地 Redis 同步（Camellia）

## 1. 專案骨架（Maven 多模組 + Node 前端）

- [x] 1.1 建立 Maven parent `pom.xml`：Spring Boot 3.x、Java 21、`camellia-redis-proxy-spring-boot-starter:1.4.2` dependency management、模組聚合
- [x] 1.2 建立 `sync-common` 模組：`MqPack` 模型（command/bid/bgroup/ext）、`QueueType` 與 `RedisMode` enum、共用設定模型、佇列指標介面
- [x] 1.3 建立 `sync-proxy` 模組：依賴 camellia starter + `sync-common`，空殼可編譯
- [x] 1.4 建立 `sync-app` 模組：`@SpringBootApplication` + `@EnableCamelliaRedisProxyServer` 主類、`app.role` 參數與 `@ConditionalOnProperty` 骨架
- [x] 1.5 建立 `web-ui` Node 專案：`package.json`、dev server、連線 cloud-app/onprem-app 的設定

## 2. Redis 拓樸路由（RouteConfProvider）

- [x] 2.1 實作 `RedisModeRouteConfProvider extends RouteConfProvider`，依 `RedisMode` 產出 route.conf 字串
- [x] 2.2 `Single` 模式：單一 `redis://host:port` route.conf，經 `invokeUpdateResourceTable()` 推播
- [x] 2.3 `Sentinel` 模式：`redis-sentinel://s1,s2,s3@masterName` route.conf
- [x] 2.4 `ReadWrite` 模式：讀寫分離雙 IP（read IP + write IP）route.conf
- [x] 2.5 掛接 `route.conf.provider` 至 `RedisModeRouteConfProvider`（`@Bean` + class name）

## 3. 出站佇列（MqPackSender 三實作）

- [x] 3.1 實作 `GcpPubSubMqPackSender implements MqPackSender`：抽 `PubSubClient` 介面（send），先對準 emulator
- [x] 3.2 實作 `RedisPubSubMqPackSender implements MqPackSender`：`PUBLISH` 至 `__camellia-sync` channel
- [x] 3.3 驗證 `KafkaMqPackSender`（Camellia 已附）可經 `mq.multi.write.sender.class.name` 掛接
- [x] 3.4 依 `QueueType` 選擇 sender：`@ConditionalOnProperty` 產出對應 `@Bean`，寫入 `mq.multi.write.sender.class.name`

## 4. 入站佇列（consumer plugin）

- [x] 4.1 實作 `GcpPubSubConsumerProxyPlugin implements ProxyPlugin`：`init()` 起消費執行緒 → poll → `MqPackSerializer` 反序列化 → 重放至本地 Redis
- [x] 4.2 實作 `RedisPubSubConsumerProxyPlugin`：訂閱 `__camellia-sync` channel 重放
- [x] 4.3 驗證 `KafkaMqPackConsumerProxyPlugin`（Camellia 已附）掛接
- [x] 4.4 依 `app.role` 將 consumer plugin 寫入 `proxy.plugin.list`（SyncWorker / All）

## 5. 角色切換與整合

- [x] 5.1 `RedisGateway` role：掛 producer plugin + 對應 sender（主寫本地 + 發本地出站佇列）
- [x] 5.2 `SyncWorker` role：掛 consumer plugin（消費對岸出站佇列 + 重放本地）
- [x] 5.3 `All` role：producer + consumer 共存於同一 JVM
- [x] 5.4 Port 分離：`camellia-redis-proxy.config.port`（Redis）、`server.port`（Web）、`console.port`（16379）
- [x] 5.5 驗證 EXPIRE/SETEX 以原始 RESP 轉送、重放保留 TTL 語意

## 6. 可觀測性（補足 Camellia MQ 指標缺口）

- [x] 6.1 sender 暴露 `queue.size()`/`remainingCapacity()` 為 Micrometer gauge
- [x] 6.2 consumer 計數 consumed / replay-success / replay-fail
- [x] 6.3 broker lag：Kafka `AdminClient`（consumer group offsets vs endOffsets）、PubSub 訂閱 backlog
- [x] 6.4 實作 `SyncConsoleService extends ConsoleServiceAdaptor` 覆寫 `custom()` 輸出 MQ 指標
- [x] 6.5 設定 `monitor.enable=true` 與 `monitor.interval.seconds`

## 7. Console REST API

- [x] 7.1 `POST /api/session`：寫入測試 session 至目前主端
- [x] 7.2 `GET /api/compare?key=`：比對 key 於兩端之值
- [x] 7.3 `POST /api/switch`：大小網切換，含 10 秒防抖（過於頻繁拒絕）
- [x] 7.4 `GET/POST /api/params`：檢視/調整 QueueType 與 TTL
- [x] 7.5 `GET /api/latency`：同步延遲（寫入時間戳 vs 對岸出現時間戳）
- [x] 7.6 `GET /api/queue-status`：佇列積壓與已消費
- [x] 7.7 `GET /api/health`：各元件 role/location 與存活

## 8. Web UI（Node 前端）

- [x] 8.1 操作頁：寫入測試 session、切換大小網、調整 QueueType/TTL
- [x] 8.2 檢查頁：兩端一致性比對、同步延遲、佇列積壓、元件健康
- [x] 8.3 連線 cloud-app 與 onprem-app 的 REST API（跨端視圖）

## 9. 部署（docker-compose）

- [x] 9.1 `cloud-` 側：`cloud-redis-write`、`cloud-redis-read`（Memorystore 讀寫分離替身）
- [x] 9.2 `cloud-pubsub`：PubSub emulator
- [x] 9.3 `cloud-app`：Dockerfile + 環境變數（8080、QueueType=PubSub、RedisMode=ReadWrite）
- [x] 9.4 `onprem-` 側：`onprem-redis-master`、`onprem-redis-replica`、`onprem-redis-sentinel`×3
- [x] 9.5 `onprem-kafka`：Kafka（或 Redpanda）
- [x] 9.6 `onprem-app`：Dockerfile + 環境變數（8081、QueueType=Kafka、RedisMode=Sentinel）
- [x] 9.7 `web-ui` 服務 + 連線兩端
- [x] 9.8 env 切換：真實 GCP Memorystore / PubSub 連線資訊以環境變數覆寫（預設本地模擬）

## 10. 端到端驗證

- [x] 10.1 本地 `docker compose up` 起雲地兩端，驗證雙向同步（雲寫→地現、地寫→雲現）
- [x] 10.2 驗證 EXPIRE/SETEX 轉送與 TTL 語意保留
- [x] 10.3 驗證大小網切換 + 兩端一致性比對 + 同步延遲
- [x] 10.4 驗證 QueueType 切換（PubSub ↔ Kafka）同步行為不變
- [x] 10.5 驗證佇列積壓/消費/健康指標於 UI 顯示
