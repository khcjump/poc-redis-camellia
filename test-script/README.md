# test-script — 資料同步流程測試

對 camellia-sync 的兩區資料同步流程做端到端測試（REST API 驅動，不需額外工具）。

## 執行

```bash
cd test-script
./run-all.sh          # 依序跑全部測試並彙總
# 或單獨跑一個：
./test-02-cloud-to-onprem.sh
```

需要 stack 已啟動（`docker compose up -d --build`），且 cloud-app / onprem-app /
web-ui 皆為 UP。

## 測試清單

| 腳本 | 驗證項目 | 方向 |
|---|---|---|
| `test-01-health.sh` | 兩端 `/api/health` UP + 正確的 queue/redis 拓撲 | — |
| `test-02-cloud-to-onprem.sh` | Cloud 寫入 → OnPrem 讀到，`origin=Cloud` | GcpPubSub 出站 |
| `test-03-onprem-to-cloud.sh` | OnPrem 寫入 → Cloud 讀到，`origin=OnPrem` | Kafka 出站 |
| `test-04-ttl-expire.sh` | TTL 轉送：TTL=8 的 key 同步後兩端倒數、到期消失 | EXPIRE/SETEX |
| `test-05-switch-debounce.sh` | 大小網切換：首次 200，10 秒內第二次 → 429 | 防抖 |
| `test-06-queue-metrics.sh` | 兩端 `replayFail=0`、`sendFail=0` | 佇列健康 |
| `test-07-webui.sh` | web-ui `:3000` HTTP 200 | 前端 |

## 設定（環境變數）

| 變數 | 預設 | 說明 |
|---|---|---|
| `CLOUD_URL` | `http://localhost:8080` | cloud-app REST API |
| `ONPREM_URL` | `http://localhost:8086` | onprem-app REST API |
| `WEBUI_URL` | `http://localhost:3000` | web-ui |

## 行為說明

- 每個測試獨立的 `key`（含 timestamp），互不干擾、可重複執行。
- 同步等待上限 30 秒（GcpPubSub/Kafka 傳播 + 消費），TTL 測試等待到期上限 25 秒。
- `test-05` 會先讀取目前 debounce 窗口，若上次切換仍在 10 秒內則先休眠再切，
  確保第一次切換一定得到 200。
- 共用函式庫在 `common.sh`（`jq` 可用則用 `jq`，否則退回 `python3`）。
