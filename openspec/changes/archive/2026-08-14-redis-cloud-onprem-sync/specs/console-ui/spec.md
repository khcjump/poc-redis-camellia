## Purpose

提供 demo 用的 Web 操作與檢查介面，讓使用者可直接演示並驗證雲地同步與大小網切換行為。

## ADDED Requirements

### Requirement: 寫入測試 session
UI SHALL 允許使用者在目前主端寫入一筆測試 session，並觀察其同步至對岸。

#### Scenario: 寫入並觀察同步
- **WHEN** 使用者在 UI 寫入一筆測試 session
- **THEN** 該 session 寫入目前主端，並於短暫延遲後可在對岸查到

### Requirement: 切換大小網
UI SHALL 允許使用者切換進線（大小網）至雲端或地端，且切換必須平緩（不快速來回切換）。

#### Scenario: 切換進線
- **WHEN** 使用者在 UI 執行大小網切換
- **THEN** 進線流量切換至目標端，且系統不因快速反覆切換而異常

### Requirement: 調整參數
UI SHALL 允許使用者檢視並調整 QueueType 與 TTL 等參數以演示可插拔能力。

#### Scenario: 調整 QueueType
- **WHEN** 使用者在 UI 調整 QueueType 參數
- **THEN** 同步改經由新佇列後端運作

### Requirement: 兩端一致性比對
UI SHALL 顯示同一 key 在雲端與地端的值以供比對。

#### Scenario: 比對 key 值
- **WHEN** 使用者輸入一個 key 進行比對
- **THEN** UI 同時顯示該 key 在雲端與地端的值

### Requirement: 同步延遲檢視
UI SHALL 顯示同步延遲（寫入時間戳與對岸出現時間戳之差）。

#### Scenario: 顯示同步延遲
- **WHEN** 使用者寫入一筆 session
- **THEN** UI 顯示該筆寫入從來源出現於對岸的延遲

### Requirement: 佇列積壓與消費狀態
UI SHALL 顯示佇列積壓與消費狀態。

#### Scenario: 顯示積壓
- **WHEN** 使用者開啟檢查頁面
- **THEN** UI 顯示各佇列的積壓數量與已消費數量

### Requirement: 元件健康
UI SHALL 顯示 Gateway 與 SyncWorker 的存活狀態及其 role/location。

#### Scenario: 顯示元件健康
- **WHEN** 使用者開啟檢查頁面
- **THEN** UI 顯示各元件的存活狀態與其角色/位置
