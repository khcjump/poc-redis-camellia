package com.example.sync.common.metrics;

import java.util.Map;

/**
 * 佇列同步指標（補足 Camellia 缺 MQ 指標的缺口）。
 *
 * <p>單一 JVM 內共享一份實作，同時涵蓋出站（producer）與入站（consumer）兩方向的計數。</p>
 */
public interface QueueMetrics {

    /** producer 成功發送一則訊息 */
    void recordSent();

    /** producer 發送失敗 */
    void recordSendFail();

    /** consumer 收到一則訊息 */
    void recordConsumed();

    /** consumer 成功重放至本地 Redis */
    void recordReplaySuccess();

    /** consumer 重放失敗 */
    void recordReplayFail();

    long sentCount();

    long sendFailCount();

    long consumedCount();

    long replaySuccessCount();

    long replayFailCount();

    /** sender 內部佇列在途量（in-flight） */
    long inFlight();

    /** broker 積壓（best-effort lag；不支援回 -1） */
    long backlog();

    /** 供 JSON/Console 輸出的快照 */
    Map<String, Object> snapshot();
}
