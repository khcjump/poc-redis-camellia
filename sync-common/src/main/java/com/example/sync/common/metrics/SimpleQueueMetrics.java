package com.example.sync.common.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * 記憶體實作：以 {@link AtomicLong} 累計，並（可選）註冊 Micrometer gauge。
 */
public class SimpleQueueMetrics implements QueueMetrics {

    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong sendFail = new AtomicLong();
    private final AtomicLong consumed = new AtomicLong();
    private final AtomicLong replaySuccess = new AtomicLong();
    private final AtomicLong replayFail = new AtomicLong();

    /** sender 在途量供應者（由 sender 掛上，無則回 0） */
    private final LongSupplier inFlightSupplier;
    /** broker 積壓供應者（由 lag poller 掛上，無則回 -1） */
    private final LongSupplier backlogSupplier;

    public SimpleQueueMetrics() {
        this(() -> 0, () -> -1);
    }

    public SimpleQueueMetrics(LongSupplier inFlightSupplier, LongSupplier backlogSupplier) {
        this.inFlightSupplier = inFlightSupplier;
        this.backlogSupplier = backlogSupplier;
    }

    /** 註冊 Micrometer gauge（MeterRegistry 存在時）。 */
    public void bindTo(MeterRegistry registry) {
        Gauge.builder("sync.mq.sent", sent, AtomicLong::doubleValue).register(registry);
        Gauge.builder("sync.mq.send.fail", sendFail, AtomicLong::doubleValue).register(registry);
        Gauge.builder("sync.mq.consumed", consumed, AtomicLong::doubleValue).register(registry);
        Gauge.builder("sync.mq.replay.success", replaySuccess, AtomicLong::doubleValue).register(registry);
        Gauge.builder("sync.mq.replay.fail", replayFail, AtomicLong::doubleValue).register(registry);
        Gauge.builder("sync.mq.inflight", this, m -> m.inFlight()).register(registry);
        Gauge.builder("sync.mq.backlog", this, m -> m.backlog()).register(registry);
    }

    @Override
    public void recordSent() {
        sent.incrementAndGet();
    }

    @Override
    public void recordSendFail() {
        sendFail.incrementAndGet();
    }

    @Override
    public void recordConsumed() {
        consumed.incrementAndGet();
    }

    @Override
    public void recordReplaySuccess() {
        replaySuccess.incrementAndGet();
    }

    @Override
    public void recordReplayFail() {
        replayFail.incrementAndGet();
    }

    @Override
    public long sentCount() {
        return sent.get();
    }

    @Override
    public long sendFailCount() {
        return sendFail.get();
    }

    @Override
    public long consumedCount() {
        return consumed.get();
    }

    @Override
    public long replaySuccessCount() {
        return replaySuccess.get();
    }

    @Override
    public long replayFailCount() {
        return replayFail.get();
    }

    @Override
    public long inFlight() {
        return inFlightSupplier != null ? inFlightSupplier.getAsLong() : 0;
    }

    @Override
    public long backlog() {
        return backlogSupplier != null ? backlogSupplier.getAsLong() : -1;
    }

    @Override
    public Map<String, Object> snapshot() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("sent", sent.get());
        m.put("sendFail", sendFail.get());
        m.put("consumed", consumed.get());
        m.put("replaySuccess", replaySuccess.get());
        m.put("replayFail", replayFail.get());
        m.put("inFlight", inFlight());
        m.put("backlog", backlog());
        return m;
    }
}
