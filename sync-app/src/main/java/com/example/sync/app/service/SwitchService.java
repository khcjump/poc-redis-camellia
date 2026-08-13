package com.example.sync.app.service;

import com.example.sync.common.config.AppProperties;
import com.example.sync.common.config.SyncProperties;
import com.example.sync.common.model.Location;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Traffic switch ("大小網切換") between the Cloud and OnPrem regions. Enforces a minimum
 * interval between flips to guarantee a smooth switch (no rapid oscillation).
 */
@Service
public class SwitchService {

    private final AppProperties appProperties;
    private final SyncProperties syncProperties;
    private final AtomicLong lastSwitchAt = new AtomicLong(0);
    private volatile Location primary;

    public SwitchService(AppProperties appProperties, SyncProperties syncProperties) {
        this.appProperties = appProperties;
        this.syncProperties = syncProperties;
        this.primary = appProperties.getLocation();
    }

    public Map<String, Object> current() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("primary", primary.name());
        result.put("location", appProperties.getLocation().name());
        result.put("lastSwitchAt", lastSwitchAt.get() == 0 ? null : lastSwitchAt.get());
        result.put("minIntervalSeconds", syncProperties.getSwtch().getMinIntervalSeconds());
        return result;
    }

    public Map<String, Object> switchTo(Location target) {
        long minIntervalMs = syncProperties.getSwtch().getMinIntervalSeconds() * 1000L;
        long now = System.currentTimeMillis();
        long last = lastSwitchAt.get();
        if (last != 0 && now - last < minIntervalMs) {
            throw new TooFrequentSwitchException(
                    "switch too frequent: last=" + last + ", minIntervalMs=" + minIntervalMs);
        }
        if (!lastSwitchAt.compareAndSet(last, now)) {
            throw new TooFrequentSwitchException("concurrent switch detected");
        }
        this.primary = target;
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("primary", primary.name());
        result.put("location", appProperties.getLocation().name());
        result.put("switchedAt", now);
        return result;
    }

    public static class TooFrequentSwitchException extends RuntimeException {
        public TooFrequentSwitchException(String message) {
            super(message);
        }
    }
}
