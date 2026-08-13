package com.example.sync.app.service;

import com.example.sync.common.config.AppProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Service;
import redis.clients.jedis.JedisPooled;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Demo session store. Writes are wrapped in a JSON envelope
 * {@code {"value":..., "ts":..., "origin":...}} so the compare endpoint can report the
 * originating region and end-to-end lag without extra bookkeeping.
 */
@Service
public class SyncService {

    private final JedisPooled jedis;
    private final AppProperties appProperties;
    private final ObjectMapper mapper = new ObjectMapper();

    public SyncService(JedisPooled jedis, AppProperties appProperties) {
        this.jedis = jedis;
        this.appProperties = appProperties;
    }

    /** Writes a session key with an optional TTL (SETEX when ttl &gt; 0, SET otherwise). */
    public Map<String, Object> writeSession(String key, String value, Long ttlSeconds) {
        long ts = System.currentTimeMillis();
        ObjectNode envelope = mapper.createObjectNode();
        envelope.put("value", value);
        envelope.put("ts", ts);
        envelope.put("origin", appProperties.getLocation().name());

        String stored;
        try {
            stored = mapper.writeValueAsString(envelope);
        } catch (Exception e) {
            throw new IllegalStateException("failed to serialize envelope", e);
        }

        long ttl = (ttlSeconds != null && ttlSeconds > 0) ? ttlSeconds : -1;
        if (ttl > 0) {
            jedis.setex(key, ttl, stored);
        } else {
            jedis.set(key, stored);
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("key", key);
        result.put("value", value);
        result.put("origin", appProperties.getLocation().name());
        result.put("location", appProperties.getLocation().name());
        result.put("writtenAt", ts);
        result.put("ttlSeconds", ttl > 0 ? ttl : null);
        return result;
    }

    /** Reads a session key and reports value, origin, local location, and lag. */
    public Map<String, Object> readKey(String key) {
        String stored = jedis.get(key);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("key", key);
        result.put("location", appProperties.getLocation().name());
        if (stored == null) {
            result.put("exists", false);
            return result;
        }
        try {
            JsonNode node = mapper.readTree(stored);
            result.put("exists", true);
            result.put("value", node.path("value").asText());
            result.put("origin", node.path("origin").asText(null));
            long ts = node.path("ts").asLong(System.currentTimeMillis());
            result.put("writtenAt", ts);
            result.put("lagMs", System.currentTimeMillis() - ts);
            result.put("ttlSeconds", jedis.ttl(key));
        } catch (Exception e) {
            // not an envelope — treat as raw value
            result.put("exists", true);
            result.put("value", stored);
            result.put("origin", "unknown");
            result.put("writtenAt", null);
            result.put("lagMs", null);
        }
        return result;
    }
}
