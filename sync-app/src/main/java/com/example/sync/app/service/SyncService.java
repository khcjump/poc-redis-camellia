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

    /** Writes a Hash field with JSON envelope {"value":..., "ts":..., "origin":...}. */
    public Map<String, Object> writeHash(String key, String field, String value) {
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

        jedis.hset(key, field, stored);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("key", key);
        result.put("field", field);
        result.put("value", value);
        result.put("origin", appProperties.getLocation().name());
        result.put("location", appProperties.getLocation().name());
        result.put("writtenAt", ts);
        return result;
    }

    /** Reads all fields of a Hash key and reports fields, origin, and lag per field. */
    public Map<String, Object> readHash(String key) {
        Map<String, String> map = jedis.hgetAll(key);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("key", key);
        result.put("location", appProperties.getLocation().name());
        if (map == null || map.isEmpty()) {
            result.put("exists", false);
            result.put("fields", Map.of());
            return result;
        }

        result.put("exists", true);
        Map<String, Object> fields = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String field = entry.getKey();
            String stored = entry.getValue();
            Map<String, Object> item = new LinkedHashMap<>();
            try {
                JsonNode node = mapper.readTree(stored);
                item.put("value", node.path("value").asText());
                item.put("origin", node.path("origin").asText(null));
                long ts = node.path("ts").asLong(System.currentTimeMillis());
                item.put("writtenAt", ts);
                item.put("lagMs", System.currentTimeMillis() - ts);
            } catch (Exception e) {
                item.put("value", stored);
                item.put("origin", "unknown");
                item.put("writtenAt", null);
                item.put("lagMs", null);
            }
            fields.put(field, item);
        }
        result.put("fields", fields);
        return result;
    }

    /** Writes a Sorted Set (ZSet) member with score and JSON envelope. */
    public Map<String, Object> writeZSet(String key, String member, double score) {
        long ts = System.currentTimeMillis();
        ObjectNode envelope = mapper.createObjectNode();
        envelope.put("member", member);
        envelope.put("ts", ts);
        envelope.put("origin", appProperties.getLocation().name());

        String storedMember;
        try {
            storedMember = mapper.writeValueAsString(envelope);
        } catch (Exception e) {
            throw new IllegalStateException("failed to serialize envelope", e);
        }

        jedis.zadd(key, score, storedMember);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("key", key);
        result.put("member", member);
        result.put("score", score);
        result.put("origin", appProperties.getLocation().name());
        result.put("location", appProperties.getLocation().name());
        result.put("writtenAt", ts);
        return result;
    }

    /** Reads members and scores of a Sorted Set (ZSet) key. */
    public Map<String, Object> readZSet(String key) {
        java.util.List<redis.clients.jedis.resps.Tuple> tuples = jedis.zrangeWithScores(key, 0, -1);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("key", key);
        result.put("location", appProperties.getLocation().name());
        if (tuples == null || tuples.isEmpty()) {
            result.put("exists", false);
            result.put("members", java.util.List.of());
            return result;
        }

        result.put("exists", true);
        java.util.List<Map<String, Object>> members = new java.util.ArrayList<>();
        for (redis.clients.jedis.resps.Tuple tuple : tuples) {
            String storedElement = tuple.getElement();
            double score = tuple.getScore();
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("score", score);
            try {
                JsonNode node = mapper.readTree(storedElement);
                item.put("member", node.path("member").asText());
                item.put("origin", node.path("origin").asText(null));
                long ts = node.path("ts").asLong(System.currentTimeMillis());
                item.put("writtenAt", ts);
                item.put("lagMs", System.currentTimeMillis() - ts);
            } catch (Exception e) {
                item.put("member", storedElement);
                item.put("origin", "unknown");
                item.put("writtenAt", null);
                item.put("lagMs", null);
            }
            members.add(item);
        }
        result.put("members", members);
        return result;
    }
}
