package com.example.sync.proxy.mq;

import com.example.sync.common.config.SyncProperties;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/**
 * Redis Pub/Sub broker：以 JedisPool 發送（publish）與訂閱（subscribe）。
 *
 * <p>全程使用 byte[] 介面（{@code publish(byte[],byte[])} / {@code BinaryJedisPubSub}）以保持 binary-safe，
 * 因為 MqPackSerializer 產出的 payload 是 msgpack 二進位。</p>
 */
public class RedisPubSubBroker {

    private final JedisPool pool;
    private final byte[] channelBytes;

    public RedisPubSubBroker(SyncProperties.RedisPubSubConfig config) {
        this.channelBytes = config.getChannel().getBytes(StandardCharsets.UTF_8);
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(8);
        poolConfig.setMaxIdle(8);
        String password = config.getPassword();
        if (password == null || password.isEmpty()) {
            this.pool = new JedisPool(poolConfig, config.getHost(), config.getPort(),
                    Protocol.DEFAULT_TIMEOUT);
        } else {
            this.pool = new JedisPool(poolConfig, config.getHost(), config.getPort(),
                    Protocol.DEFAULT_TIMEOUT, password);
        }
    }

    /** 發送一則訊息至 channel（binary-safe）。 */
    public void publish(byte[] data) {
        try (Jedis jedis = pool.getResource()) {
            jedis.publish(channelBytes, data);
        }
    }

    /** 阻塞式訂閱；每收到一則訊息呼叫 onMessage（由呼叫端起專屬 thread）。 */
    public void subscribe(Consumer<byte[]> onMessage) {
        try (Jedis jedis = pool.getResource()) {
            jedis.subscribe(new BinaryJedisPubSub() {
                @Override
                public void onMessage(byte[] channel, byte[] message) {
                    onMessage.accept(message);
                }
            }, channelBytes);
        }
    }

    public byte[] channelBytes() {
        return channelBytes;
    }

    public void close() {
        pool.close();
    }
}
