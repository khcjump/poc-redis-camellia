package com.example.sync.app.config;

import com.example.sync.common.model.AppRole;
import com.example.sync.common.model.QueueType;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Computes the Camellia proxy plugin wiring from the run-time role and queue selection and
 * injects it as {@code camellia-redis-proxy.config.*} dynamic configuration.
 *
 * <p>The Camellia Spring Boot starter binds {@code camellia-redis-proxy.config.<key>} into a
 * {@code Map<String,String>} that flows into {@code ProxyDynamicConf}, so anything we inject
 * here is visible to the proxy's plugin factory and route resolution.</p>
 *
 * <p>Wiring rules:</p>
 * <ul>
 *   <li>{@code proxy.plugin.list} = producer FQCN (role Gateway|All) + consumer FQCN
 *       (role SyncWorker|All, selected by consumer queue type) + {@code monitorPlugin}.</li>
 *   <li>{@code mq.multi.write.sender.class.name} = MqPackSender FQCN selected by producer
 *       queue type.</li>
 * </ul>
 */
public class SyncEnvironmentPostProcessor implements EnvironmentPostProcessor {

    private static final String PRODUCER_PLUGIN =
            "com.netease.nim.camellia.redis.proxy.mq.common.MqMultiWriteProducerProxyPlugin";

    private static final String CONSUMER_PLUGIN_PREFIX = "com.example.sync.proxy.plugin.";
    private static final String SENDER_PREFIX = "com.example.sync.proxy.mq.";

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        AppRole role = AppRole.valueOf(resolve(environment, "app.role", "APP_ROLE", AppRole.All.name()));
        QueueType producer = QueueType.valueOf(
                resolve(environment, "sync.producer-queue-type", "SYNC_PRODUCER_QUEUE_TYPE", QueueType.Kafka.name()));
        QueueType consumer = QueueType.valueOf(
                resolve(environment, "sync.consumer-queue-type", "SYNC_CONSUMER_QUEUE_TYPE", QueueType.Kafka.name()));

        List<String> pluginList = new ArrayList<>();
        if (role == AppRole.RedisGateway || role == AppRole.All) {
            pluginList.add(PRODUCER_PLUGIN);
        }
        if (role == AppRole.SyncWorker || role == AppRole.All) {
            pluginList.add(CONSUMER_PLUGIN_PREFIX + consumerPluginClass(consumer));
        }
        pluginList.add("monitorPlugin");

        Map<String, Object> injected = new HashMap<>();
        injected.put("camellia-redis-proxy.config.proxy.plugin.list", String.join(",", pluginList));
        injected.put("camellia-redis-proxy.config.mq.multi.write.sender.class.name",
                SENDER_PREFIX + senderClass(producer));

        MutablePropertySources sources = environment.getPropertySources();
        sources.addFirst(new MapPropertySource("syncEnvironmentPostProcessor", injected));
    }

    private static String consumerPluginClass(QueueType type) {
        return switch (type) {
            case GcpPubSub -> "GcpPubSubConsumerProxyPlugin";
            case RedisPubSub -> "RedisPubSubConsumerProxyPlugin";
            case Kafka -> "KafkaConsumerProxyPlugin";
        };
    }

    private static String senderClass(QueueType type) {
        return switch (type) {
            case GcpPubSub -> "GcpPubSubMqPackSender";
            case RedisPubSub -> "RedisPubSubMqPackSender";
            case Kafka -> "KafkaMqPackSender";
        };
    }

    private static String resolve(ConfigurableEnvironment environment, String propKey, String envKey, String fallback) {
        String value = environment.getProperty(propKey);
        if (value != null && !value.isBlank()) {
            return value;
        }
        value = System.getenv(envKey);
        if (value != null && !value.isBlank()) {
            return value;
        }
        return fallback;
    }
}
