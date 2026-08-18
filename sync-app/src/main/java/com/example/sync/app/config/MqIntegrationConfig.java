package com.example.sync.app.config;

import com.example.sync.common.config.AppProperties;
import com.example.sync.common.config.SyncProperties;
import com.example.sync.common.metrics.QueueMetrics;
import com.example.sync.common.metrics.SimpleQueueMetrics;
import com.example.sync.proxy.console.SyncConsoleService;
import com.example.sync.proxy.mq.EmulatorPubSubClient;
import com.example.sync.proxy.mq.GcpPubSubMqPackSender;
import com.example.sync.proxy.mq.KafkaMqPackSender;
import com.example.sync.proxy.mq.PubSubClient;
import com.example.sync.proxy.mq.RedisPubSubBroker;
import com.example.sync.proxy.mq.RedisPubSubMqPackSender;
import com.example.sync.proxy.plugin.GcpPubSubConsumerProxyPlugin;
import com.example.sync.proxy.plugin.KafkaConsumerProxyPlugin;
import com.example.sync.proxy.plugin.RedisPubSubConsumerProxyPlugin;
import com.example.sync.proxy.route.RedisModeRouteConfProvider;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the MQ integration beans. The Camellia plugin factory resolves our plugin/sender
 * classes through {@code ServerConf.getProxyBeanFactory().getBean(clazz)}, which in turn
 * delegates to the Spring {@code ApplicationContext}, so every class referenced by
 * {@code proxy.plugin.list} / {@code mq.multi.write.sender.class.name} must be registered
 * here as a Spring bean with its concrete type.
 */
@Configuration
public class MqIntegrationConfig {

    @Bean
    public QueueMetrics queueMetrics(MeterRegistry meterRegistry) {
        SimpleQueueMetrics metrics = new SimpleQueueMetrics();
        metrics.bindTo(meterRegistry);
        return metrics;
    }

    @Bean
    public PubSubClient pubSubClient(SyncProperties props) {
        return new EmulatorPubSubClient(props.getPubsub());
    }

    @Bean
    public RedisPubSubBroker redisPubSubBroker(SyncProperties props) {
        return new RedisPubSubBroker(props.getRedisPubSub());
    }

    // --- producers & consumers (selected by sync.queue-type) ---

    @Bean
    @ConditionalOnProperty(name = "sync.queue-type", havingValue = "GcpPubSub")
    public GcpPubSubMqPackSender gcpPubSubMqPackSender(PubSubClient client, SyncProperties props, QueueMetrics metrics) {
        return new GcpPubSubMqPackSender(client, props.getPubsub(), metrics);
    }

    @Bean
    @ConditionalOnProperty(name = "sync.queue-type", havingValue = "RedisPubSub")
    public RedisPubSubMqPackSender redisPubSubMqPackSender(RedisPubSubBroker broker, QueueMetrics metrics) {
        return new RedisPubSubMqPackSender(broker, metrics);
    }

    @Bean
    @ConditionalOnProperty(name = "sync.queue-type", havingValue = "Kafka")
    public KafkaMqPackSender kafkaMqPackSender(SyncProperties props, QueueMetrics metrics) {
        return new KafkaMqPackSender(props.getKafka(), metrics);
    }

    @Bean
    @ConditionalOnProperty(name = "sync.queue-type", havingValue = "GcpPubSub")
    public GcpPubSubConsumerProxyPlugin gcpPubSubConsumerProxyPlugin(
            PubSubClient client, SyncProperties props, QueueMetrics metrics) {
        return new GcpPubSubConsumerProxyPlugin(client, props.getPubsub(), metrics, props.getRemoteAppUrl(), props.getSkipRegExp());
    }

    @Bean
    @ConditionalOnProperty(name = "sync.queue-type", havingValue = "RedisPubSub")
    public RedisPubSubConsumerProxyPlugin redisPubSubConsumerProxyPlugin(
            RedisPubSubBroker broker, SyncProperties props, QueueMetrics metrics) {
        return new RedisPubSubConsumerProxyPlugin(broker, metrics, props.getRemoteAppUrl(), props.getSkipRegExp());
    }

    @Bean
    @ConditionalOnProperty(name = "sync.queue-type", havingValue = "Kafka")
    public KafkaConsumerProxyPlugin kafkaConsumerProxyPlugin(SyncProperties props, QueueMetrics metrics) {
        return new KafkaConsumerProxyPlugin(props.getKafka(), metrics, props.getRemoteAppUrl(), props.getSkipRegExp());
    }

    // --- route + console ---

    @Bean
    public RedisModeRouteConfProvider redisModeRouteConfProvider(SyncProperties props) {
        return new RedisModeRouteConfProvider(props.getRedis(), props.getRedisMode());
    }

    @Bean
    public SyncConsoleService syncConsoleService(
            AppProperties appProperties, SyncProperties syncProperties, QueueMetrics metrics) {
        return new SyncConsoleService(appProperties, syncProperties, metrics);
    }
}
