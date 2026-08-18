package com.example.sync.common.config;

import com.example.sync.common.model.QueueType;
import com.example.sync.common.model.RedisMode;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 同步層設定（prefix = {@code sync}）。
 */
@ConfigurationProperties(prefix = "sync")
public class SyncProperties {

    /** 佇列類型（出站發布與入站消費之訊息佇列方案） */
    private QueueType queueType = QueueType.Kafka;

    /** 本地 Redis 拓樸 */
    private RedisMode redisMode = RedisMode.Single;

    /** 遠端 App API 位址（例: http://onprem-app:8081），用於佇列消費後跨區叫用 REST API 重放至遠端 Redis */
    private String remoteAppUrl = "";

    /** 佇列 TTL（秒），套用於 PubSub retention 與 Kafka retention；RedisPubSub 不適用 */
    private long queueTtlSeconds = 30;

    /** 大小網切換防抖 */
    private SwitchConfig swtch = new SwitchConfig();

    /** Redis 後端連線 */
    private RedisConfig redis = new RedisConfig();

    /** GCP Pub/Sub */
    private PubSubConfig pubsub = new PubSubConfig();

    /** Kafka */
    private KafkaConfig kafka = new KafkaConfig();

    /** Redis Pub/Sub（broker） */
    private RedisPubSubConfig redisPubSub = new RedisPubSubConfig();

    public QueueType getQueueType() {
        return queueType;
    }

    public void setQueueType(QueueType queueType) {
        this.queueType = queueType;
    }

    public QueueType getProducerQueueType() {
        return getQueueType();
    }

    public void setProducerQueueType(QueueType producerQueueType) {
        setQueueType(producerQueueType);
    }

    public QueueType getConsumerQueueType() {
        return getQueueType();
    }

    public void setConsumerQueueType(QueueType consumerQueueType) {
        setQueueType(consumerQueueType);
    }

    public RedisMode getRedisMode() {
        return redisMode;
    }

    public void setRedisMode(RedisMode redisMode) {
        this.redisMode = redisMode;
    }

    public String getRemoteAppUrl() {
        return remoteAppUrl;
    }

    public void setRemoteAppUrl(String remoteAppUrl) {
        this.remoteAppUrl = remoteAppUrl;
    }

    public long getQueueTtlSeconds() {
        return queueTtlSeconds;
    }

    public void setQueueTtlSeconds(long queueTtlSeconds) {
        this.queueTtlSeconds = queueTtlSeconds;
    }

    public SwitchConfig getSwtch() {
        return swtch;
    }

    public void setSwtch(SwitchConfig swtch) {
        this.swtch = swtch;
    }

    public RedisConfig getRedis() {
        return redis;
    }

    public void setRedis(RedisConfig redis) {
        this.redis = redis;
    }

    public PubSubConfig getPubsub() {
        return pubsub;
    }

    public void setPubsub(PubSubConfig pubsub) {
        this.pubsub = pubsub;
    }

    public KafkaConfig getKafka() {
        return kafka;
    }

    public void setKafka(KafkaConfig kafka) {
        this.kafka = kafka;
    }

    public RedisPubSubConfig getRedisPubSub() {
        return redisPubSub;
    }

    public void setRedisPubSub(RedisPubSubConfig redisPubSub) {
        this.redisPubSub = redisPubSub;
    }

    /** 大小網切換防抖 */
    public static class SwitchConfig {
        /** 最短切換間隔（秒） */
        private long minIntervalSeconds = 10;

        public long getMinIntervalSeconds() {
            return minIntervalSeconds;
        }

        public void setMinIntervalSeconds(long minIntervalSeconds) {
            this.minIntervalSeconds = minIntervalSeconds;
        }
    }

    /** Redis 後端連線 */
    public static class RedisConfig {
        private String password = "";
        /** Single：單節點 */
        private String singleHost = "127.0.0.1";
        private int singlePort = 6379;
        /** Sentinel */
        private String sentinelNodes = "127.0.0.1:26379";
        private String sentinelMaster = "mymaster";
        /** ReadWrite：讀寫分離 */
        private String readHost = "127.0.0.1";
        private int readPort = 6379;
        private String writeHost = "127.0.0.1";
        private int writePort = 6379;

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getSingleHost() {
            return singleHost;
        }

        public void setSingleHost(String singleHost) {
            this.singleHost = singleHost;
        }

        public int getSinglePort() {
            return singlePort;
        }

        public void setSinglePort(int singlePort) {
            this.singlePort = singlePort;
        }

        public String getSentinelNodes() {
            return sentinelNodes;
        }

        public void setSentinelNodes(String sentinelNodes) {
            this.sentinelNodes = sentinelNodes;
        }

        public String getSentinelMaster() {
            return sentinelMaster;
        }

        public void setSentinelMaster(String sentinelMaster) {
            this.sentinelMaster = sentinelMaster;
        }

        public String getReadHost() {
            return readHost;
        }

        public void setReadHost(String readHost) {
            this.readHost = readHost;
        }

        public int getReadPort() {
            return readPort;
        }

        public void setReadPort(int readPort) {
            this.readPort = readPort;
        }

        public String getWriteHost() {
            return writeHost;
        }

        public void setWriteHost(String writeHost) {
            this.writeHost = writeHost;
        }

        public int getWritePort() {
            return writePort;
        }

        public void setWritePort(int writePort) {
            this.writePort = writePort;
        }
    }

    /** GCP Pub/Sub（emulator 優先） */
    public static class PubSubConfig {
        /** emulator base URL（含 scheme，如 http://cloud-pubsub:8085） */
        private String host = "http://cloud-pubsub:8085";
        private String projectId = "camellia-sync-demo";
        /** 出站 topic */
        private String topic = "camellia-sync";
        /** 入站 subscription */
        private String subscription = "camellia-sync-sub";
        private int pollMaxMessages = 200;
        private long pollIntervalMs = 100;

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public String getProjectId() {
            return projectId;
        }

        public void setProjectId(String projectId) {
            this.projectId = projectId;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getSubscription() {
            return subscription;
        }

        public void setSubscription(String subscription) {
            this.subscription = subscription;
        }

        public int getPollMaxMessages() {
            return pollMaxMessages;
        }

        public void setPollMaxMessages(int pollMaxMessages) {
            this.pollMaxMessages = pollMaxMessages;
        }

        public long getPollIntervalMs() {
            return pollIntervalMs;
        }

        public void setPollIntervalMs(long pollIntervalMs) {
            this.pollIntervalMs = pollIntervalMs;
        }
    }

    /** Kafka */
    public static class KafkaConfig {
        private String bootstrapServers = "localhost:9092";
        private String topic = "camellia-sync";
        private String groupId = "camellia";

        public String getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }
    }

    /** Redis Pub/Sub（broker 連線） */
    public static class RedisPubSubConfig {
        private String host = "127.0.0.1";
        private int port = 6379;
        private String password = "";
        private String channel = "__camellia-sync";

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getChannel() {
            return channel;
        }

        public void setChannel(String channel) {
            this.channel = channel;
        }
    }
}
