package com.example.sync.proxy.route;

import com.example.sync.common.config.SyncProperties;
import com.example.sync.common.model.RedisMode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netease.nim.camellia.redis.proxy.auth.ClientIdentity;
import com.netease.nim.camellia.redis.proxy.route.RouteConfProvider;

/**
 * 依 {@link RedisMode} 產生 Camellia route.conf 字串。
 *
 * <ul>
 *   <li>Single → {@code redis://[pass@]host:port}</li>
 *   <li>Sentinel → {@code redis-sentinel://[pass@]node1:26379,node2:26379/master}</li>
 *   <li>ReadWrite → rw_separate JSON（讀寫分離雙 IP）</li>
 * </ul>
 *
 * <p>由 {@code camellia-redis-proxy.config.route.conf.provider} 指向本類 FQCN，
 * ConfigInitUtil 經 SpringProxyBeanFactory.getBean 解析（須註冊為 Spring bean）。</p>
 */
public class RedisModeRouteConfProvider extends RouteConfProvider {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final SyncProperties.RedisConfig redis;
    private final RedisMode mode;

    public RedisModeRouteConfProvider(SyncProperties.RedisConfig redis, RedisMode mode) {
        this.redis = redis;
        this.mode = mode;
    }

    @Override
    public ClientIdentity auth(String userName, String password) {
        return new ClientIdentity(1L, "default", true);
    }

    @Override
    public boolean isPasswordRequired() {
        return false;
    }

    @Override
    public String getRouteConfig() {
        return buildRouteConf();
    }

    @Override
    public String getRouteConfig(long bid, String bgroup) {
        return buildRouteConf();
    }

    @Override
    public boolean isMultiTenantsSupport() {
        return false;
    }

    private String buildRouteConf() {
        switch (mode) {
            case Sentinel:
                return sentinelUrl();
            case ReadWrite:
                return rwSeparateJson();
            case Single:
            default:
                return singleUrl();
        }
    }

    private String singleUrl() {
        return redisUrl(redis.getSingleHost(), redis.getSinglePort());
    }

    private String sentinelUrl() {
        String pass = redis.getPassword();
        // Camellia RedisResourceUtil 要求 URL 必含 @（空 auth 也需 @）：redis-sentinel://@nodes/master
        String auth = (pass == null || pass.isEmpty()) ? "@" : ":" + pass + "@";
        return "redis-sentinel://" + auth + redis.getSentinelNodes() + "/" + redis.getSentinelMaster();
    }

    private String rwSeparateJson() {
        ObjectNode op = MAPPER.createObjectNode();
        op.put("type", "rw_separate");
        op.put("read", redisUrl(redis.getReadHost(), redis.getReadPort()));
        op.put("write", redisUrl(redis.getWriteHost(), redis.getWritePort()));

        ObjectNode root = MAPPER.createObjectNode();
        root.put("type", "simple");
        root.set("operation", op);
        return root.toString();
    }

    /** 單節點 Redis URL：無密碼 {@code redis://@host:port}；有密碼 {@code redis://:pass@host:port}（Camellia 要求必含 @）。 */
    private String redisUrl(String host, int port) {
        String pass = redis.getPassword();
        if (pass == null || pass.isEmpty()) {
            return "redis://@" + host + ":" + port;
        }
        return "redis://:" + pass + "@" + host + ":" + port;
    }
}
