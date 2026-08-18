package com.example.sync.proxy.route;

import com.example.sync.common.config.SyncProperties;
import com.example.sync.common.model.RedisMode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netease.nim.camellia.redis.proxy.auth.ClientIdentity;
import com.netease.nim.camellia.redis.proxy.route.RouteConfProvider;

/**
 * Camellia 路由動態提供者（Redis Mode RouteConfProvider）：
 * 
 * <h3>資料流程與架構腳色：</h3>
 * <ol>
 *   <li><b>Proxy 路由初始化</b>：
 *       Camellia Redis Proxy 啟動時，會向此 Provider 查詢當前的 Upstream Redis 路由規則（{@link #getRouteConfig}）。
 *   </li>
 *   <li><b>多架構動態適配 (RedisMode)</b>：
 *       根據環境變數配置的 {@link RedisMode} 動態產生 Camellia 標準 Route JSON 或 URI 格式：
 *       <ul>
 *         <li><b>Single 模式</b>： {@code redis://@host:port}（單節點直連）。</li>
 *         <li><b>Sentinel 模式</b>： {@code redis-sentinel://@sentinel_nodes/master_name}（高可用哨兵集群）。</li>
 *         <li><b>ReadWrite 模式</b>： 產生 {@code rw_separate} JSON，將寫入指令（SET/HSET/ZADD）導向寫庫（Write Node），讀取指令（GET/HGET/ZRANGE）導向讀庫（Read Node）。</li>
 *       </ul>
 *   </li>
 * </ol>
 *
 * <p>註冊為 Spring Bean；全名 (FQCN) 經由 {@code camellia-redis-proxy.config.route.conf.provider} 配置。</p>
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

    /**
     * 依據 RedisMode 產生對應的 Camellia 路由字串或 JSON
     */
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

    /**
     * 產生讀寫分離 (Read/Write Separation) 的 Camellia 路由 JSON
     */
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
