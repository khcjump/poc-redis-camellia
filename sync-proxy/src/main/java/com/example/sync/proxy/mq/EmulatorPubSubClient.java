package com.example.sync.proxy.mq;

import com.example.sync.common.config.SyncProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * GCP Pub/Sub emulator 的 REST 客戶端（無 SDK，純 HTTP）。
 *
 * <p>emulator 端點格式（以 base = {@code {host}/v1/projects/{projectId}} 為前綴）：</p>
 * <ul>
 *   <li>PUT {@code /topics/{topic}}</li>
 *   <li>PUT {@code /subscriptions/{sub}}</li>
 *   <li>POST {@code /topics/{topic}:publish}</li>
 *   <li>POST {@code /subscriptions/{sub}:pull}</li>
 *   <li>POST {@code /subscriptions/{sub}:acknowledge}</li>
 * </ul>
 */
public class EmulatorPubSubClient implements PubSubClient {

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    private final ObjectMapper mapper = new ObjectMapper();

    private final String base;
    private final String projectId;

    public EmulatorPubSubClient(SyncProperties.PubSubConfig config) {
        this.projectId = config.getProjectId();
        this.base = trimTrailingSlash(config.getHost()) + "/v1/projects/" + projectId;
    }

    private static String trimTrailingSlash(String s) {
        return s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
    }

    private String topicPath(String topic) {
        return "projects/" + projectId + "/topics/" + topic;
    }

    @Override
    public void ensureTopic(String topic) throws Exception {
        put("/topics/" + topic, "{}");
    }

    @Override
    public void ensureSubscription(String topic, String subscription) throws Exception {
        ObjectNode body = mapper.createObjectNode();
        body.put("topic", topicPath(topic));
        put("/subscriptions/" + subscription, mapper.writeValueAsString(body));
    }

    @Override
    public void publish(String topic, byte[] data) throws Exception {
        ObjectNode message = mapper.createObjectNode();
        message.put("data", Base64.getEncoder().encodeToString(data));
        ArrayNode messages = mapper.createArrayNode();
        messages.add(message);
        ObjectNode body = mapper.createObjectNode();
        body.set("messages", messages);
        post("/topics/" + topic + ":publish", mapper.writeValueAsString(body));
    }

    @Override
    public List<PubSubMessage> pull(String subscription, int maxMessages) throws Exception {
        ObjectNode body = mapper.createObjectNode();
        body.put("maxMessages", maxMessages);
        body.put("returnImmediately", true);
        String resp = post("/subscriptions/" + subscription + ":pull", mapper.writeValueAsString(body));

        List<PubSubMessage> out = new ArrayList<>();
        JsonNode root = mapper.readTree(resp);
        JsonNode received = root.path("receivedMessages");
        if (received.isArray()) {
            for (JsonNode rm : received) {
                String ackId = rm.path("ackId").asText("");
                String dataB64 = rm.path("message").path("data").asText("");
                if (dataB64.isEmpty()) {
                    continue;
                }
                byte[] data = Base64.getDecoder().decode(dataB64);
                out.add(new PubSubMessage(data, ackId));
            }
        }
        return out;
    }

    @Override
    public void acknowledge(String subscription, List<String> ackIds) throws Exception {
        if (ackIds == null || ackIds.isEmpty()) {
            return;
        }
        ArrayNode ids = mapper.createArrayNode();
        for (String id : ackIds) {
            ids.add(id);
        }
        ObjectNode body = mapper.createObjectNode();
        body.set("ackIds", ids);
        post("/subscriptions/" + subscription + ":acknowledge", mapper.writeValueAsString(body));
    }

    @Override
    public void close() {
        // HttpClient 無需顯式關閉
    }

    private void put(String path, String body) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(base + path))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        checkStatus(resp, 200, 409);
    }

    private String post(String path, String body) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(base + path))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        checkStatus(resp, 200);
        return resp.body();
    }

    private void checkStatus(HttpResponse<String> resp, int... ok) {
        for (int code : ok) {
            if (resp.statusCode() == code) {
                return;
            }
        }
        throw new IllegalStateException("PubSub emulator HTTP " + resp.statusCode() + ": " + resp.body());
    }
}
