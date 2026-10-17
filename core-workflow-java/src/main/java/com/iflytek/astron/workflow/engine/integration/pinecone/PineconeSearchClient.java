package com.iflytek.astron.workflow.engine.integration.pinecone;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.iflytek.astron.workflow.exception.ErrorCode;
import com.iflytek.astron.workflow.exception.NodeCustomException;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class PineconeSearchClient {
    private static final MediaType JSON_MEDIA_TYPE = MediaType.get("application/json; charset=utf-8");

    private final PineconeProperties properties;
    private final OkHttpClient httpClient;

    @Autowired
    public PineconeSearchClient(PineconeProperties properties) {
        this(properties, new OkHttpClient.Builder()
                .connectTimeout(properties.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                .readTimeout(properties.getReadTimeoutMs(), TimeUnit.MILLISECONDS)
                .callTimeout(Duration.ofMillis(properties.getReadTimeoutMs()))
                .build());
    }

    PineconeSearchClient(PineconeProperties properties, OkHttpClient httpClient) {
        this.properties = properties;
        this.httpClient = httpClient;
    }

    public List<PineconeMatch> search(String query, String namespace, int topK, Map<String, Object> filter) {
        validateConfiguration();
        if (!StringUtils.hasText(query)) {
            return List.of();
        }

        JSONObject requestBody = new JSONObject();
        JSONObject queryBody = new JSONObject();
        queryBody.put("top_k", topK);
        queryBody.put("inputs", Map.of("text", query));
        if (filter != null && !filter.isEmpty()) {
            queryBody.put("filter", filter);
        }
        requestBody.put("query", queryBody);
        requestBody.put("fields", List.of("chunk_text", "repo_id", "document_id", "file_name", "page", "chunk_index", "source_uri"));

        String resolvedNamespace = StringUtils.hasText(namespace) ? namespace : properties.getDefaultNamespace();
        String url = normalizeHost(properties.getIndexHost()) + "/records/namespaces/" + resolvedNamespace + "/search";
        Request request = new Request.Builder()
                .url(url)
                .header("Api-Key", properties.getApiKey())
                .header("X-Pinecone-Api-Version", properties.getApiVersion())
                .header("Content-Type", "application/json")
                .post(RequestBody.create(JSON.toJSONString(requestBody), JSON_MEDIA_TYPE))
                .build();

        try (Response response = httpClient.newCall(request).execute()) {
            String responseBody = response.body() == null ? "" : response.body().string();
            if (!response.isSuccessful()) {
                log.warn("Pinecone search failed, status={}, body={}", response.code(), responseBody);
                throw new NodeCustomException(ErrorCode.NODE_RUN_ERROR, "Pinecone search failed: HTTP " + response.code());
            }
            return parseMatches(responseBody);
        } catch (IOException e) {
            throw new NodeCustomException(ErrorCode.NODE_RUN_ERROR, "Pinecone search request failed: " + e.getMessage());
        }
    }

    private List<PineconeMatch> parseMatches(String responseBody) {
        try {
            JSONObject root = JSON.parseObject(responseBody);
            JSONArray hits = root.getJSONArray("result");
            if (hits == null) {
                JSONObject result = root.getJSONObject("result");
                hits = result == null ? null : result.getJSONArray("hits");
            }
            if (hits == null) {
                hits = root.getJSONArray("matches");
            }
            if (hits == null || hits.isEmpty()) {
                return List.of();
            }
            List<PineconeMatch> matches = new ArrayList<>(hits.size());
            for (Object hit : hits) {
                JSONObject item = (JSONObject) hit;
                JSONObject fields = item.getJSONObject("fields");
                if (fields == null) {
                    fields = new JSONObject();
                }
                String id = item.getString("_id");
                if (!StringUtils.hasText(id)) {
                    id = item.getString("id");
                }
                Double score = item.getDouble("_score");
                if (score == null) {
                    score = item.getDouble("score");
                }
                matches.add(PineconeMatch.builder()
                        .id(id)
                        .score(score == null ? 0D : score)
                        .content(fields.getString("chunk_text"))
                        .metadata(Map.copyOf(fields))
                        .build());
            }
            return matches;
        } catch (Exception e) {
            throw new NodeCustomException(ErrorCode.NODE_RUN_ERROR, "Invalid Pinecone response: " + e.getMessage());
        }
    }

    private void validateConfiguration() {
        if (!properties.isEnabled()) {
            throw new NodeCustomException(ErrorCode.INVALID_NODE_CONFIGURATION, "Pinecone integration is disabled");
        }
        if (!StringUtils.hasText(properties.getApiKey()) || !StringUtils.hasText(properties.getIndexHost())) {
            throw new NodeCustomException(ErrorCode.INVALID_NODE_CONFIGURATION, "Pinecone API key or index host is not configured");
        }
    }

    private String normalizeHost(String host) {
        return host.startsWith("http://") || host.startsWith("https://") ? host : "https://" + host;
    }
}


