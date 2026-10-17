package com.iflytek.astron.console.toolkit.integration.pinecone;

import com.alibaba.fastjson2.JSON;
import com.iflytek.astron.console.commons.exception.BusinessException;
import com.iflytek.astron.console.commons.constant.ResponseEnum;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Component
public class PineconeIngestionClient {
    private static final MediaType NDJSON_MEDIA_TYPE = MediaType.get("application/x-ndjson; charset=utf-8");

    private final PineconeProperties properties;
    private final OkHttpClient httpClient;

    public PineconeIngestionClient(PineconeProperties properties) {
        this.properties = properties;
        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(properties.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                .writeTimeout(properties.getWriteTimeoutMs(), TimeUnit.MILLISECONDS)
                .callTimeout(Duration.ofMillis(properties.getWriteTimeoutMs()))
                .build();
    }

    public void upsert(String namespace, List<Map<String, Object>> records) {
        if (records == null || records.isEmpty() || !properties.isEnabled()) {
            return;
        }
        validateConfiguration();
        int batchSize = Math.max(1, properties.getUpsertBatchSize());
        for (int start = 0; start < records.size(); start += batchSize) {
            List<Map<String, Object>> batch = records.subList(start, Math.min(start + batchSize, records.size()));
            upsertBatch(resolveNamespace(namespace), batch);
        }
    }

    private void upsertBatch(String namespace, Collection<Map<String, Object>> records) {
        String body = records.stream().map(JSON::toJSONString).collect(Collectors.joining("\n"));
        Request request = new Request.Builder()
                .url(normalizeHost(properties.getIndexHost()) + "/records/namespaces/" + namespace + "/upsert")
                .header("Api-Key", properties.getApiKey())
                .header("X-Pinecone-Api-Version", properties.getApiVersion())
                .header("Content-Type", "application/x-ndjson")
                .post(RequestBody.create(body, NDJSON_MEDIA_TYPE))
                .build();
        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new BusinessException(ResponseEnum.REPO_KNOWLEDGE_ADD_FAILED);
            }
        } catch (IOException e) {
            throw new BusinessException(ResponseEnum.REPO_KNOWLEDGE_ADD_FAILED);
        }
    }

    private void validateConfiguration() {
        if (!properties.isEnabled() || !StringUtils.hasText(properties.getApiKey()) || !StringUtils.hasText(properties.getIndexHost())) {
            throw new BusinessException(ResponseEnum.REPO_KNOWLEDGE_ADD_FAILED);
        }
    }

    private String resolveNamespace(String namespace) {
        return StringUtils.hasText(namespace) ? namespace : properties.getDefaultNamespace();
    }

    private String normalizeHost(String host) {
        return host.startsWith("http://") || host.startsWith("https://") ? host : "https://" + host;
    }
}


