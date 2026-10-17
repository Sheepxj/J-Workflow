package com.iflytek.astron.console.toolkit.integration.pinecone;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.iflytek.astron.console.commons.constant.ResponseEnum;
import com.iflytek.astron.console.commons.exception.BusinessException;
import com.iflytek.astron.console.toolkit.entity.core.knowledge.ChunkInfo;
import com.iflytek.astron.console.toolkit.entity.table.knowledge.MysqlPreviewKnowledge;
import com.iflytek.astron.console.toolkit.entity.table.repo.FileInfoV2;
import com.iflytek.astron.console.toolkit.mapper.knowledge.PreviewKnowledgeMapper;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
public class PineconeIngestionService {
    private static final MediaType JSON_MEDIA_TYPE = MediaType.get("application/json; charset=utf-8");

    private final PineconeProperties properties;
    private final PineconeIngestionClient ingestionClient;
    private final PreviewKnowledgeMapper previewKnowledgeMapper;
    private final OkHttpClient httpClient;

    public PineconeIngestionService(PineconeProperties properties, PineconeIngestionClient ingestionClient,
            PreviewKnowledgeMapper previewKnowledgeMapper) {
        this.properties = properties;
        this.ingestionClient = ingestionClient;
        this.previewKnowledgeMapper = previewKnowledgeMapper;
        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(properties.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                .writeTimeout(properties.getWriteTimeoutMs(), TimeUnit.MILLISECONDS)
                .build();
    }

    public void upsertDocument(FileInfoV2 fileInfo, List<ChunkInfo> chunks) {
        if (!properties.isEnabled() || CollectionUtils.isEmpty(chunks)) {
            return;
        }
        List<Map<String, Object>> records = new ArrayList<>();
        for (int index = 0; index < chunks.size(); index++) {
            ChunkInfo chunk = chunks.get(index);
            if (chunk == null || chunk.getContent() == null || chunk.getContent().isBlank()) {
                continue;
            }
            Map<String, Object> record = new LinkedHashMap<>();
            record.put("_id", recordId(fileInfo, index));
            record.put("chunk_text", chunk.getContent());
            record.put("repo_id", String.valueOf(fileInfo.getRepoId()));
            record.put("document_id", String.valueOf(fileInfo.getId()));
            record.put("document_uuid", fileInfo.getUuid());
            record.put("file_name", fileInfo.getName());
            record.put("chunk_index", index);
            record.put("enabled", Integer.valueOf(1).equals(fileInfo.getEnabled()));
            record.put("source_uri", fileInfo.getAddress());
            appendReference(record, chunk.getReferences());
            records.add(record);
        }
        ingestionClient.upsert(namespace(fileInfo), records);
    }

    public boolean isEnabled() {
        return properties.isEnabled();
    }

    public void syncDocumentEnabled(FileInfoV2 fileInfo) {
        if (!properties.isEnabled()) {
            return;
        }
        if (!Integer.valueOf(1).equals(fileInfo.getEnabled())) {
            deleteDocument(fileInfo);
            return;
        }
        String previewFileId = fileInfo.getLastUuid() == null ? fileInfo.getUuid() : fileInfo.getLastUuid();
        List<MysqlPreviewKnowledge> previews = previewKnowledgeMapper.findByFileId(previewFileId);
        List<ChunkInfo> chunks = previews.stream()
                .map(preview -> JSON.parseObject(JSON.toJSONString(preview.getContent()), ChunkInfo.class))
                .toList();
        replaceDocument(fileInfo, chunks);
    }

    public void replaceDocument(FileInfoV2 fileInfo, List<ChunkInfo> chunks) {
        if (!properties.isEnabled()) {
            return;
        }
        deleteDocument(fileInfo);
        upsertDocument(fileInfo, chunks);
    }

    public void deleteDocument(FileInfoV2 fileInfo) {
        if (!properties.isEnabled()) {
            return;
        }
        JSONObject payload = new JSONObject();
        payload.put("namespace", namespace(fileInfo));
        payload.put("filter", Map.of("document_id", Map.of("$eq", String.valueOf(fileInfo.getId()))));
        Request request = new Request.Builder()
                .url(normalizeHost(properties.getIndexHost()) + "/vectors/delete")
                .header("Api-Key", properties.getApiKey())
                .header("X-Pinecone-Api-Version", properties.getApiVersion())
                .header("Content-Type", "application/json")
                .post(RequestBody.create(JSON.toJSONString(payload), JSON_MEDIA_TYPE))
                .build();
        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new BusinessException(ResponseEnum.REPO_KNOWLEDGE_DELETE_FAILED);
            }
        } catch (IOException e) {
            throw new BusinessException(ResponseEnum.REPO_KNOWLEDGE_DELETE_FAILED);
        }
    }

    private void appendReference(Map<String, Object> record, JSONObject references) {
        if (references == null || references.isEmpty()) {
            return;
        }
        record.put("references", references);
        Object page = references.get("page");
        if (page != null) {
            record.put("page", page);
        }
    }

    private String recordId(FileInfoV2 fileInfo, int chunkIndex) {
        return namespace(fileInfo) + ":" + fileInfo.getRepoId() + ":" + fileInfo.getId() + ":" + chunkIndex;
    }

    private String namespace(FileInfoV2 fileInfo) {
        return fileInfo.getSpaceId() == null ? properties.getDefaultNamespace() : String.valueOf(fileInfo.getSpaceId());
    }

    private String normalizeHost(String host) {
        return host.startsWith("http://") || host.startsWith("https://") ? host : "https://" + host;
    }
}


