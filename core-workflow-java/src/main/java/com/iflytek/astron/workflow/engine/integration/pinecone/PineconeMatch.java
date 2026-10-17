package com.iflytek.astron.workflow.engine.integration.pinecone;

import lombok.Builder;

import java.util.Map;

@Builder
public record PineconeMatch(
        String id,
        double score,
        String content,
        Map<String, Object> metadata) {
}
