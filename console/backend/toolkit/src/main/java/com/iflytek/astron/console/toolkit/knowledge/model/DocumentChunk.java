package com.iflytek.astron.console.toolkit.knowledge.model;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
public class DocumentChunk {
    private String chunkId;
    private int chunkIndex;
    private String documentId;
    private String title;
    private String content;
    private String context;
    private Integer pageStart;
    private Integer pageEnd;
    private String headingPath;
    private BlockType blockType;
    @Builder.Default
    private Map<String, Object> metadata = Map.of();
}
