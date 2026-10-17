package com.iflytek.astron.console.toolkit.knowledge.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@Builder
public class ParsedDocument {
    private String documentId;
    private String title;
    @Builder.Default
    private List<DocumentBlock> blocks = List.of();
    @Builder.Default
    private Map<String, Object> metadata = Map.of();
}
