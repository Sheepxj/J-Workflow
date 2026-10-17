package com.iflytek.astron.console.toolkit.knowledge.model;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
public class DocumentBlock {
    private String text;
    private BlockType type;
    private Integer headingLevel;
    private Integer page;
    private String sheetName;
    @Builder.Default
    private Map<String, Object> metadata = Map.of();
}
