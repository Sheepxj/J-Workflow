package com.iflytek.astron.console.toolkit.knowledge.splitter;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class SplitOptions {
    private int targetSize;
    private int minSize;
    private int maxSize;
    private int overlapSize;
    @Builder.Default
    private List<String> separators = List.of("\n\n", "\n", "。", "！", "？", "；", ". ", "! ", "? ", "; ", "，", ",", " ");
}
