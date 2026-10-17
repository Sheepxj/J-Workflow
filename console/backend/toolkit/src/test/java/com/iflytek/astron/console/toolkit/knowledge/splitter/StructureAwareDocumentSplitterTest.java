package com.iflytek.astron.console.toolkit.knowledge.splitter;

import com.iflytek.astron.console.toolkit.knowledge.model.BlockType;
import com.iflytek.astron.console.toolkit.knowledge.model.DocumentBlock;
import com.iflytek.astron.console.toolkit.knowledge.model.DocumentChunk;
import com.iflytek.astron.console.toolkit.knowledge.model.ParsedDocument;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class StructureAwareDocumentSplitterTest {
    private final StructureAwareDocumentSplitter splitter = new StructureAwareDocumentSplitter();

    @Test
    void shouldPreserveHeadingPathAndRecursivelySplitLongParagraph() {
        ParsedDocument document = ParsedDocument.builder()
                .documentId("doc-1")
                .title("部署手册")
                .blocks(List.of(
                        block("系统部署", BlockType.HEADING, 1, 1),
                        block("Docker 部署", BlockType.HEADING, 2, 1),
                        block("第一句内容比较长。第二句内容比较长。第三句内容比较长。第四句内容比较长。", BlockType.PARAGRAPH, null, 2)))
                .build();
        SplitOptions options = SplitOptions.builder()
                .minSize(10)
                .targetSize(24)
                .maxSize(30)
                .overlapSize(5)
                .build();

        List<DocumentChunk> chunks = splitter.split(document, options);

        assertThat(chunks).hasSizeGreaterThan(1);
        assertThat(chunks).allSatisfy(chunk -> {
            assertThat(chunk.getContext()).isEqualTo("系统部署 > Docker 部署");
            assertThat(chunk.getContent()).startsWith("系统部署 > Docker 部署");
            assertThat(chunk.getPageStart()).isEqualTo(2);
        });
        assertThat(chunks).extracting(DocumentChunk::getChunkId).containsExactly("doc-1:0", "doc-1:1");
    }

    @Test
    void shouldKeepTableRowsSeparateFromParagraphOverlap() {
        ParsedDocument document = ParsedDocument.builder()
                .documentId("doc-2")
                .title("数据")
                .blocks(List.of(
                        block("统计", BlockType.HEADING, 1, 1),
                        block("名称 | 数量\n苹果 | 10", BlockType.TABLE, null, 1),
                        block("补充说明文本", BlockType.PARAGRAPH, null, 1)))
                .build();
        SplitOptions options = SplitOptions.builder()
                .minSize(2)
                .targetSize(100)
                .maxSize(120)
                .overlapSize(10)
                .build();

        List<DocumentChunk> chunks = splitter.split(document, options);

        assertThat(chunks).hasSize(2);
        assertThat(chunks.get(0).getBlockType()).isEqualTo(BlockType.TABLE);
        assertThat(chunks.get(1).getContent()).doesNotContain("苹果 | 10");
    }

    private DocumentBlock block(String text, BlockType type, Integer level, Integer page) {
        return DocumentBlock.builder()
                .text(text)
                .type(type)
                .headingLevel(level)
                .page(page)
                .metadata(Map.of())
                .build();
    }
}
