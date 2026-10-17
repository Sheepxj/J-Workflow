package com.iflytek.astron.console.toolkit.knowledge.parser;

import com.iflytek.astron.console.toolkit.knowledge.model.BlockType;
import com.iflytek.astron.console.toolkit.knowledge.model.ParsedDocument;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class StructuredDocumentParserTest {
    private final StructuredDocumentParser parser = new StructuredDocumentParser();

    @Test
    void shouldParseMarkdownHeadingsParagraphsAndCodeBlocks() throws Exception {
        String markdown = "# 系统部署\n\n正文内容。\n\n## 配置\n\n```yaml\nserver:\n  port: 8080\n```";

        ParsedDocument document = parser.parse(
                "doc-1",
                "README.md",
                "md",
                new ByteArrayInputStream(markdown.getBytes(StandardCharsets.UTF_8)));

        assertThat(document.getBlocks()).extracting(block -> block.getType()).containsExactly(
                BlockType.HEADING,
                BlockType.PARAGRAPH,
                BlockType.HEADING,
                BlockType.CODE);
        assertThat(document.getBlocks().getFirst().getHeadingLevel()).isEqualTo(1);
        assertThat(document.getBlocks().getLast().getText()).contains("port: 8080");
    }
}
