package com.iflytek.astron.console.toolkit.knowledge.parser;

import com.iflytek.astron.console.toolkit.knowledge.model.BlockType;
import com.iflytek.astron.console.toolkit.knowledge.model.DocumentBlock;
import com.iflytek.astron.console.toolkit.knowledge.model.ParsedDocument;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.poi.extractor.ExtractorFactory;
import org.apache.poi.extractor.POITextExtractor;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.apache.poi.xwpf.usermodel.XWPFTable;
import org.apache.poi.xwpf.usermodel.XWPFTableCell;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Component
public class StructuredDocumentParser {
    private static final Pattern MARKDOWN_HEADING = Pattern.compile("^(#{1,6})\\s+(.+)$");
    private static final Pattern NUMBERED_HEADING = Pattern.compile("^(第.{1,20}[章节篇部]|[一二三四五六七八九十]+、|\\d+(?:\\.\\d+)*[、.\\s]).+");

    public ParsedDocument parse(String documentId, String title, String fileType, InputStream inputStream) throws IOException {
        String normalizedType = fileType == null ? "" : fileType.toLowerCase(Locale.ROOT);
        List<DocumentBlock> blocks = switch (normalizedType) {
            case "txt" -> parsePlainText(inputStream);
            case "md", "markdown" -> parseMarkdown(inputStream);
            case "html", "htm" -> parseHtml(inputStream);
            case "pdf" -> parsePdf(inputStream);
            case "docx" -> parseDocx(inputStream);
            case "xls", "xlsx" -> parseWorkbook(inputStream);
            case "csv" -> parseCsv(inputStream);
            case "doc", "ppt", "pptx" -> parseOfficeFallback(inputStream);
            default -> parsePlainText(inputStream);
        };
        return ParsedDocument.builder()
                .documentId(documentId)
                .title(title)
                .blocks(blocks)
                .metadata(Map.of("fileType", normalizedType))
                .build();
    }

    private List<DocumentBlock> parsePlainText(InputStream inputStream) throws IOException {
        return paragraphs(new String(inputStream.readAllBytes(), StandardCharsets.UTF_8), null, false);
    }

    private List<DocumentBlock> parseMarkdown(InputStream inputStream) throws IOException {
        String text = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8).replace("\r\n", "\n");
        List<DocumentBlock> blocks = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean codeBlock = false;
        for (String line : text.split("\n", -1)) {
            if (line.stripLeading().startsWith("```")) {
                if (!codeBlock) {
                    flushTextBlock(blocks, current, null, BlockType.PARAGRAPH);
                    codeBlock = true;
                } else {
                    current.append(line).append('\n');
                    flushTextBlock(blocks, current, null, BlockType.CODE);
                    codeBlock = false;
                    continue;
                }
            }
            if (codeBlock) {
                current.append(line).append('\n');
                continue;
            }
            Matcher matcher = MARKDOWN_HEADING.matcher(line.trim());
            if (matcher.matches()) {
                flushTextBlock(blocks, current, null, BlockType.PARAGRAPH);
                blocks.add(block(matcher.group(2), BlockType.HEADING, matcher.group(1).length(), null, Map.of()));
            } else if (line.isBlank()) {
                flushTextBlock(blocks, current, null, BlockType.PARAGRAPH);
            } else {
                current.append(line).append('\n');
            }
        }
        flushTextBlock(blocks, current, null, codeBlock ? BlockType.CODE : BlockType.PARAGRAPH);
        return blocks;
    }

    private List<DocumentBlock> parseHtml(InputStream inputStream) throws IOException {
        org.jsoup.nodes.Document document = Jsoup.parse(inputStream, StandardCharsets.UTF_8.name(), "");
        document.select("script,style,noscript,nav,footer").remove();
        List<DocumentBlock> blocks = new ArrayList<>();
        for (Element element : document.select("h1,h2,h3,h4,h5,h6,p,li,pre,table")) {
            String text = element.text().trim();
            if (!StringUtils.hasText(text)) {
                continue;
            }
            String tag = element.tagName();
            if (tag.matches("h[1-6]")) {
                blocks.add(block(text, BlockType.HEADING, Integer.parseInt(tag.substring(1)), null, Map.of()));
            } else if ("table".equals(tag)) {
                blocks.add(block(tableText(element), BlockType.TABLE, null, null, Map.of()));
            } else {
                BlockType type = "li".equals(tag) ? BlockType.LIST_ITEM : "pre".equals(tag) ? BlockType.CODE : BlockType.PARAGRAPH;
                blocks.add(block(text, type, null, null, Map.of()));
            }
        }
        return blocks;
    }

    private String tableText(Element table) {
        return table.select("tr").stream()
                .map(row -> row.select("th,td").stream().map(Element::text).collect(Collectors.joining(" | ")))
                .filter(StringUtils::hasText)
                .collect(Collectors.joining("\n"));
    }

    private List<DocumentBlock> parsePdf(InputStream inputStream) throws IOException {
        List<DocumentBlock> blocks = new ArrayList<>();
        try (PDDocument document = Loader.loadPDF(inputStream.readAllBytes())) {
            PDFTextStripper stripper = new PDFTextStripper();
            stripper.setSortByPosition(true);
            for (int page = 1; page <= document.getNumberOfPages(); page++) {
                stripper.setStartPage(page);
                stripper.setEndPage(page);
                blocks.addAll(paragraphs(stripper.getText(document), page, true));
            }
        }
        return blocks;
    }

    private List<DocumentBlock> parseDocx(InputStream inputStream) throws IOException {
        List<DocumentBlock> blocks = new ArrayList<>();
        try (XWPFDocument document = new XWPFDocument(inputStream)) {
            for (XWPFParagraph paragraph : document.getParagraphs()) {
                String text = paragraph.getText().trim();
                if (!StringUtils.hasText(text)) {
                    continue;
                }
                Integer headingLevel = headingLevel(paragraph.getStyle(), text);
                blocks.add(block(text, headingLevel == null ? BlockType.PARAGRAPH : BlockType.HEADING, headingLevel, null, Map.of()));
            }
            for (XWPFTable table : document.getTables()) {
                String text = table.getRows().stream()
                        .map(row -> row.getTableCells().stream().map(XWPFTableCell::getText).collect(Collectors.joining(" | ")))
                        .filter(StringUtils::hasText)
                        .collect(Collectors.joining("\n"));
                if (StringUtils.hasText(text)) {
                    blocks.add(block(text, BlockType.TABLE, null, null, Map.of()));
                }
            }
        }
        return blocks;
    }

    private Integer headingLevel(String style, String text) {
        if (StringUtils.hasText(style)) {
            Matcher matcher = Pattern.compile("(?i)(?:heading|标题)\\s*([1-6])").matcher(style);
            if (matcher.find()) {
                return Integer.parseInt(matcher.group(1));
            }
        }
        return looksLikeHeading(text) ? 2 : null;
    }

    private List<DocumentBlock> parseWorkbook(InputStream inputStream) throws IOException {
        List<DocumentBlock> blocks = new ArrayList<>();
        DataFormatter formatter = new DataFormatter();
        try (Workbook workbook = WorkbookFactory.create(inputStream)) {
            for (Sheet sheet : workbook) {
                List<String> rows = new ArrayList<>();
                for (Row row : sheet) {
                    List<String> cells = new ArrayList<>();
                    int lastCell = Math.max(row.getLastCellNum(), 0);
                    for (int index = 0; index < lastCell; index++) {
                        Cell cell = row.getCell(index, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
                        cells.add(cell == null ? "" : formatter.formatCellValue(cell).trim());
                    }
                    String rowText = String.join(" | ", cells).trim();
                    if (StringUtils.hasText(rowText.replace("|", ""))) {
                        rows.add(rowText);
                    }
                }
                if (!rows.isEmpty()) {
                    String header = rows.getFirst();
                    for (int index = 1; index < rows.size(); index++) {
                        blocks.add(block(header + "\n" + rows.get(index), BlockType.TABLE, null, null, Map.of("sheetName", sheet.getSheetName())));
                    }
                    if (rows.size() == 1) {
                        blocks.add(block(header, BlockType.TABLE, null, null, Map.of("sheetName", sheet.getSheetName())));
                    }
                }
            }
        }
        return blocks;
    }

    private List<DocumentBlock> parseCsv(InputStream inputStream) throws IOException {
        String text = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8).replace("\r\n", "\n");
        List<String> rows = text.lines().filter(StringUtils::hasText).toList();
        if (rows.isEmpty()) {
            return List.of();
        }
        String header = rows.getFirst();
        List<DocumentBlock> blocks = new ArrayList<>();
        for (int index = 1; index < rows.size(); index++) {
            blocks.add(block(header + "\n" + rows.get(index), BlockType.TABLE, null, null, Map.of()));
        }
        if (rows.size() == 1) {
            blocks.add(block(header, BlockType.TABLE, null, null, Map.of()));
        }
        return blocks;
    }

    private List<DocumentBlock> parseOfficeFallback(InputStream inputStream) throws IOException {
        try (POITextExtractor extractor = ExtractorFactory.createExtractor(inputStream)) {
            return paragraphs(extractor.getText(), null, true);
        } catch (Exception e) {
            throw new IOException("Office document parsing failed", e);
        }
    }

    private List<DocumentBlock> paragraphs(String text, Integer page, boolean inferHeadings) {
        String normalized = text.replace("\r\n", "\n").replace('\r', '\n');
        List<DocumentBlock> blocks = new ArrayList<>();
        for (String paragraph : normalized.split("\\n\\s*\\n|\\n")) {
            String value = paragraph.trim();
            if (!StringUtils.hasText(value)) {
                continue;
            }
            boolean heading = inferHeadings && looksLikeHeading(value);
            blocks.add(block(value, heading ? BlockType.HEADING : BlockType.PARAGRAPH, heading ? 2 : null, page, Map.of()));
        }
        return blocks;
    }

    private boolean looksLikeHeading(String text) {
        if (text.length() > 80 || text.contains("\n")) {
            return false;
        }
        return NUMBERED_HEADING.matcher(text).matches();
    }

    private void flushTextBlock(List<DocumentBlock> blocks, StringBuilder current, Integer page, BlockType type) {
        String value = current.toString().trim();
        current.setLength(0);
        if (StringUtils.hasText(value)) {
            blocks.add(block(value, type, null, page, Map.of()));
        }
    }

    private DocumentBlock block(String text, BlockType type, Integer headingLevel, Integer page, Map<String, Object> metadata) {
        return DocumentBlock.builder()
                .text(text)
                .type(type)
                .headingLevel(headingLevel)
                .page(page)
                .metadata(new LinkedHashMap<>(metadata))
                .build();
    }
}
