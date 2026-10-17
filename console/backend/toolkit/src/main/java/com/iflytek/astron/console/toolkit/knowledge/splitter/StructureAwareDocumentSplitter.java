package com.iflytek.astron.console.toolkit.knowledge.splitter;

import com.iflytek.astron.console.toolkit.knowledge.model.BlockType;
import com.iflytek.astron.console.toolkit.knowledge.model.DocumentBlock;
import com.iflytek.astron.console.toolkit.knowledge.model.DocumentChunk;
import com.iflytek.astron.console.toolkit.knowledge.model.ParsedDocument;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class StructureAwareDocumentSplitter {
    public List<DocumentChunk> split(ParsedDocument document, SplitOptions options) {
        validate(options);
        List<Piece> pieces = createPieces(document, options);
        List<Piece> packed = mergeShortPieces(packPieces(pieces, options), options);
        return buildChunks(document, packed, options);
    }

    private List<Piece> createPieces(ParsedDocument document, SplitOptions options) {
        List<Piece> pieces = new ArrayList<>();
        List<String> headingStack = new ArrayList<>();
        for (DocumentBlock block : document.getBlocks()) {
            if (block == null || !StringUtils.hasText(block.getText())) {
                continue;
            }
            if (block.getType() == BlockType.HEADING) {
                updateHeadingPath(headingStack, block.getHeadingLevel(), block.getText().trim());
                continue;
            }
            String context = String.join(" > ", headingStack);
            for (String content : splitRecursively(block.getText(), 0, options)) {
                if (StringUtils.hasText(content)) {
                    pieces.add(new Piece(content.trim(), context, block.getType(), block.getPage(), block.getPage(), block.getMetadata()));
                }
            }
        }
        return pieces;
    }

    private void updateHeadingPath(List<String> headingStack, Integer headingLevel, String heading) {
        int level = headingLevel == null ? 1 : Math.max(1, Math.min(6, headingLevel));
        level = Math.min(level, headingStack.size() + 1);
        while (headingStack.size() >= level) {
            headingStack.removeLast();
        }
        headingStack.add(heading);
    }

    private List<Piece> packPieces(List<Piece> pieces, SplitOptions options) {
        List<Piece> result = new ArrayList<>();
        Piece current = null;
        for (Piece piece : pieces) {
            if (current == null) {
                current = piece;
                continue;
            }
            if (canMerge(current, piece) && current.content().length() + 1 + piece.content().length() <= options.getTargetSize()) {
                current = merge(current, piece);
            } else {
                result.add(current);
                current = piece;
            }
        }
        if (current != null) {
            result.add(current);
        }
        return result;
    }

    private List<Piece> mergeShortPieces(List<Piece> pieces, SplitOptions options) {
        if (pieces.size() < 2) {
            return pieces;
        }
        List<Piece> result = new ArrayList<>();
        for (Piece piece : pieces) {
            if (!result.isEmpty() && piece.content().length() < options.getMinSize()) {
                Piece previous = result.getLast();
                if (canMerge(previous, piece) && previous.content().length() + 1 + piece.content().length() <= options.getMaxSize()) {
                    result.set(result.size() - 1, merge(previous, piece));
                    continue;
                }
            }
            result.add(piece);
        }
        return result;
    }

    private boolean canMerge(Piece left, Piece right) {
        if (!left.context().equals(right.context())) {
            return false;
        }
        if (left.type() == BlockType.TABLE || right.type() == BlockType.TABLE) {
            return left.type() == right.type() && left.metadata().equals(right.metadata());
        }
        return left.type() != BlockType.CODE && right.type() != BlockType.CODE;
    }

    private Piece merge(Piece left, Piece right) {
        Map<String, Object> metadata = new LinkedHashMap<>(left.metadata());
        metadata.putAll(right.metadata());
        return new Piece(
                left.content() + "\n" + right.content(),
                left.context(),
                left.type(),
                firstPage(left.pageStart(), right.pageStart()),
                lastPage(left.pageEnd(), right.pageEnd()),
                metadata);
    }

    private List<DocumentChunk> buildChunks(ParsedDocument document, List<Piece> pieces, SplitOptions options) {
        List<DocumentChunk> chunks = new ArrayList<>();
        Piece previous = null;
        for (Piece piece : pieces) {
            String body = piece.content();
            if (previous != null && shouldOverlap(previous, piece) && options.getOverlapSize() > 0) {
                String overlap = createOverlap(previous.content(), options.getOverlapSize());
                int available = options.getMaxSize() - overlap.length() - 1;
                if (available > 0 && body.length() > available) {
                    body = body.substring(0, available).trim();
                }
                if (StringUtils.hasText(overlap)) {
                    body = overlap + "\n" + body;
                }
            }
            int index = chunks.size();
            String embeddingContent = StringUtils.hasText(piece.context()) ? piece.context() + "\n\n" + body : body;
            chunks.add(DocumentChunk.builder()
                    .chunkId(document.getDocumentId() + ":" + index)
                    .chunkIndex(index)
                    .documentId(document.getDocumentId())
                    .title(document.getTitle())
                    .content(embeddingContent)
                    .context(piece.context())
                    .headingPath(piece.context())
                    .pageStart(piece.pageStart())
                    .pageEnd(piece.pageEnd())
                    .blockType(piece.type())
                    .metadata(piece.metadata())
                    .build());
            previous = piece;
        }
        return chunks;
    }

    private boolean shouldOverlap(Piece previous, Piece current) {
        return previous.context().equals(current.context())
                && previous.type() != BlockType.TABLE
                && current.type() != BlockType.TABLE
                && previous.type() != BlockType.CODE
                && current.type() != BlockType.CODE;
    }

    private String createOverlap(String content, int overlapSize) {
        if (content.length() <= overlapSize) {
            return content;
        }
        int start = content.length() - overlapSize;
        for (int index = start; index < content.length(); index++) {
            char current = content.charAt(index);
            if (current == '。' || current == '！' || current == '？' || current == '\n') {
                String overlap = content.substring(index + 1).trim();
                if (StringUtils.hasText(overlap)) {
                    return overlap;
                }
            }
        }
        return content.substring(start).trim();
    }

    private List<String> splitRecursively(String text, int separatorIndex, SplitOptions options) {
        String normalized = normalize(text);
        if (!StringUtils.hasText(normalized)) {
            return List.of();
        }
        if (normalized.length() <= options.getMaxSize()) {
            return List.of(normalized);
        }
        List<String> separators = resolvedSeparators(options);
        if (separatorIndex >= separators.size()) {
            return hardSplit(normalized, options.getMaxSize());
        }
        String separator = separators.get(separatorIndex);
        List<String> parts = splitAndKeepSeparator(normalized, separator);
        if (parts.size() <= 1) {
            return splitRecursively(normalized, separatorIndex + 1, options);
        }
        List<String> result = new ArrayList<>();
        for (String part : parts) {
            if (part.length() <= options.getMaxSize()) {
                result.add(part);
            } else {
                result.addAll(splitRecursively(part, separatorIndex + 1, options));
            }
        }
        return mergeTextParts(result, options);
    }

    private List<String> resolvedSeparators(SplitOptions options) {
        Set<String> separators = new LinkedHashSet<>();
        for (String separator : options.getSeparators()) {
            if (StringUtils.hasLength(separator)) {
                separators.add(separator);
            }
        }
        return List.copyOf(separators);
    }

    private List<String> splitAndKeepSeparator(String text, String separator) {
        List<String> result = new ArrayList<>();
        int start = 0;
        int index;
        while ((index = text.indexOf(separator, start)) >= 0) {
            int end = index + separator.length();
            String part = text.substring(start, end).trim();
            if (StringUtils.hasText(part)) {
                result.add(part);
            }
            start = end;
        }
        if (start < text.length()) {
            String remaining = text.substring(start).trim();
            if (StringUtils.hasText(remaining)) {
                result.add(remaining);
            }
        }
        return result;
    }

    private List<String> mergeTextParts(List<String> parts, SplitOptions options) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        for (String part : parts) {
            if (!current.isEmpty() && current.length() + part.length() > options.getTargetSize()) {
                result.add(current.toString().trim());
                current.setLength(0);
            }
            current.append(part);
        }
        if (!current.isEmpty()) {
            result.add(current.toString().trim());
        }
        if (result.size() > 1 && result.getLast().length() < options.getMinSize()) {
            String last = result.removeLast();
            String previous = result.getLast();
            if (previous.length() + last.length() <= options.getMaxSize()) {
                result.set(result.size() - 1, previous + last);
            } else {
                result.add(last);
            }
        }
        return result;
    }

    private List<String> hardSplit(String text, int maxSize) {
        List<String> result = new ArrayList<>();
        for (int start = 0; start < text.length(); start += maxSize) {
            result.add(text.substring(start, Math.min(text.length(), start + maxSize)).trim());
        }
        return result;
    }

    private String normalize(String text) {
        return text.replace("\r\n", "\n")
                .replace('\r', '\n')
                .replaceAll("[ \\t]+", " ")
                .replaceAll("\\n{3,}", "\n\n")
                .trim();
    }

    private Integer firstPage(Integer left, Integer right) {
        if (left == null) {
            return right;
        }
        return right == null ? left : Math.min(left, right);
    }

    private Integer lastPage(Integer left, Integer right) {
        if (left == null) {
            return right;
        }
        return right == null ? left : Math.max(left, right);
    }

    private void validate(SplitOptions options) {
        if (options.getMinSize() <= 0
                || options.getTargetSize() < options.getMinSize()
                || options.getMaxSize() < options.getTargetSize()
                || options.getOverlapSize() < 0
                || options.getOverlapSize() >= options.getMaxSize()) {
            throw new IllegalArgumentException("Invalid document split options");
        }
    }

    private record Piece(
            String content,
            String context,
            BlockType type,
            Integer pageStart,
            Integer pageEnd,
            Map<String, Object> metadata) {
    }
}
