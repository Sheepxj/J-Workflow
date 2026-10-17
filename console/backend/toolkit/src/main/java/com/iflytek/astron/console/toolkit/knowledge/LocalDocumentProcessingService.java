package com.iflytek.astron.console.toolkit.knowledge;

import com.alibaba.fastjson2.JSONObject;
import com.iflytek.astron.console.commons.constant.ResponseEnum;
import com.iflytek.astron.console.commons.exception.BusinessException;
import com.iflytek.astron.console.toolkit.entity.core.knowledge.ChunkInfo;
import com.iflytek.astron.console.toolkit.entity.pojo.SliceConfig;
import com.iflytek.astron.console.toolkit.entity.table.repo.FileInfoV2;
import com.iflytek.astron.console.toolkit.knowledge.model.DocumentChunk;
import com.iflytek.astron.console.toolkit.knowledge.model.ParsedDocument;
import com.iflytek.astron.console.toolkit.knowledge.parser.StructuredDocumentParser;
import com.iflytek.astron.console.toolkit.knowledge.splitter.SplitOptions;
import com.iflytek.astron.console.toolkit.knowledge.splitter.StructureAwareDocumentSplitter;
import com.iflytek.astron.console.toolkit.util.S3Util;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

@Slf4j
@Service
public class LocalDocumentProcessingService {
    private static final List<String> DEFAULT_SEPARATORS = List.of(
            "\n\n", "\n", "。", "！", "？", "；", ". ", "! ", "? ", "; ", "，", ",", " ");

    private final S3Util s3Util;
    private final StructuredDocumentParser parser;
    private final StructureAwareDocumentSplitter splitter;
    private final LocalDocumentProcessingProperties properties;

    public LocalDocumentProcessingService(
            S3Util s3Util,
            StructuredDocumentParser parser,
            StructureAwareDocumentSplitter splitter,
            LocalDocumentProcessingProperties properties) {
        this.s3Util = s3Util;
        this.parser = parser;
        this.splitter = splitter;
        this.properties = properties;
    }

    public List<ChunkInfo> process(FileInfoV2 fileInfo, SliceConfig sliceConfig) {
        try (InputStream inputStream = s3Util.getObject(fileInfo.getAddress())) {
            if (inputStream == null) {
                throw new BusinessException(ResponseEnum.REPO_FILE_DOWNLOAD_FAILED);
            }
            ParsedDocument document = parser.parse(
                    fileInfo.getUuid(),
                    fileInfo.getName(),
                    fileInfo.getType(),
                    inputStream);
            if (CollectionUtils.isEmpty(document.getBlocks())) {
                throw new BusinessException(ResponseEnum.REPO_FILE_SLICE_FAILED);
            }
            List<DocumentChunk> chunks = splitter.split(document, resolveOptions(sliceConfig));
            if (chunks.isEmpty()) {
                throw new BusinessException(ResponseEnum.REPO_FILE_SLICE_FAILED);
            }
            return chunks.stream().map(this::toChunkInfo).toList();
        } catch (BusinessException e) {
            throw e;
        } catch (Exception e) {
            log.error("Local document processing failed, fileId={}, type={}", fileInfo.getId(), fileInfo.getType(), e);
            throw new BusinessException(ResponseEnum.REPO_FILE_SLICE_FAILED);
        }
    }

    public boolean supports(FileInfoV2 fileInfo) {
        String fileType = fileInfo.getType() == null ? "" : fileInfo.getType().toLowerCase(Locale.ROOT);
        if ("html".equals(fileType) && fileInfo.getAddress() != null
                && (fileInfo.getAddress().startsWith("http://") || fileInfo.getAddress().startsWith("https://"))) {
            return false;
        }
        return Set.of("txt", "md", "markdown", "html", "htm", "pdf", "doc", "docx", "xls", "xlsx", "csv", "ppt", "pptx")
                .contains(fileType);
    }

    private SplitOptions resolveOptions(SliceConfig sliceConfig) {
        int minSize = properties.getMinSize();
        int maxSize = properties.getMaxSize();
        if (sliceConfig != null && sliceConfig.getLengthRange() != null && sliceConfig.getLengthRange().size() >= 2) {
            Integer configuredMin = sliceConfig.getLengthRange().get(0);
            Integer configuredMax = sliceConfig.getLengthRange().get(1);
            if (configuredMin != null && configuredMin > 0) {
                minSize = configuredMin;
            }
            if (configuredMax != null && configuredMax >= minSize) {
                maxSize = configuredMax;
            }
        }
        int targetSize = Math.max(minSize, Math.min(properties.getTargetSize(), maxSize));
        int overlapSize = Math.min(properties.getOverlapSize(), Math.max(0, Math.min(maxSize - 1, targetSize / 4)));
        return SplitOptions.builder()
                .minSize(minSize)
                .targetSize(targetSize)
                .maxSize(maxSize)
                .overlapSize(overlapSize)
                .separators(resolveSeparators(sliceConfig))
                .build();
    }

    private List<String> resolveSeparators(SliceConfig sliceConfig) {
        Set<String> separators = new LinkedHashSet<>();
        if (sliceConfig != null && sliceConfig.getSeperator() != null) {
            sliceConfig.getSeperator().stream().filter(StringUtils::hasLength).forEach(separators::add);
        }
        separators.addAll(DEFAULT_SEPARATORS);
        return new ArrayList<>(separators);
    }

    private ChunkInfo toChunkInfo(DocumentChunk chunk) {
        JSONObject references = new JSONObject();
        references.put("page", chunk.getPageStart());
        references.put("pageStart", chunk.getPageStart());
        references.put("pageEnd", chunk.getPageEnd());
        references.put("headingPath", chunk.getHeadingPath());
        references.put("chunkIndex", chunk.getChunkIndex());
        references.put("blockType", chunk.getBlockType().name());
        references.putAll(chunk.getMetadata());

        ChunkInfo result = new ChunkInfo();
        result.setDocId(chunk.getDocumentId());
        result.setDataIndex(chunk.getChunkId());
        result.setTitle(chunk.getTitle());
        result.setContent(chunk.getContent());
        result.setContext(chunk.getContext());
        result.setReferences(references);
        return result;
    }
}
