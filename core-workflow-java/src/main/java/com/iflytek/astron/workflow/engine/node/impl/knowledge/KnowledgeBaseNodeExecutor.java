package com.iflytek.astron.workflow.engine.node.impl.knowledge;

import com.iflytek.astron.workflow.engine.constants.NodeExecStatusEnum;
import com.iflytek.astron.workflow.engine.constants.NodeTypeEnum;
import com.iflytek.astron.workflow.engine.domain.NodeRunResult;
import com.iflytek.astron.workflow.engine.domain.NodeState;
import com.iflytek.astron.workflow.engine.domain.chain.Node;
import com.iflytek.astron.workflow.engine.integration.pinecone.PineconeMatch;
import com.iflytek.astron.workflow.engine.integration.pinecone.PineconeSearchClient;
import com.iflytek.astron.workflow.engine.node.AbstractNodeExecutor;
import com.iflytek.astron.workflow.exception.ErrorCode;
import com.iflytek.astron.workflow.exception.NodeCustomException;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Component
public class KnowledgeBaseNodeExecutor extends AbstractNodeExecutor {
    private static final String CONTEXT = "context";
    private static final String SOURCES = "sources";
    private static final String RESULTS = "results";

    private final PineconeSearchClient pineconeSearchClient;

    public KnowledgeBaseNodeExecutor(PineconeSearchClient pineconeSearchClient) {
        this.pineconeSearchClient = pineconeSearchClient;
    }

    @Override
    public NodeTypeEnum getNodeType() {
        return NodeTypeEnum.KNOWLEDGE_BASE;
    }

    @Override
    protected NodeRunResult executeNode(NodeState nodeState, Map<String, Object> inputs) {
        Node node = nodeState.node();
        Map<String, Object> nodeParam = node.getData().getNodeParam();
        String query = resolveQuery(inputs);
        List<String> repoIds = stringList(nodeParam.get("repoId"));
        if (repoIds.isEmpty()) {
            throw new NodeCustomException(ErrorCode.INVALID_NODE_CONFIGURATION, "Knowledge node requires repoId");
        }

        List<String> documentIds = stringList(nodeParam.get("docIds"));
        int topN = positiveInt(nodeParam.get("topN"), 5, "topN");
        double scoreThreshold = nonNegativeDouble(nodeParam.get("score"), 0D, "score");
        int maxContextChars = positiveInt(nodeParam.get("maxContextChars"), 12000, "maxContextChars");
        String namespace = stringValue(nodeParam.get("namespace"));

        List<PineconeMatch> matches = pineconeSearchClient.search(query, namespace, topN, buildFilter(repoIds, documentIds));
        List<PineconeMatch> acceptedMatches = matches.stream()
                .filter(match -> match.score() >= scoreThreshold)
                .sorted(Comparator.comparingDouble(PineconeMatch::score).reversed())
                .limit(topN)
                .toList();

        Map<String, Object> outputs = buildOutputs(acceptedMatches, maxContextChars);
        NodeRunResult result = new NodeRunResult();
        result.setInputs(inputs);
        result.setOutputs(outputs);
        result.setStatus(NodeExecStatusEnum.SUCCESS);
        return result;
    }

    private String resolveQuery(Map<String, Object> inputs) {
        Object query = inputs.get("query");
        if (query == null && !inputs.isEmpty()) {
            query = inputs.values().iterator().next();
        }
        if (query == null || !StringUtils.hasText(String.valueOf(query))) {
            throw new NodeCustomException(ErrorCode.PARAM_ERROR, "Knowledge node query cannot be empty");
        }
        return String.valueOf(query);
    }

    private Map<String, Object> buildFilter(List<String> repoIds, List<String> documentIds) {
        List<Map<String, Object>> filters = new ArrayList<>();
        filters.add(Map.of("repo_id", Map.of("$in", repoIds)));
        filters.add(Map.of("enabled", Map.of("$eq", true)));
        if (!documentIds.isEmpty()) {
            filters.add(Map.of("document_id", Map.of("$in", documentIds)));
        }
        return filters.size() == 1 ? filters.getFirst() : Map.of("$and", filters);
    }

    private Map<String, Object> buildOutputs(List<PineconeMatch> matches, int maxContextChars) {
        List<Map<String, Object>> sources = new ArrayList<>();
        List<Map<String, Object>> results = new ArrayList<>();
        StringBuilder context = new StringBuilder();
        int sourceNo = 1;
        for (PineconeMatch match : matches) {
            String content = match.content();
            if (!StringUtils.hasText(content)) {
                continue;
            }
            String sourcePrefix = sourceLabel(sourceNo, match.metadata());
            int remaining = maxContextChars - context.length() - sourcePrefix.length() - 2;
            if (remaining <= 0) {
                break;
            }
            String contextContent = content.length() > remaining ? content.substring(0, remaining) : content;
            if (!context.isEmpty()) {
                context.append("\n\n");
            }
            context.append(sourcePrefix).append('\n').append(contextContent);

            Map<String, Object> source = new LinkedHashMap<>();
            source.put("sourceNo", sourceNo);
            source.put("recordId", match.id());
            source.put("content", content);
            source.put("score", match.score());
            source.put("repoId", match.metadata().get("repo_id"));
            source.put("documentId", match.metadata().get("document_id"));
            source.put("fileName", match.metadata().get("file_name"));
            source.put("page", match.metadata().get("page"));
            source.put("chunkIndex", match.metadata().get("chunk_index"));
            source.put("sourceUri", match.metadata().get("source_uri"));
            sources.add(source);

            Map<String, Object> raw = new LinkedHashMap<>();
            raw.put("id", match.id());
            raw.put("content", content);
            raw.put("score", match.score());
            raw.put("metadata", match.metadata());
            results.add(raw);
            sourceNo++;
        }
        Map<String, Object> outputs = new LinkedHashMap<>();
        outputs.put(CONTEXT, context.toString());
        outputs.put(SOURCES, sources);
        outputs.put(RESULTS, results);
        return outputs;
    }

    private String sourceLabel(int sourceNo, Map<String, Object> metadata) {
        String fileName = stringValue(metadata.get("file_name"));
        Object page = metadata.get("page");
        StringBuilder label = new StringBuilder("[来源 ").append(sourceNo);
        if (StringUtils.hasText(fileName)) {
            label.append("：").append(fileName);
        }
        if (page != null) {
            label.append("，第 ").append(page).append(" 页");
        }
        return label.append(']').toString();
    }

    private List<String> stringList(Object value) {
        if (value == null) {
            return List.of();
        }
        if (value instanceof Collection<?> collection) {
            return collection.stream().filter(Objects::nonNull).map(String::valueOf).filter(StringUtils::hasText).toList();
        }
        String single = String.valueOf(value);
        return StringUtils.hasText(single) ? List.of(single) : List.of();
    }

    private int positiveInt(Object value, int defaultValue, String field) {
        if (value == null) {
            return defaultValue;
        }
        try {
            int parsed = Integer.parseInt(String.valueOf(value));
            if (parsed > 0) {
                return parsed;
            }
        } catch (NumberFormatException ignored) {
        }
        throw new NodeCustomException(ErrorCode.PARAM_ERROR, field + " must be a positive integer");
    }

    private double nonNegativeDouble(Object value, double defaultValue, String field) {
        if (value == null) {
            return defaultValue;
        }
        try {
            double parsed = Double.parseDouble(String.valueOf(value));
            if (parsed >= 0) {
                return parsed;
            }
        } catch (NumberFormatException ignored) {
        }
        throw new NodeCustomException(ErrorCode.PARAM_ERROR, field + " must be non-negative");
    }

    private String stringValue(Object value) {
        return value == null ? null : String.valueOf(value);
    }
}


