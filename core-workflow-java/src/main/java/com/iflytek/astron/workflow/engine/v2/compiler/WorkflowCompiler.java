package com.iflytek.astron.workflow.engine.v2.compiler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.iflytek.astron.workflow.engine.v2.definition.Condition;
import com.iflytek.astron.workflow.engine.v2.definition.EdgeDefinition;
import com.iflytek.astron.workflow.engine.v2.definition.EdgeDefinition.EdgeType;
import com.iflytek.astron.workflow.engine.v2.definition.InputBinding;
import com.iflytek.astron.workflow.engine.v2.definition.LoopDefinition;
import com.iflytek.astron.workflow.engine.v2.definition.NodeDefinition;
import com.iflytek.astron.workflow.engine.v2.definition.NodeDefinition.Routing;
import com.iflytek.astron.workflow.engine.v2.definition.WorkflowDefinition;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowError;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowExecutionException;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;

public final class WorkflowCompiler {
    private final ObjectMapper mapper;
    private final Set<String> executorTypes;

    public WorkflowCompiler(ObjectMapper mapper, Set<String> executorTypes) {
        this.mapper = mapper.copy().enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
        this.executorTypes = Set.copyOf(executorTypes);
    }

    public CompiledWorkflow compile(WorkflowDefinition definition) {
        require(definition != null && "2".equals(definition.dslVersion()), "dslVersion must be 2");
        require(present(definition.definitionId()) && definition.version() > 0, "Definition ID/version required");
        require(!definition.nodes().isEmpty(), "Nodes required");
        require(definition.limits().maxNodeExecutions() > 0 && definition.limits().maxDurationMs() > 0
                && definition.limits().maxControlTransitions() > 0, "Execution limits must be positive");

        Map<String, NodeDefinition> nodes = new LinkedHashMap<>();
        Map<String, List<EdgeDefinition>> outgoing = new LinkedHashMap<>();
        Map<String, List<EdgeDefinition>> incoming = new HashMap<>();
        for (NodeDefinition node : definition.nodes()) {
            require(present(node.id()) && present(node.type()), "Node ID/type required");
            require(nodes.putIfAbsent(node.id(), node) == null, "Duplicate node: " + node.id());
            require(Set.of("START", "END", "LOOP", "XOR").contains(node.type())
                    || executorTypes.contains(node.type()), "Unsupported node type: " + node.type());
            require(node.retry().maxAttempts() > 0 && node.retry().delayMs() >= 0
                    && node.retry().timeoutMs() > 0, "Invalid retry policy: " + node.id());
            outgoing.put(node.id(), new ArrayList<>());
            incoming.put(node.id(), new ArrayList<>());
        }
        List<NodeDefinition> starts = nodes.values().stream().filter(n -> "START".equals(n.type())).toList();
        require(starts.size() == 1 && nodes.values().stream().filter(n -> "END".equals(n.type())).count() == 1,
                "Exactly one START and END required");

        Set<String> edgeIds = new HashSet<>();
        for (EdgeDefinition edge : definition.edges()) {
            require(present(edge.id()) && edgeIds.add(edge.id()), "Missing or duplicate edge ID");
            require(nodes.containsKey(edge.source()) && nodes.containsKey(edge.target()), "Unknown edge endpoint: " + edge.id());
            require(!(edge.defaultEdge() && edge.condition() != null), "Default edge cannot have a condition");
            if (edge.condition() != null) {
                validateCondition(edge.condition(), false, 0);
            }
            outgoing.get(edge.source()).add(edge);
            incoming.get(edge.target()).add(edge);
        }

        Map<String, LoopDefinition> loops = new LinkedHashMap<>();
        Map<String, String> owners = new HashMap<>();
        Set<String> loopIds = new HashSet<>();
        for (LoopDefinition loop : definition.loops()) {
            require(present(loop.loopId()) && loopIds.add(loop.loopId()), "Missing or duplicate loop ID");
            require(nodes.containsKey(loop.headerNodeId()) && "LOOP".equals(nodes.get(loop.headerNodeId()).type()),
                    "Loop header must reference LOOP");
            require(loops.putIfAbsent(loop.headerNodeId(), loop) == null, "Duplicate loop header");
            require(loop.maxIterations() > 0 && !loop.bodyNodeIds().isEmpty(), "Positive loop limit and body required");
            require(new HashSet<>(loop.bodyNodeIds()).size() == loop.bodyNodeIds().size(), "Duplicate loop body member");
            require(!loop.bodyNodeIds().contains(loop.headerNodeId()), "Header cannot be its own body member");
            for (String member : loop.bodyNodeIds()) {
                require(nodes.containsKey(member) && !Set.of("START", "END", "LOOP").contains(nodes.get(member).type()),
                        "Unsupported loop body member: " + member);
                require(owners.putIfAbsent(member, loop.loopId()) == null, "Overlapping/nested loops are not supported yet");
            }
            require(owners.putIfAbsent(loop.headerNodeId(), loop.loopId()) == null, "Overlapping loop header");
            validateCondition(loop.continueWhen(), true, 0);
        }

        for (NodeDefinition node : nodes.values()) {
            List<EdgeDefinition> outs = outgoing.get(node.id());
            if ("START".equals(node.type())) {
                require(incoming.get(node.id()).isEmpty(), "START cannot have incoming edges");
            }
            if ("END".equals(node.type())) {
                require(outs.isEmpty(), "END cannot have outgoing edges");
            } else if ("LOOP".equals(node.type())) {
                require(loops.containsKey(node.id()) && outs.size() == 2, "LOOP requires definition, body and exit edges");
                require(outs.stream().filter(e -> "body".equals(e.sourcePort())).count() == 1
                        && outs.stream().filter(e -> "exit".equals(e.sourcePort())).count() == 1, "Invalid LOOP ports");
                require(outs.stream().allMatch(e -> e.condition() == null && !e.defaultEdge()), "LOOP routes are controlled by continueWhen");
                require(incoming.get(node.id()).stream().anyMatch(e -> "enter".equals(e.targetPort())), "Loop entry required");
                require(incoming.get(node.id()).stream().anyMatch(e -> "continue".equals(e.targetPort())), "Loop continuation required");
            } else if (node.routing() == Routing.EXCLUSIVE || "XOR".equals(node.type())) {
                require(!outs.isEmpty() && outs.stream().filter(EdgeDefinition::defaultEdge).count() == 1,
                        "Exclusive routing requires exactly one default edge");
                require(outs.stream().allMatch(e -> e.defaultEdge() || e.condition() != null), "Conditional edge required");
            } else {
                require(outs.size() == 1 && outs.getFirst().condition() == null && !outs.getFirst().defaultEdge(),
                        "Sequential node requires exactly one unconditional edge: " + node.id());
            }
            validateBindings(node.inputBindings(), nodes, loopIds);
        }

        for (LoopDefinition loop : definition.loops()) {
            validateBindings(loop.initialState(), nodes, loopIds);
            validateBindings(loop.nextState(), nodes, loopIds);
            validateBindings(loop.outputBindings(), nodes, loopIds);
        }
        for (EdgeDefinition edge : definition.edges()) {
            validateBoundary(edge, owners, loops);
        }
        requireAcyclicWithoutBackEdges(nodes.keySet(), outgoing);
        String startId = starts.getFirst().id();
        require(reachable(startId, outgoing, false).size() == nodes.size(), "Unreachable nodes found");
        String endId = nodes.values().stream().filter(n -> "END".equals(n.type())).findFirst().orElseThrow().id();
        Map<String, List<EdgeDefinition>> reverse = new HashMap<>();
        nodes.keySet().forEach(id -> reverse.put(id, incoming.get(id)));
        require(reachable(endId, reverse, true).size() == nodes.size(), "Every node must have a structural path to END");

        Map<String, List<EdgeDefinition>> frozen = new LinkedHashMap<>();
        outgoing.forEach((key, edges) -> frozen.put(key, List.copyOf(edges)));
        return new CompiledWorkflow(definition, hash(definition), startId, nodes, frozen, loops, owners);
    }

    private void validateBoundary(EdgeDefinition edge, Map<String, String> owners,
                                  Map<String, LoopDefinition> loops) {
        String sourceOwner = owners.get(edge.source());
        String targetOwner = owners.get(edge.target());
        boolean sourceHeader = loops.containsKey(edge.source());
        boolean targetHeader = loops.containsKey(edge.target());
        if (edge.edgeType() == EdgeType.BACK) {
            scope(targetHeader && !sourceHeader && Objects.equals(sourceOwner, edge.loopId())
                    && Objects.equals(targetOwner, edge.loopId()) && "continue".equals(edge.targetPort()),
                    "Invalid back edge: " + edge.id());
        } else {
            scope(edge.loopId() == null && !"continue".equals(edge.targetPort()), "Undeclared back edge: " + edge.id());
            if (!Objects.equals(sourceOwner, targetOwner)) {
                boolean enters = targetHeader && "enter".equals(edge.targetPort());
                boolean exits = sourceHeader && "exit".equals(edge.sourcePort());
                scope((sourceOwner == null || exits) && (targetOwner == null || enters),
                        "Edge crosses loop boundary: " + edge.id());
            } else if (targetHeader) {
                scope(false, "Loop return must be a declared BACK edge: " + edge.id());
            }
        }
        scope(targetHeader || edge.targetPort() == null, "Only LOOP supports target ports");
        scope(sourceHeader || edge.sourcePort() == null, "Only LOOP supports source ports");
        if (targetHeader) {
            scope("enter".equals(edge.targetPort()) || "continue".equals(edge.targetPort()), "Unknown LOOP target port");
        }
        if (sourceHeader && "body".equals(edge.sourcePort())) {
            scope(Objects.equals(sourceOwner, targetOwner) && !targetHeader, "Body edge must enter its loop body");
        }
        if (sourceHeader && "exit".equals(edge.sourcePort())) {
            scope(targetOwner == null || targetHeader && "enter".equals(edge.targetPort())
                    && !Objects.equals(sourceOwner, targetOwner), "Loop exit must leave the region");
        }
    }

    private void validateBindings(Map<String, InputBinding> bindings, Map<String, NodeDefinition> nodes,
                                   Set<String> loopIds) {
        bindings.forEach((name, binding) -> {
            require(present(name) && binding.source() != null, "Invalid input binding");
            switch (binding.source()) {
                case NODE_OUTPUT -> {
                    require(nodes.containsKey(binding.nodeId()), "Unknown referenced node: " + binding.nodeId());
                    require(binding.scope() == null || Set.of("CURRENT", "CURRENT_ITERATION", "PARENT").contains(binding.scope()),
                            "Unknown binding scope");
                }
                case LOOP_OUTPUT -> require(loopIds.contains(binding.loopId()), "Unknown referenced loop");
                case LOOP_STATE -> require(binding.loopId() == null || loopIds.contains(binding.loopId()), "Unknown loop state");
                default -> {
                }
            }
        });
    }

    private void validateCondition(Condition condition, boolean loop, int depth) {
        require(condition != null && condition.operator() != null && depth <= 32, "Invalid/deep condition");
        switch (condition.operator()) {
            case AND, OR -> {
                require(condition.operands().size() >= 2, "Boolean combination requires operands");
                condition.operands().forEach(item -> validateCondition(item, loop, depth + 1));
            }
            case NOT -> {
                require(condition.operands().size() == 1, "NOT requires one operand");
                validateCondition(condition.operands().getFirst(), loop, depth + 1);
            }
            default -> require(condition.operands().isEmpty() && (loop ? present(condition.statePath()) && condition.output() == null
                    : present(condition.output()) && condition.statePath() == null), "Condition selector does not match context");
        }
    }

    private void requireAcyclicWithoutBackEdges(Set<String> nodes, Map<String, List<EdgeDefinition>> outgoing) {
        Map<String, Integer> degrees = new HashMap<>();
        nodes.forEach(id -> degrees.put(id, 0));
        outgoing.values().forEach(edges -> edges.stream().filter(e -> e.edgeType() != EdgeType.BACK)
                .forEach(e -> degrees.merge(e.target(), 1, Integer::sum)));
        Queue<String> ready = new ArrayDeque<>();
        degrees.forEach((id, degree) -> {
            if (degree == 0) {
                ready.add(id);
            }
        });
        int visited = 0;
        while (!ready.isEmpty()) {
            String node = ready.remove();
            visited++;
            outgoing.get(node).stream().filter(e -> e.edgeType() != EdgeType.BACK).forEach(edge -> {
                if (degrees.merge(edge.target(), -1, Integer::sum) == 0) {
                    ready.add(edge.target());
                }
            });
        }
        if (visited != nodes.size()) {
            throw new WorkflowExecutionException(WorkflowError.INVALID_CYCLE, "Cycle has no declared loop continuation");
        }
    }

    private Set<String> reachable(String start, Map<String, List<EdgeDefinition>> graph, boolean reverse) {
        Set<String> seen = new HashSet<>();
        Queue<String> ready = new ArrayDeque<>();
        ready.add(start);
        while (!ready.isEmpty()) {
            String current = ready.remove();
            if (seen.add(current)) {
                graph.get(current).forEach(edge -> ready.add(reverse ? edge.source() : edge.target()));
            }
        }
        return seen;
    }

    private String hash(WorkflowDefinition definition) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(mapper.writeValueAsBytes(definition)));
        } catch (JsonProcessingException | NoSuchAlgorithmException e) {
            throw new WorkflowExecutionException(WorkflowError.INVALID_DEFINITION, "Cannot hash workflow definition");
        }
    }

    private boolean present(String value) {
        return value != null && !value.isBlank();
    }

    private void require(boolean valid, String message) {
        if (!valid) {
            throw new WorkflowExecutionException(WorkflowError.INVALID_DEFINITION, message);
        }
    }

    private void scope(boolean valid, String message) {
        if (!valid) {
            throw new WorkflowExecutionException(WorkflowError.INVALID_SCOPE, message);
        }
    }
}
