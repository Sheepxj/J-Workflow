package com.iflytek.astron.workflow.engine.execution;

import com.iflytek.astron.workflow.engine.execution.WorkflowGraphRun.Token;
import com.iflytek.astron.workflow.exception.ErrorCode;
import com.iflytek.astron.workflow.exception.NodeCustomException;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntPredicate;

final class WorkflowTokenScheduler {
    private final WorkflowGraph graph;
    private final Map<String, Traversal> traversals = new HashMap<>();
    private final Map<String, List<Token>> arrivals = new HashMap<>();

    WorkflowTokenScheduler(WorkflowGraph graph) {
        this.graph = graph;
    }

    void register(Token token) {
        traversals.put(token.id(), new Traversal(List.of(), false));
    }

    void registerSuccessor(Token token, Token source) {
        List<String> path = new ArrayList<>(traversals.get(source.id()).path());
        path.add(source.targetNodeId());
        int repeated = path.indexOf(token.targetNodeId());
        traversals.put(token.id(), new Traversal(repeated < 0 ? List.copyOf(path)
                : List.copyOf(path.subList(0, repeated)), repeated >= 0));
    }

    List<Token> arrivals(String activationId) {
        return arrivals.getOrDefault(activationId, List.of());
    }

    Selection select(List<Token> pending) {
        Map<String, List<Token>> grouped = new LinkedHashMap<>();
        Set<String> unique = new HashSet<>();
        for (Token token : pending) {
            if (unique.add(token.id())) {
                grouped.computeIfAbsent(token.targetNodeId(), ignored -> new ArrayList<>()).add(token);
            }
        }
        List<Group> groups = grouped.entrySet().stream().map(entry -> new Group(entry.getKey(), List.copyOf(entry.getValue()))).toList();
        Group selected = groups.size() == 1 ? groups.getFirst() : choose(groups);
        Token representative = selected.tokens().getFirst();
        Set<String> path = new LinkedHashSet<>();
        selected.tokens().forEach(token -> path.addAll(traversals.get(token.id()).path()));
        traversals.put(representative.id(), new Traversal(List.copyOf(path), false));
        arrivals.put(representative.id(), selected.tokens());
        return new Selection(representative, selected.tokens());
    }

    private Group choose(List<Group> groups) {
        BitSet[] blockers = new BitSet[groups.size()];
        for (int candidate = 0; candidate < groups.size(); candidate++) {
            blockers[candidate] = new BitSet(groups.size());
            for (int upstream = 0; upstream < groups.size(); upstream++) {
                if (candidate != upstream && blocks(groups.get(upstream), groups.get(candidate))) {
                    blockers[candidate].set(upstream);
                }
            }
        }
        int ready = first(groups, index -> blockers[index].isEmpty());
        if (ready >= 0) {
            return groups.get(ready);
        }
        BitSet[] dependencies = new BitSet[groups.size()];
        for (int index = 0; index < groups.size(); index++) {
            dependencies[index] = (BitSet) blockers[index].clone();
        }
        for (int intermediate = 0; intermediate < groups.size(); intermediate++) {
            for (int index = 0; index < groups.size(); index++) {
                if (dependencies[index].get(intermediate)) {
                    dependencies[index].or(dependencies[intermediate]);
                }
            }
        }
        int cyclic = first(groups, index -> {
            for (int upstream = dependencies[index].nextSetBit(0); upstream >= 0;
                 upstream = dependencies[index].nextSetBit(upstream + 1)) {
                if (!dependencies[upstream].get(index)) {
                    return false;
                }
            }
            return true;
        });
        if (cyclic < 0) {
            throw new NodeCustomException(ErrorCode.WORKFLOW_ROUTING_ERROR, "Cannot resolve pending control dependencies");
        }
        return groups.get(cyclic);
    }

    private int first(List<Group> groups, IntPredicate eligible) {
        int feedback = -1;
        for (int index = 0; index < groups.size(); index++) {
            if (!eligible.test(index)) {
                continue;
            }
            if (groups.get(index).tokens().stream().anyMatch(token -> !traversals.get(token.id()).feedback())) {
                return index;
            }
            if (feedback < 0) {
                feedback = index;
            }
        }
        return feedback;
    }

    private boolean blocks(Group upstream, Group candidate) {
        if (candidate.nodeId().equals(graph.endNodeId())) {
            return true;
        }
        for (Token token : upstream.tokens()) {
            Traversal traversal = traversals.get(token.id());
            if (traversal.feedback() && candidate.tokens().stream()
                    .anyMatch(arrival -> traversals.get(arrival.id()).path().contains(upstream.nodeId()))) {
                continue;
            }
            if (canReach(upstream.nodeId(), candidate.nodeId(), traversal.path())) {
                return true;
            }
        }
        return false;
    }

    private boolean canReach(String source, String target, List<String> path) {
        Set<String> visited = new HashSet<>(path);
        ArrayDeque<String> pending = new ArrayDeque<>();
        pending.add(source);
        while (!pending.isEmpty()) {
            String current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            if (current.equals(target)) {
                return true;
            }
            graph.outgoing(current).forEach(edge -> pending.addLast(edge.target()));
        }
        return false;
    }

    record Selection(Token token, List<Token> arrivals) {
    }

    private record Group(String nodeId, List<Token> tokens) {
    }

    private record Traversal(List<String> path, boolean feedback) {
    }

}
