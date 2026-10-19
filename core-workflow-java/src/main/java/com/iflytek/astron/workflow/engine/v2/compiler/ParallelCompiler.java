package com.iflytek.astron.workflow.engine.v2.compiler;

import com.iflytek.astron.workflow.engine.v2.definition.EdgeDefinition;
import com.iflytek.astron.workflow.engine.v2.definition.EdgeDefinition.EdgeType;
import com.iflytek.astron.workflow.engine.v2.definition.EdgeDefinition.Outcome;
import com.iflytek.astron.workflow.engine.v2.definition.NodeDefinition;
import com.iflytek.astron.workflow.engine.v2.definition.ParallelDefinition;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowError;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowExecutionException;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;

final class ParallelCompiler {
    Map<String, ParallelDefinition> compile(List<ParallelDefinition> definitions, Map<String, NodeDefinition> nodes,
                                             Map<String, List<EdgeDefinition>> outgoing,
                                             Map<String, List<EdgeDefinition>> incoming, Map<String, String> loopOwners) {
        Map<String, ParallelDefinition> forks = new LinkedHashMap<>();
        Set<String> joins = new HashSet<>();
        Set<String> allBranchMembers = new HashSet<>();
        for (ParallelDefinition definition : definitions) {
            require(nodes.containsKey(definition.forkNodeId()) && "FORK".equals(nodes.get(definition.forkNodeId()).type()),
                    "Parallel fork must reference FORK");
            require(nodes.containsKey(definition.joinNodeId()) && "JOIN".equals(nodes.get(definition.joinNodeId()).type()),
                    "Parallel join must reference JOIN");
            require(forks.putIfAbsent(definition.forkNodeId(), definition) == null && joins.add(definition.joinNodeId()),
                    "FORK and JOIN must be uniquely paired");
            require(Objects.equals(loopOwners.get(definition.forkNodeId()), loopOwners.get(definition.joinNodeId())),
                    "FORK and JOIN must belong to the same loop region");
            require(definition.branches().size() >= 2, "Parallel requires at least two branches");
            List<EdgeDefinition> starts = outgoing.get(definition.forkNodeId());
            require(starts.size() == definition.branches().size(), "FORK edges must match its branches");
            Set<String> ports = new HashSet<>();
            for (EdgeDefinition edge : starts) {
                require(edge.sourcePort() != null && ports.add(edge.sourcePort()) && edge.condition() == null
                        && !edge.defaultEdge() && edge.edgeType() == EdgeType.NORMAL
                        && edge.on() == Outcome.SUCCESS
                        && edge.target().equals(definition.branches().get(edge.sourcePort())), "Invalid FORK branch edge");
            }
            Map<String, String> members = new HashMap<>();
            definition.branches().forEach((branchId, entry) -> {
                require(branchId.matches("[A-Za-z_][A-Za-z0-9_-]*") && nodes.containsKey(entry), "Invalid branch ID or entry");
                Set<String> branch = collect(entry, definition.joinNodeId(), loopOwners.get(definition.forkNodeId()),
                        nodes, outgoing, loopOwners);
                branch.forEach(member -> {
                    require(members.putIfAbsent(member, branchId) == null && allBranchMembers.add(member),
                            "Parallel branches overlap or contain another parallel region");
                });
            });
            members.forEach((member, branchId) -> {
                for (EdgeDefinition edge : incoming.get(member)) {
                    if ("LOOP".equals(nodes.get(member).type()) && "continue".equals(edge.targetPort())) {
                        continue;
                    }
                    boolean fromBranch = branchId.equals(members.get(edge.source()));
                    boolean fromFork = edge.source().equals(definition.forkNodeId())
                            && branchId.equals(edge.sourcePort()) && member.equals(definition.branches().get(branchId));
                    require(fromBranch || fromFork, "External entry into parallel branch: " + member);
                }
            });
            for (EdgeDefinition edge : incoming.get(definition.joinNodeId())) {
                boolean emptyBranch = edge.source().equals(definition.forkNodeId())
                        && definition.joinNodeId().equals(definition.branches().get(edge.sourcePort()));
                require(members.containsKey(edge.source()) || emptyBranch, "JOIN has an unrelated incoming edge");
            }
        }
        nodes.values().forEach(node -> {
            if ("FORK".equals(node.type())) {
                require(forks.containsKey(node.id()), "Unpaired FORK: " + node.id());
            }
            if ("JOIN".equals(node.type())) {
                require(joins.contains(node.id()), "Unpaired JOIN: " + node.id());
            }
        });
        return forks;
    }

    private Set<String> collect(String entry, String join, String loopOwner, Map<String, NodeDefinition> nodes,
                                 Map<String, List<EdgeDefinition>> outgoing, Map<String, String> loopOwners) {
        Set<String> visited = new HashSet<>();
        Queue<String> queue = new ArrayDeque<>();
        queue.add(entry);
        while (!queue.isEmpty()) {
            String current = queue.remove();
            if (current.equals(join) || !visited.add(current)) {
                continue;
            }
            NodeDefinition node = nodes.get(current);
            require(!Set.of("START", "END", "FORK", "JOIN").contains(node.type()),
                    "Parallel branch must reach its paired JOIN without crossing another gateway");
            require(Objects.equals(loopOwner, loopOwners.get(current)), "Parallel branch crosses a loop boundary");
            List<EdgeDefinition> edges = outgoing.get(current).stream()
                    .filter(edge -> !"LOOP".equals(node.type()) || "exit".equals(edge.sourcePort())).toList();
            require(!edges.isEmpty() && edges.stream().allMatch(edge -> edge.edgeType() == EdgeType.NORMAL),
                    "Parallel branch must finish at its paired JOIN");
            edges.forEach(edge -> queue.add(edge.target()));
        }
        return visited;
    }

    private void require(boolean valid, String message) {
        if (!valid) {
            throw new WorkflowExecutionException(WorkflowError.INVALID_SCOPE, message);
        }
    }
}
