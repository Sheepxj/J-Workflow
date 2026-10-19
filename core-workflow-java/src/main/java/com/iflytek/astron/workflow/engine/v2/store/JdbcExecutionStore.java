package com.iflytek.astron.workflow.engine.v2.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowError;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowExecutionException;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowRun;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public final class JdbcExecutionStore implements ExecutionStore {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final ObjectMapper mapper;

    public JdbcExecutionStore(DataSource dataSource, ObjectMapper mapper, boolean initializeSchema) {
        this.jdbc = new JdbcTemplate(dataSource);
        this.transactions = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        this.mapper = mapper.copy();
        if (initializeSchema) {
            jdbc.execute("""
                    CREATE TABLE IF NOT EXISTS workflow_v2_run (
                        run_id VARCHAR(64) PRIMARY KEY,
                        status VARCHAR(32) NOT NULL,
                        revision BIGINT NOT NULL,
                        state_json LONGTEXT NOT NULL
                    )
                    """);
        }
    }

    @Override
    public void create(WorkflowRun run) {
        try {
            jdbc.update("INSERT INTO workflow_v2_run (run_id, status, revision, state_json) VALUES (?, ?, ?, ?)",
                    run.getRunId(), run.getStatus().name(), run.getRevision(), encode(run));
        } catch (DuplicateKeyException e) {
            throw new WorkflowExecutionException(WorkflowError.INVALID_RUN_STATE, "Run already exists");
        }
    }

    @Override
    public WorkflowRun get(String runId) {
        return read(runId, false);
    }

    @Override
    public void update(String runId, Consumer<WorkflowRun> transition) {
        transactions.executeWithoutResult(transaction -> {
            WorkflowRun run = read(runId, true);
            long previous = run.getRevision();
            transition.accept(run);
            run.setRevision(previous + 1);
            int changed = jdbc.update("UPDATE workflow_v2_run SET status=?, revision=?, state_json=? WHERE run_id=? AND revision=?",
                    run.getStatus().name(), run.getRevision(), encode(run), runId, previous);
            if (changed != 1) {
                throw new WorkflowExecutionException(WorkflowError.STALE_REVISION, "Concurrent run update");
            }
        });
    }

    @Override
    public Set<String> activeRunIds() {
        return new HashSet<>(jdbc.queryForList(
                "SELECT run_id FROM workflow_v2_run WHERE status IN ('RUNNING', 'PAUSING')", String.class));
    }

    private WorkflowRun read(String runId, boolean lock) {
        List<String> rows = jdbc.queryForList("SELECT state_json FROM workflow_v2_run WHERE run_id=?"
                + (lock ? " FOR UPDATE" : ""), String.class, runId);
        if (rows.isEmpty()) {
            throw new WorkflowExecutionException(WorkflowError.RUN_NOT_FOUND, "Run not found: " + runId);
        }
        try {
            return mapper.readValue(rows.getFirst(), WorkflowRun.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Stored workflow state cannot be decoded", e);
        }
    }

    private String encode(WorkflowRun run) {
        try {
            return mapper.writeValueAsString(run);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Workflow state cannot be encoded", e);
        }
    }
}
