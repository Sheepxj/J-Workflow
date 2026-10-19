package com.iflytek.astron.workflow.engine.v2.runtime;

public final class RuntimeStatus {
    private RuntimeStatus() {
    }

    public enum RunStatus {
        RUNNING, PAUSING, PAUSED, SUCCEEDED, FAILED, CANCELLED;

        public boolean terminal() {
            return this == SUCCEEDED || this == FAILED || this == CANCELLED;
        }
    }

    public enum ExecutionStatus {
        READY, RUNNING, RETRY_WAIT, RECOVERY_REQUIRED, SUCCEEDED, FAILED, CANCELLED;

        public boolean terminal() {
            return this == SUCCEEDED || this == FAILED || this == CANCELLED;
        }
    }

    public enum AttemptStatus {
        READY, RUNNING, SUCCEEDED, FAILED, TIMED_OUT, INTERRUPTED, CANCELLED
    }

    public enum ScopeType {
        ROOT, LOOP, ITERATION, PARALLEL, BRANCH
    }
}
