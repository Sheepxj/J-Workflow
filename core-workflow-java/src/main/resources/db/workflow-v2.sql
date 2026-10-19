CREATE TABLE IF NOT EXISTS workflow_v2_run (
    run_id VARCHAR(64) PRIMARY KEY,
    status VARCHAR(32) NOT NULL,
    revision BIGINT NOT NULL,
    state_json LONGTEXT NOT NULL
);
