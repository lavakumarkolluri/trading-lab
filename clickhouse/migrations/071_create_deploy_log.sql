-- deploy_log: full append history of every test run / auto-deploy event.
-- Unlike ci_results (ReplacingMergeTree keyed by branch), this keeps every row
-- so the dashboard can show the last N propagations from stage→prod.
CREATE TABLE IF NOT EXISTS system_meta.deploy_log (
    branch        String,
    commit_sha    String,
    commit_msg    String,
    commit_author String,
    commit_ts     DateTime,
    tests_passed  UInt32,
    tests_failed  UInt32,
    tests_total   UInt32,
    duration_s    Float32,
    failed_tests  String DEFAULT '',
    status        LowCardinality(String),
    run_at        DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (branch, run_at);
