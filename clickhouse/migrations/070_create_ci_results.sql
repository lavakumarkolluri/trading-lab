-- CI test results written by scheduler after each auto-deploy.
-- One row per branch (ReplacingMergeTree deduplicates on branch).
-- Dashboard reads this to show git status and test pass/fail.

CREATE TABLE IF NOT EXISTS system_meta.ci_results
(
    branch          String,
    commit_sha      String,
    commit_msg      String,
    commit_author   String,
    commit_ts       DateTime,
    tests_passed    UInt32,
    tests_failed    UInt32,
    tests_total     UInt32,
    duration_s      Float32,
    failed_tests    String   DEFAULT '',   -- newline-separated names of failed tests
    status          LowCardinality(String), -- 'pass' | 'fail' | 'error'
    run_at          DateTime DEFAULT now(),
    version         UInt64   DEFAULT toUnixTimestamp(now())
)
ENGINE = ReplacingMergeTree(version)
ORDER BY branch;
