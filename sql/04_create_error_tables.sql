
-- ===================================================================
-- 04_create_error_tables.sql
-- Error tracking + monitoring
-- ===================================================================

CREATE TABLE error_records
(
    ErrorID               UInt64,
    ErrorDate             DateTime64(3) DEFAULT now64(),
    SourceTable           LowCardinality(String),
    RecordNaturalKey      String,
    ErrorType             LowCardinality(String),
    ErrorSeverity         LowCardinality(String), -- Info / Warning / Critical
    ErrorMessage          String,
    ErrorDetails          String,
    FailedData            String,                 -- JSON
    ProcessingBatchID     String,
    TaskName              String,
    IsRecoverable         UInt8 DEFAULT 1,
    RetryCount            UInt8 DEFAULT 0,
    LastAttemptDate       DateTime64(3),
    IsResolved            UInt8 DEFAULT 0,
    ResolutionComment     Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (ErrorDate, SourceTable, ErrorType, IsResolved)
PARTITION BY toYYYYMM(ErrorDate);

-- Simple monitoring table
CREATE TABLE etl_run_log
(
    RunID                 UUID DEFAULT generateUUIDv4(),
    RunDate               DateTime DEFAULT now(),
    DAGName               String,
    Status                LowCardinality(String), -- SUCCESS / FAILED / RUNNING
    RecordsProcessed      UInt64,
    RecordsFailed         UInt64,
    StartTime             DateTime,
    EndTime               Nullable(DateTime),
    DurationSeconds       Nullable(UInt32)
)
ENGINE = MergeTree()
ORDER BY (RunDate, DAGName)
PARTITION BY toYYYYMM(RunDate);
