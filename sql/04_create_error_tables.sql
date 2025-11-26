-- Error Tracking Tables

CREATE TABLE IF NOT EXISTS error_records
(
    ErrorID UInt64,
    ErrorDate DateTime,
    SourceTable String,
    RecordNaturalKey String,
    ErrorType String,
    ErrorSeverity String,
    ErrorMessage String,
    ErrorDetails String,
    FailedData String,
    ProcessingBatchID String,
    TaskName String,
    IsRecoverable UInt8,
    RetryCount UInt8 DEFAULT 0,
    LastAttemptDate DateTime,
    IsResolved UInt8 DEFAULT 0,
    ResolutionComment Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (ErrorDate, SourceTable, ErrorType)
PARTITION BY toYYYYMM(ErrorDate);

CREATE TABLE IF NOT EXISTS error_monitoring_summary
(
    SnapshotDate DateTime,
    ErrorType String,
    ErrorCount UInt64,
    RecoverableCount UInt64,
    CriticalCount UInt64
)
ENGINE = SummingMergeTree((ErrorCount, RecoverableCount, CriticalCount))
ORDER BY (SnapshotDate, ErrorType)
PARTITION BY toYYYYMM(SnapshotDate);


