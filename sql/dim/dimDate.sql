CREATE TABLE dim_date
(
    DateKey UInt32,
    FullDate Date,
    Year UInt16,
    Month UInt8,
    Day UInt8,
    DayOfWeek UInt8,
    MonthName String,
    DayName String
)
ENGINE = MergeTree()
ORDER BY DateKey;
