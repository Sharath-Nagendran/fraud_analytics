-- ClickHouse-oriented starter DDL. Adjust storage paths and engine settings for your environment.
CREATE TABLE fraud_demo.transactions (
 transaction_id String, transaction_ts DateTime, customer_id String, account_id String, channel LowCardinality(String), transaction_type LowCardinality(String), amount_inr Decimal(18,2), currency FixedString(3), merchant_id String, merchant_category LowCardinality(String), city LowCardinality(String), state LowCardinality(String), latitude Float64, longitude Float64, device_id String, device_trust_status LowCardinality(String), ip_address String, txn_count_1h UInt16, unusual_hour_flag UInt8, distance_from_home_km UInt16, amount_deviation_score Float32, fraud_risk_score Float32, decision LowCardinality(String), transaction_status LowCardinality(String), is_fraud UInt8, fraud_type LowCardinality(String), rrn String
) ENGINE=MergeTree PARTITION BY toYYYYMM(transaction_ts) ORDER BY (toDate(transaction_ts), customer_id, transaction_ts);

CREATE VIEW fraud_demo.vw_fraud_detection_performance AS
SELECT toDate(transaction_ts) business_date, channel, fraud_type, decision,
 count() transaction_count, sum(amount_inr) transaction_amount_inr, sum(is_fraud) fraud_count,
 sumIf(amount_inr,is_fraud=1) fraud_amount_inr, round(100.0*sum(is_fraud)/count(),3) fraud_rate_pct, avg(fraud_risk_score) avg_risk_score
FROM fraud_demo.transactions GROUP BY business_date, channel, fraud_type, decision;

CREATE VIEW fraud_demo.vw_confusion_matrix AS
SELECT
 sum(is_fraud=1 AND decision IN ('DECLINE','CHALLENGE')) true_positive,
 sum(is_fraud=0 AND decision IN ('DECLINE','CHALLENGE')) false_positive,
 sum(is_fraud=0 AND decision='APPROVE') true_negative,
 sum(is_fraud=1 AND decision='APPROVE') false_negative
FROM fraud_demo.transactions;
