-- Customer-level feature mart (last 30 days) + churn label
-- churn_30d: no SUCCESS txn AND no successful session in last 30 days

CREATE OR REPLACE VIEW customer_feature_mart_30d AS
WITH params AS (
    SELECT (MAX(txn_time))::timestamp AS as_of
    FROM transactions
),
txn_30d AS (
    SELECT
        a.customer_id,
        COUNT(*) FILTER (WHERE t.status='SUCCESS') AS txn_success_30d,
        COUNT(*) FILTER (WHERE t.status='FAILED') AS txn_failed_30d,
        COUNT(*) FILTER (WHERE t.status='PENDING') AS txn_pending_30d,
        COALESCE(SUM(t.amount) FILTER (WHERE t.status='SUCCESS'),0) AS amt_success_30d,
        COALESCE(AVG(t.amount) FILTER (WHERE t.status='SUCCESS'),0) AS avg_amt_success_30d,
        MAX(t.txn_time) FILTER (WHERE t.status='SUCCESS') AS last_success_txn_time
    FROM transactions t
    JOIN accounts a ON a.account_id = t.account_id
    JOIN params p ON t.txn_time >= p.as_of - INTERVAL '30 days'
    GROUP BY a.customer_id
),
sess_30d AS (
    SELECT
        s.customer_id,
        COUNT(*) FILTER (WHERE s.success=true) AS login_success_30d,
        COUNT(DISTINCT s.device_id) AS uniq_devices_30d,
        MAX(s.login_time) FILTER (WHERE s.success=true) AS last_success_login_time
    FROM sessions s
    JOIN params p ON s.login_time >= p.as_of - INTERVAL '30 days'
    GROUP BY s.customer_id
),
disp_30d AS (
    SELECT
        a.customer_id,
        COUNT(*) AS disputes_30d
    FROM disputes d
    JOIN accounts a ON a.account_id = d.account_id
    JOIN params p ON d.created_time >= p.as_of - INTERVAL '30 days'
    GROUP BY a.customer_id
)
SELECT
    c.customer_id,
    c.segment,
    c.city,
    COALESCE(t.txn_success_30d,0)       AS txn_success_30d,
    COALESCE(t.txn_failed_30d,0)        AS txn_failed_30d,
    COALESCE(t.txn_pending_30d,0)       AS txn_pending_30d,
    COALESCE(t.amt_success_30d,0)       AS amt_success_30d,
    COALESCE(t.avg_amt_success_30d,0)   AS avg_amt_success_30d,
    COALESCE(s.login_success_30d,0)     AS login_success_30d,
    COALESCE(s.uniq_devices_30d,0)      AS uniq_devices_30d,
    COALESCE(d.disputes_30d,0)          AS disputes_30d,
    -- recency features (days)
    CASE
        WHEN t.last_success_txn_time IS NULL THEN 999
        ELSE EXTRACT(EPOCH FROM ((SELECT as_of FROM params) - t.last_success_txn_time))/86400
    END AS recency_txn_days,
    CASE
        WHEN s.last_success_login_time IS NULL THEN 999
        ELSE EXTRACT(EPOCH FROM ((SELECT as_of FROM params) - s.last_success_login_time))/86400
    END AS recency_login_days,
    -- churn label
    CASE
        WHEN COALESCE(t.txn_success_30d,0)=0 AND COALESCE(s.login_success_30d,0)=0 THEN 1
        ELSE 0
    END AS churn_30d
FROM customers c
LEFT JOIN txn_30d t ON t.customer_id = c.customer_id
LEFT JOIN sess_30d s ON s.customer_id = c.customer_id
LEFT JOIN disp_30d d ON d.customer_id = c.customer_id;