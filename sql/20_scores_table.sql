CREATE TABLE IF NOT EXISTS customer_churn_scores (
    customer_id TEXT PRIMARY KEY,
    as_of TIMESTAMP NOT NULL,
    model_run_id TEXT NOT NULL,
    churn_proba DOUBLE PRECISION NOT NULL,
    churn_pred INTEGER NOT NULL,
    threshold DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);