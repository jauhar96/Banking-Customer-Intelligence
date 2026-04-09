# Banking Customer Intelligence - Churn Scoring Platform (DW + MLflow + Scoring API)

Portofolio-grade **Banking Customer Intelligence** project that simulates a lightweight production pipeline for **customer churn scoring**:
- Synthetic banking event data generation (privacy-safe)
- Postgres "warehouse" + **feature mart view** (SQL)
- Model training (baseline churn classifier) with **MLflow experiment tracking**
- FastAPI scoring service (**single / batch / model reload**)
- Prediction **write-back** to DW for downstream analytics

> Disclaimer: This project uses synthetic data only. No real customer data is used.

---

## Why this is "production-oriented"

This project intentionally mirrors core patterns used in banking digital platforms:

- **Separation of concerns**
    - Data storage & modeling: Postgres + SQL views
    - Training & experiment tracking: MLflow
    - Serving & scoring: FastAPI
    - Online-to-offline handoff: score write-back table

- **Model lifecycle**
    - Train -> log to MLflow -> serve "latest" -> hot reload without server restart

- **Downstream readiness**
    - Predictions are persisted in a durable table (`customer_churn_scores`) to power BI/dashboarding, customer outreach, risk ops, and monitoring queries.

---

## Architecture

```text
           ┌────────────────────────────────────────────────────┐
           │             Synthetic Data Generator                │
           │   customers / accounts / txns / sessions / disputes │
           └───────────────┬────────────────────────────────────┘
                           │ CSV
                           ▼
┌────────────────────────────────────────────────────────────────────┐
│                         PostgreSQL (DW)                             │
│  tables: customers, accounts, transactions, sessions, disputes       │
│  view  : customer_feature_mart_30d                                   │
│  table : customer_churn_scores (write-back predictions)              │
└───────────────┬────────────────────────────────────────────────────┘
                │ features (SQL view)
                ▼
┌────────────────────────────────────────────────────────────────────┐
│                       Model Training (sklearn)                       │
│    - reads feature mart view                                         │
│    - trains churn classifier                                         │
│    - logs metrics + model artifacts to MLflow                        │
└───────────────┬────────────────────────────────────────────────────┘
                │ runs:/<run_id>/model
                ▼
┌────────────────────────────────────────────────────────────────────┐
│                          MLflow Tracking                             │
│     - experiment: bci_churn_baseline_v2 (configurable)               │
│     - model artifacts stored & versioned per run                     │
└───────────────┬────────────────────────────────────────────────────┘
                │ load latest run
                ▼
┌────────────────────────────────────────────────────────────────────┐
│                        FastAPI Scoring Service                       │
│  POST /model/reload       -> hot load latest MLflow model            │
│  POST /score/customer     -> score single customer_id                │
│  POST /score/batch        -> score list of customer_ids              │
│  POST /scores/writeback   -> score + upsert into DW                  │
└────────────────────────────────────────────────────────────────────┘

## Tech Stack
- Python 3.12+
- PostgreSQL (Docker)
- MLflow (Docker)
- scikit-learn
- FastAPI + Uvicorn
- pandas + SQLAlchemy

## Respository Structure
banking-customer-intel/
├─ api/
│  └─ main.py                   # Scoring API + MLflow reload + writeback
├─ data_generator/
│  └─ generate_synth.py          # Synthetic data generator
├─ pipelines/
│  └─ load_postgres.py           # Load CSVs into Postgres
├─ models/
│  └─ train_churn.py             # Train churn baseline + log to MLflow
├─ sql/
│  ├─ 00_schema.sql              # DDL tables
│  ├─ 10_feature_mart.sql         # Feature mart view
│  └─ 20_scores_table.sql         # Prediction writeback table
├─ data/
│  └─ synth/                     # Generated CSVs (local only)
└─ docker-compose.yml            # Postgres + MLflow

## Data Contracts
**Input (Feature Mart)**
The scoring service reads from:
- FEATURE_VIEW (default: customer_feature_mart_30d)

Expected columns include:
- customer_id (primary key)
- engineered behavioral / transactional features
- optional label column churn_30d (dropped during inference)

**Output (Write-back Table)**
Predictions are written to:
- customer_schurn_scores(customer_id PF, as_of, model_run_id, churn_proba, churn_pred, threshold, created_at)

This table enables:
- downstream analytics (aggregation by segment)
- operational use (outreach list)
- monitoring drift (compate score distribution over time)
- audability (run_id for traceability)

## Quick Start (End-to-End Demo)

**0) Prerequisites**
- Docker + Docker Compose (WSL2 recommended)
- Python 3.12+

**1) Start services (Postgres + MLflow)**
docker compose up -d
docker compose ps

MLflow UI:
- http://127.0.0.1:5000

**2) Python venv**
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install pandas numpy scikit-learn fastapi uvicorn sqlalchemy psycopg2-binary python-dotenv mlflow

**3) Create schema + feature mart + score table**
docker exec -i bci_postgres psql -U banking -d banking_dw < sql/00_schema.sql
docker exec -i bci_postgres psql -U banking -d banking_dw < sql/10_feature_mart.sql
docker exec -i bci_postgres psql -U banking -d banking_dw < sql/20_scores_table.sql

**4) Generate synthe thic data**
Example: 5,000 customers over 180 days.
python data_generator/generate_synth.py --customers 5000 --days 180 --out data/synth

**5) Load data into Postgres**
python pipelines/load_postgres.py --data_dir data/synth --truncate

Sanity check:
docker exec -it bci_postgres psql -U banking -d banking_dw -c \
"SELECT 'customers' t, COUNT(*) c FROM customers
 UNION ALL
 SELECT 'transactions', COUNT(*) FROM transactions;"

**6) Train model + log to MLflow**
python models/train_churn.py

Open MLflow:
- http://127.0.0.1:5000

**7) Run scoring API**
uvicorn api.main:app --reload --host 0.0.0.0 --port 8001 --reload-dir api

Swagger docs:
- http://127.0.0.1:8001/docs

**8) Reload latest model (hot swap)**
curl -X POST "http://127.0.0.1:8001/model/reload" \
  -H "Content-Type: application/json" \
  -d '{}'

**9) Demo: score + write-back**
Get sample IDs:
docker exec -it bci_postgres psql -U banking -d banking_dw -c \
"SELECT customer_id FROM customers LIMIT 3;"

Batch score:
curl -X POST "http://127.0.0.1:8001/score/batch" \
  -H "Content-Type: application/json" \
  -d '{"customer_ids":["<ID1>","<ID2>","<ID3>"]}'

Write-back:
curl -X POST "http://127.0.0.1:8001/scores/writeback" \
  -H "Content-Type: application/json" \
  -d '{"customer_ids":["<ID1>","<ID2>","<ID3>"]}'

Verify DW table:
docker exec -it bci_postgres psql -U banking -d banking_dw -c \
"SELECT * FROM customer_churn_scores ORDER BY created_at DESC LIMIT 5;"

## Operational Notes (Runbook)
**- Model not loaded?**
    Call POST /model/reload before scoring.

**-Where does the model come from?**
    Latest run in MLflow experiment MLFLOW_EXPERIMENT (default: bci_churn_baseline_v2).

**- Change threshold for scoring**
    Provide threshold in requests or set CHURN_THRESHOLD in .env.

## Configuration
Environment variables (optional) via .env (gitignored):
# Postgres
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5432
POSTGRES_USER=banking
POSTGRES_PASSWORD=bankingpass
POSTGRES_DB=banking_dw

# MLflow
MLFLOW_TRACKING_URI=http://127.0.0.1:5000
MLFLOW_EXPERIMENT=bci_churn_baseline_v2

# Features & scoring
FEATURE_VIEW=customer_feature_mart_30d
CHURN_THRESHOLD=0.5

## Oberservability & Auditability
- All prediction include model_run_id -> ties each score to an MLfow run (audit trail).
- Write-back table enables monitoring:
    - score distribution drift over time
    - threshold impact analysis
    - segment-level churn risk reporting

## License
MIT (see LICENSE)