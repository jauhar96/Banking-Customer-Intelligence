import os
from typing import List, Optional, Dict, Any
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from sqlalchemy import create_engine, text
import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient

# Load .env if present (safe: .env should be gitignored)
load_dotenv(dotenv_path=Path('.env'), override=False)

# -----------------------------
# Config
# -----------------------------
FEATURE_VIEW = os.getenv('FEATURE_VIEW', 'customer_feature_mart_30d')
EXPERIMENT_NAME = os.getenv('MLFLOW_EXPERIMENT', 'bci_churn_baseline_v2')
TRACKING_URI = os.getenv('MLFLOW_TRACKING_URI', 'http://127.0.0.1:5000')

DEFAULT_THRESHOLD = float(os.getenv('CHURN_THRESHOLD', '0.5'))

# DB
DB_USER = os.getenv('POSTGRES_USER', 'banking')
DB_PWD = os.getenv('POSTGRES_PASSWORD', 'bankingpass')
DB_NAME = os.getenv('POSTGRES_DB', 'banking_dw')
DB_HOST = os.getenv('POSTGRES_HOST', '127.0.0.1')
DB_PORT = os.getenv('POSTGRES_PORT', '5432')

def db_url() -> str:
    return f'postgresql+psycopg2://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

engine = create_engine(db_url(), pool_pre_ping=True)

# -----------------------------
# MLflow model cache
# -----------------------------
MODEL = None
MODEL_RUN_ID = None

def get_latest_run_id(experiment_name: str) -> str:
    mlflow.set_tracking_uri(TRACKING_URI)
    client = MlflowClient(tracking_uri=TRACKING_URI)

    exp = client.get_experiment_by_name(experiment_name)
    if exp is None:
        raise RuntimeError(f'MLflow experiment not found: {experiment_name}')
    
    runs = client.search_runs(
        experiment_ids=[exp.experiment_id],
        order_by=['attributes.start_time DESC'],
        max_results=1,
    )
    if not runs:
        raise RuntimeError(f'No runs found in experiment: {experiment_name}')
    
    return runs[0].info.run_id

def load_model_latest() -> Dict[str, Any]:
    """
    Load the latest run's sklearn model.
    Using mlflow.sklearn.load_model so we can do predict_proba (true probability).
    """
    global MODEL, MODEL_RUN_ID

    run_id = get_latest_run_id(EXPERIMENT_NAME)
    model_uri = f'runs:/{run_id}/model' # artifact path from mlflow.sklearn.log_model(..., "model")
    mlflow.set_tracking_uri(TRACKING_URI)

    MODEL = mlflow.sklearn.load_model(model_uri)
    MODEL_RUN_ID = run_id

    return {'run_id': run_id, 'model_uri': model_uri}

# -----------------------------
# Feature retrieval
# -----------------------------

def fetch_features(customer_ids: List[str]) -> pd.DataFrame:
    if not customer_ids:
        return pd.DataFrame()
    
    # Parametrized IN query
    q = text(f"""
        SELECT *
        FROM {FEATURE_VIEW}
        WHERE customer_id = ANY(:ids)
    """)

    with engine.begin() as conn:
        df = pd.read_sql(q, conn, params={'ids': customer_ids})
    
    return df

def prepare_x(df: pd.DataFrame) -> pd.DataFrame:
    # Model was trained on x = all columns except churn_30d
    if 'churn_30d' in df.columns:
        df = df.drop(columns=['churn_30d'])
    return df

def predict_proba_like(model, x: pd.DataFrame) -> List[float]:
    """
    Return churn probability-like scores.
    Prefer predict_proba; fallback to predict (0/1).
    """
    if hasattr(model, 'predict_proba'):
        proba = model.predict_proba(x)
        # proba: (n,2) => take class-1 prob
        return [float(p[1]) for p in proba]
    # Fallback: label prediction as float
    preds = model.predict(x)
    return [float(p) for p in preds]

def get_as_of_timestamp():
    with engine.begin() as conn:
        return conn.execute(text("SELECT MAX(txn_time) FROM transactions")).scalar()

# -----------------------------
# FastAPI schemas
# -----------------------------

class ScoreCustomerRequest(BaseModel):
    customer_id: str = Field(..., min_length=3)
    threshold: Optional[float] = Field(default=None, ge=0.0, le=1.0)

class ScoreBatchRequest(BaseModel):
    customer_ids: List[str] = Field(..., min_items=1)
    threshold: Optional[float] = Field(default=None, ge=0.0, le=1.0)

class WritebackRequest(BaseModel):
    customer_ids: List[str] = Field(..., min_items=1)
    threshold: Optional[float] = Field(default=None, ge=0.0, le=1.0)

# -----------------------------
# App
# -----------------------------

app = FastAPI(title='Banking Customer Intelligence - Scoring API')

@app.on_event('startup')
def _startup():
    # Load model on start for nicer UX
    try:
        info = load_model_latest()
        print(f'[startup] Load model from MLflow run_id={info['run_id']}')
    except Exception as e:
        print(f'[startup] Model not loaded yet: {e}')

@app.get('/health')
def health():
    # DB check
    try:
        with engine.begin() as conn:
            conn.execute(text('SELECT 1'))
        db_ok = True
    except Exception:
        db_ok = False
    
    return {
        'status': 'ok',
        'db_ok': db_ok,
        'mlflow_tracking_uri': TRACKING_URI,
        'experiment': EXPERIMENT_NAME,
        'model_loaded': MODEL is not None,
        'model_run_id': MODEL_RUN_ID,
        'feature_view': FEATURE_VIEW,
    }

@app.post('/model/reload')
def reload_model():
    info = load_model_latest()
    return {'status': 'reloaded', **info}

@app.post('/score/customer')
def score_customer(req: ScoreCustomerRequest):
    if MODEL is None:
        raise HTTPException(status_code=503, detail='Model not loaded. Call /model/reload')
    
    df = fetch_features([req.customer_id])
    if df.empty:
        raise HTTPException(status_code=404, detail=f'customer_id not found: {req.customer_id}')
    
    x = prepare_x(df)
    #pyfunc predict returns numpy array-like
    proba = predict_proba_like(MODEL, x)[0] # churn probability=like score
    thr = req.threshold if req.threshold is not None else DEFAULT_THRESHOLD
    pred = int(proba >= thr)

    return {
        'customer_id': req.customer_id,
        'churn_proba': float(proba),
        'threshold': float(thr),
        'churn_pred': pred,
        'model_run_id': MODEL_RUN_ID,
    }

@app.post('/score/batch')
def score_batch(req: ScoreBatchRequest):
    if MODEL is None:
        raise HTTPException(status_code=503, detail='Model not loaded. call /model/reload')
    
    df = fetch_features(req.customer_ids)
    found = set(df['customer_id'].tolist()) if not df.empty else set()
    missing = [cid for cid in req.customer_ids if cid not in found]

    if df.empty:
        raise HTTPException(status_code=404, detail="No customer_ids found")
    
    x = prepare_x(df)
    scores = predict_proba_like(MODEL, x)
    thr = req.threshold if req.threshold is not None else DEFAULT_THRESHOLD

    out = []
    for cid, s in zip(df['customer_id'].tolist(), scores):
        proba = float(s)
        out.append({
            'customer_id': cid,
            'churn_proba': proba,
            'churn_pred': int(proba >= thr),
        })

    return {
        'threshold': thr,
        'model_run_id': MODEL_RUN_ID,
        'results': out,
        'missing': missing,
    }

@app.post("/scores/writeback")
def score_writeback(req: WritebackRequest):
    """
    Score customers and write results back to table 'customer_churn_scores'.
    Requires you to create the table first (sql/20_scores_table.sql).
    """
    if MODEL is None:
        raise HTTPException(status_code=503, detail='Model not loaded. Call POST /model/reload')
    
    df = fetch_features(req.customer_ids)
    if df.empty:
        raise HTTPException(status_code=404, detail='No customer_ids found')
    
    thr = req.threshold if req.threshold is not None else DEFAULT_THRESHOLD

    x = prepare_x(df)
    scores = predict_proba_like(MODEL, x)

    as_of = get_as_of_timestamp()
    if as_of is None:
        raise HTTPException(status_code=500, detail='Cannot compute as_of timestamp (transactions empty?)')
    
    row = []
    for cid, proba in zip(df['customer_id'].tolist(), scores):
        row.append({
            'customer_id': cid,
            'as_of': as_of,
            'model_run_id': MODEL_RUN_ID,
            'churn_proba': float(proba),
            'churn_pred': int(proba >= thr),
            'threshold': float(thr),
        })

    upsert_sql = text("""
        INSERT INTO customer_churn_scores
            (customer_id, as_of, model_run_id, churn_proba, churn_pred, threshold)
        VALUES
            (:customer_id, :as_of, :model_run_id, :churn_proba, :churn_pred, :threshold)
        ON CONFLICT (customer_id) DO UPDATE SET
            as_of = EXCLUDED.as_of,
            model_run_id = EXCLUDED.model_run_id,
            churn_proba = EXCLUDED.churn_proba,
            churn_pred = EXCLUDED.churn_pred,
            threshold = EXCLUDED.threshold,
            created_at = NOW();
    """)

    with engine.begin() as conn:
        conn.execute(upsert_sql, row)
    
    return {
        'status': 'ok',
        'written': len(row),
        'model_run_id': MODEL_RUN_ID,
        'threshold': float(thr),
        'as_of': str(as_of),
    }
