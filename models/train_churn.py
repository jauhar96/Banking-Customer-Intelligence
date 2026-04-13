import os
import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import roc_auc_score, average_precision_score, precision_recall_fscore_support
from sklearn.linear_model import LogisticRegression
import mlflow
import mlflow.sklearn

def db_url():
    user = os.getenv("POSTGRES_USER", "banking")
    pwd = os.getenv("POSTGRES_PASSWORD", "bankingpass")
    db = os.getenv("POSTGRES_DB", "banking_dw")
    host = os.getenv("POSTGRES_HOST", "127.0.0.1")
    port = os.getenv("POSTGRES_PORT", "5432")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}/{db}"

def main():
    tracking_url = os.getenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:5000")
    mlflow.set_tracking_uri(tracking_url)
    mlflow.set_experiment("bci_churn_baseline_v2")

    engine = create_engine(db_url())
    df = pd.read_sql("SELECT * FROM customer_feature_mart_30d", engine)

    y = df["churn_30d"].astype(int)
    x = df.drop(columns=["churn_30d"])

    cat_cols = ['segment', 'city']
    num_cols = [c for c in x.columns if c not in cat_cols and c != "customer_id"]

    pre = ColumnTransformer([
        ('cat', OneHotEncoder(handle_unknown='ignore'), cat_cols),
        ('num', 'passthrough', num_cols),
    ])
    
    clf = LogisticRegression(max_iter=1000)

    pipe = Pipeline([('pre', pre), ('clf', clf)])

    x_train, x_test, y_train, y_test = train_test_split(
        x, y, test_size=0.2, random_state=42, stratify=y
    )

    with mlflow.start_run():
        mlflow.log_param('model', 'logreg')
        mlflow.log_param('feature_window_days', 30)

        pipe.fit(x_train, y_train)
        proba = pipe.predict_proba(x_test)[:,1]

        auc = roc_auc_score(y_test, proba)
        ap = average_precision_score(y_test, proba)

        y_pred = (proba >= 0.5).astype(int)
        prec, rec, f1, _ = precision_recall_fscore_support(y_test, y_pred, average='binary', zero_division=0)

        mlflow.log_metric('roc_auc', float(auc))
        mlflow.log_metric('pr_auc', float(ap))
        mlflow.log_metric('precision_at_0_5', float(prec))
        mlflow.log_metric('recall_at_0_5', float(rec))
        mlflow.log_metric('f1_at_0_5', float(f1))

        mlflow.sklearn.log_model(pipe, 'model')

        print(f'ROC-AUC={auc:.4f} PR-AUC={ap:.4f} P={prec:.3f} R={rec:.3f} F1={f1:.3f}')
        print('MLflow: {tracking_url}')

if __name__ == '__main__':
    main()