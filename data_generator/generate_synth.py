import argparse
from datetime import datetime, timedelta
from pathlib import Path
import uuid
import numpy as np
import pandas as pd

SEGMENTS = ['mass', 'affluent', 'sme']
CITIES = ['Jakarta', 'Bandung', 'Surabaya', 'Medan', 'Yogyakarta', 'Semarang']
ACCOUNT_TYPES = ['savings', 'payroll', 'debit']
CHANNELS = ['mobile', 'web', 'atm']
STATUSES = ['SUCCESS', 'PENDING', 'FAILED']
MERCHANTS = ['Grocery', 'RideHailing', 'Ecommerce', 'Bills', 'Food', 'Travel', 'TopUp']

def uid(prefix: str) -> str:
    return f'{prefix}_{uuid.uuid4().hex[:12]}'

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--customers', type=int, default=5000)
    p.add_argument('--days', type=int, default=180)
    p.add_argument('--seed', type=int, default=42)
    p.add_argument('--out', type=str, default='data/synth')
    args = p.parse_args()

    rng = np.random.default_rng(args.seed)
    outdir = Path(args.out)
    outdir.mkdir(parents=True, exist_ok=True)

    end = datetime.utcnow().replace(microsecond=0)
    start = end - timedelta(days=args.days)

    n_cust = args.customers

    # Customers
    customers = pd.DataFrame({
        'customer_id': [uid('c') for _ in range(n_cust)],
        'created_at': [start + timedelta(days=int(x)) for x in rng.integers(0, args.days, size=n_cust)],
        'segment': rng.choice(SEGMENTS, size=n_cust, p=[0.75, 0.15, 0.10]),
        'city': rng.choice(CITIES, size=n_cust),
    })

    # Accounts (1 per customer for simplicity)
    accounts = pd.DataFrame({
        'account_id': [uid('a') for _ in range(n_cust)],
        'customer_id': customers['customer_id'].values,
        'opened_at': customers['created_at'].values,
        'account_type': rng.choice(ACCOUNT_TYPES, size=n_cust, p=[0.7, 0.2, 0.1]),
    })

    # Transactions
    # Assign activity level per account (power users vs low activity)
    activity = rng.lognormal(mean=0.2, sigma=0.9, size=n_cust)
    activity = activity / activity.sum()

    n_txn = int(n_cust * 30) # ~30 txns per customer on avg (tunable)
    account_ids = rng.choice(accounts['account_id'].values, size=n_txn, p=activity)

    txn_times = [start + timedelta(seconds=int(s)) for s in rng.integers(0, args.days * 86400, size=n_txn)]
    amounts = np.round(rng.lognormal(mean=4.0, sigma=0.7, size=n_txn), 2) # synthetic amount
    statuses = rng.choice(STATUSES, size=n_txn, p=[0.94, 0.03, 0.03])

    txns = pd.DataFrame({
        'txn_id': [uid('t') for _ in range(n_txn)],
        'account_id': account_ids,
        'txn_time': txn_times,
        'amount': amounts,
        'channel': rng.choice(CHANNELS, size=n_txn, p=[0.8, 0.15, 0.05]),
        'merchant': rng.choice(MERCHANTS, size=n_txn),
        'status': statuses,
    })

    # Sessions (login events)
    # Sessions count correlated with activity but with noise
    n_sess = int(n_cust * 20)
    cust_ids = rng.choice(customers['customer_id'].values, size=n_sess, p=np.clip(activity, 1e-9, None) / np.clip(activity, 1e-9, None).sum())
    login_times = [start + timedelta(seconds=int(s)) for s in rng.integers(0, args.days * 86400, size=n_sess)]
    devices = [f'dev_{rng.integers(1, 2000)}' for _ in range(n_sess)]
    countries = rng.choice(['ID', 'ID', 'ID', 'SG', 'MY', 'AU'], size=n_sess, p=[0.86, 0.05, 0.03, 0.03, 0.02, 0.01])
    success = rng.choice([True, False], size=n_sess, p=[0.92, 0.08])

    sessions = pd.DataFrame({
        'session_id': [uid('s') for _ in range(n_sess)],
        'customer_id': cust_ids,
        'login_time': login_times,
        'device_id': devices,
        'ip_country': countries,
        'success': success,
    })

    # Disputes (small % of transactions)
    dispute_candidates = txns.sample(frac=0.01, random_state=args.seed)
    disputes = pd.DataFrame({
        'dispute_id': [uid('d') for _ in range(len(dispute_candidates))],
        'account_id': dispute_candidates['account_id'].values,
        'created_time': dispute_candidates['txn_time'].values,
        'reason': rng.choice(['unrecognized', 'failed_but_debited', 'duplicate'], size=len(dispute_candidates), p=[0.6, 0.3, 0.1]),
        'resolved': rng.choice([True, False], size=len(dispute_candidates), p=[0.7, 0.3]),
    })

    # Write CSVs
    customers.to_csv(outdir / 'customers.csv', index=False)
    accounts.to_csv(outdir / 'accounts.csv', index=False)
    txns.to_csv(outdir / 'transactions.csv', index=False)
    sessions.to_csv(outdir / 'sessions.csv', index=False)
    disputes.to_csv(outdir / 'disputes.csv', index=False)

    print(f'Generated to: {outdir.resolve()}')
    print(f'customers={len(customers)} accounts={len(accounts)} txns={len(txns)} sessions={len(sessions)} disputes={len(disputes)}')
    print(f'date_range={start.isoformat()} .. {end.isoformat()}')

if __name__ == '__main__':
    main()