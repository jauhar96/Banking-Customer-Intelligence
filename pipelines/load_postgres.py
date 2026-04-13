import argparse
from pathlib import Path
import os
import psycopg2

TABLES = [
    ('customers', 'customers.csv'),
    ('accounts', 'accounts.csv'),
    ('transactions', 'transactions.csv'),
    ('sessions', 'sessions.csv'),
    ('disputes', 'disputes.csv'),
]

def get_conn():
    user = os.getenv('POSTGRES_USER', 'banking')
    pwd = os.getenv('POSTGRES_PASSWORD', 'bankingpass')
    db = os.getenv('POSTGRES_DB', 'banking_dw')
    host = os.getenv('POSTGRES_HOST', '127.0.0.1')
    port = int(os.getenv('POSTGRES_PORT', '5432'))
    return psycopg2.connect(host=host, port=port, user=user, password=pwd, dbname=db)

def copy_csv(cur, table: str, csv_path: Path):
    with csv_path.open('r', encoding='utf-8') as f:
        cur.copy_expert(f'COPY {table} FROM STDIN WITH CSV HEADER', f)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--data_dir', type=str, default='data/synth')
    ap.add_argument('--truncate', action='store_true')
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        raise SystemExit(f'data_dir not found: {data_dir}')
    
    conn = get_conn()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            if args.truncate:
                # Order matters because of FK
                cur.execute('TRUNCATE disputes, sessions, transactions, accounts, customers RESTART IDENTITY CASCADE;')
            
            for table, fname in TABLES:
                path = data_dir / fname
                if not path.exists():
                    raise SystemExit(f'Missing {path}')
                copy_csv(cur, table, path)
                print(f'Loaded {table} <- {fname}')
        
        conn.commit()
        print('DONE.')
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == '__main__':
    main()