-- Core entities (minimal warehouse-style schema)

CREATE TABLE IF NOT EXISTS customers (
    customer_id  TEXT PRIMARY KEY,
    created_at   TIMESTAMP NOT NULL,
    segment      TEXT NOT NULL,
    city         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS accounts (
    account_id   TEXT PRIMARY KEY,
    customer_id  TEXT NOT NULL REFERENCES customers(customer_id),
    opened_at    TIMESTAMP NOT NULL,
    account_type TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
    txn_id       TEXT PRIMARY KEY,
    account_id   TEXT NOT NULL REFERENCES accounts(account_id),
    txn_time     TIMESTAMP NOT NULL,
    amount       NUMERIC NOT NULL,
    channel      TEXT NOT NULL,   -- mobile/web/atm
    merchant     TEXT,
    status       TEXT NOT NULL    -- SUCCESS/PENDING/FAILED
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id   TEXT PRIMARY KEY,
    customer_id  TEXT NOT NULL REFERENCES customers(customer_id),
    login_time   TIMESTAMP NOT NULL,
    device_id    TEXT NOT NULL,
    ip_country   TEXT NOT NULL,
    success      BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS disputes (
    dispute_id   TEXT PRIMARY KEY,
    account_id   TEXT NOT NULL REFERENCES accounts(account_id),
    created_time TIMESTAMP NOT NULL,
    reason       TEXT NOT NULL,
    resolved     BOOLEAN NOT NULL DEFAULT FALSE
);