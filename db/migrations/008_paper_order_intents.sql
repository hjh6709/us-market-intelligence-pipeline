CREATE TABLE IF NOT EXISTS paper_order_intents (
    scope TEXT NOT NULL,
    request_id TEXT NOT NULL,
    client_order_id TEXT NOT NULL UNIQUE,
    intent JSONB NOT NULL,
    state TEXT NOT NULL DEFAULT 'UNKNOWN',
    broker_order_id TEXT,
    filled_qty NUMERIC NOT NULL DEFAULT 0 CHECK (filled_qty >= 0),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (scope, request_id)
);
