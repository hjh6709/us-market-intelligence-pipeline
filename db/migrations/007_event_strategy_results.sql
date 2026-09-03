CREATE TABLE IF NOT EXISTS event_strategy_results (
    strategy_result_id TEXT PRIMARY KEY,
    economic_event_id TEXT NOT NULL REFERENCES economic_events(economic_event_id),
    symbol TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    signal SMALLINT NOT NULL CHECK (signal IN (-1, 0, 1)),
    entry_at TIMESTAMPTZ NOT NULL,
    exit_at TIMESTAMPTZ NOT NULL,
    entry_price NUMERIC(18, 6),
    exit_price NUMERIC(18, 6),
    gross_return_pct NUMERIC(18, 8),
    transaction_cost_bps NUMERIC(10, 4) NOT NULL CHECK (transaction_cost_bps >= 0),
    net_return_pct NUMERIC(18, 8),
    benchmark_return_pct NUMERIC(18, 8),
    coverage_status TEXT NOT NULL,
    strategy_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT event_strategy_results_window_valid CHECK (exit_at > entry_at),
    CONSTRAINT event_strategy_results_unique_run UNIQUE (
        economic_event_id, symbol, strategy_name, strategy_version
    )
);

CREATE INDEX IF NOT EXISTS event_strategy_results_event_symbol_idx
    ON event_strategy_results (economic_event_id, symbol);
