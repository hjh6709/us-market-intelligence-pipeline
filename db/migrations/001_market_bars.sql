CREATE TABLE IF NOT EXISTS market_bars (
    symbol TEXT NOT NULL,
    bar_start TIMESTAMPTZ NOT NULL,
    timeframe TEXT NOT NULL,
    open NUMERIC(18, 6) NOT NULL CHECK (open > 0),
    high NUMERIC(18, 6) NOT NULL CHECK (high > 0),
    low NUMERIC(18, 6) NOT NULL CHECK (low > 0),
    close NUMERIC(18, 6) NOT NULL CHECK (close > 0),
    volume BIGINT NOT NULL CHECK (volume >= 0),
    trade_count BIGINT NOT NULL CHECK (trade_count >= 0),
    vwap NUMERIC(18, 6),
    source TEXT NOT NULL,
    feed TEXT NOT NULL,
    is_final BOOLEAN NOT NULL CHECK (is_final),
    condition_policy TEXT NOT NULL,
    spark_batch_id BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT market_bars_high_valid
        CHECK (high >= GREATEST(open, low, close)),
    CONSTRAINT market_bars_low_valid
        CHECK (low <= LEAST(open, high, close)),
    PRIMARY KEY (symbol, bar_start, timeframe, source, feed)
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'market_bars_high_valid'
          AND conrelid = 'market_bars'::regclass
    ) THEN
        ALTER TABLE market_bars
            ADD CONSTRAINT market_bars_high_valid
            CHECK (high >= GREATEST(open, low, close));
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'market_bars_low_valid'
          AND conrelid = 'market_bars'::regclass
    ) THEN
        ALTER TABLE market_bars
            ADD CONSTRAINT market_bars_low_valid
            CHECK (low <= LEAST(open, high, close));
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS market_bars_symbol_time_idx
    ON market_bars (symbol, timeframe, bar_start DESC);
