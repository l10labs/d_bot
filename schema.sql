CREATE TABLE IF NOT EXISTS markets (
    id INTEGER PRIMARY KEY,
    condition_id TEXT NOT NULL UNIQUE,
    asset TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    starts_at INTEGER NOT NULL,
    ends_at INTEGER NOT NULL,
    discovered_at INTEGER NOT NULL,
    CHECK (ends_at > starts_at)
);

CREATE TABLE IF NOT EXISTS market_tokens (
    id INTEGER PRIMARY KEY,
    token_id TEXT NOT NULL UNIQUE,
    market_id INTEGER NOT NULL REFERENCES markets(id),
    outcome TEXT NOT NULL CHECK (outcome IN ('up', 'down')),
    UNIQUE (market_id, outcome)
);

-- One confirmed winner per market. observed_at is local receipt time, not oracle time.
CREATE TABLE IF NOT EXISTS market_resolutions (
    market_id INTEGER PRIMARY KEY REFERENCES markets(id),
    winning_token_id INTEGER NOT NULL REFERENCES market_tokens(id),
    observed_at INTEGER NOT NULL,
    source TEXT NOT NULL CHECK (source = 'clob')
);

CREATE INDEX IF NOT EXISTS markets_end_time_idx ON markets (ends_at, id);

CREATE TABLE IF NOT EXISTS prices_chainlink (
    id INTEGER PRIMARY KEY,
    asset TEXT NOT NULL,
    symbol TEXT NOT NULL,
    price TEXT NOT NULL,
    source_at INTEGER NOT NULL,
    received_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS prices_chainlink_asset_source_time_idx
    ON prices_chainlink (asset, source_at, id);
CREATE INDEX IF NOT EXISTS prices_chainlink_received_time_idx
    ON prices_chainlink (received_at, id);

CREATE TABLE IF NOT EXISTS prices_binance (
    id INTEGER PRIMARY KEY,
    asset TEXT NOT NULL,
    symbol TEXT NOT NULL,
    price TEXT NOT NULL,
    source_at INTEGER NOT NULL,
    received_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS prices_binance_asset_source_time_idx
    ON prices_binance (asset, source_at, id);
CREATE INDEX IF NOT EXISTS prices_binance_received_time_idx
    ON prices_binance (received_at, id);

CREATE TABLE IF NOT EXISTS prices_coinbase (
    id INTEGER PRIMARY KEY,
    asset TEXT NOT NULL,
    symbol TEXT NOT NULL,
    price TEXT NOT NULL,
    source_at INTEGER NOT NULL,
    received_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS prices_coinbase_asset_source_time_idx
    ON prices_coinbase (asset, source_at, id);
CREATE INDEX IF NOT EXISTS prices_coinbase_received_time_idx
    ON prices_coinbase (received_at, id);

CREATE TABLE IF NOT EXISTS market_bid_asks (
    id INTEGER PRIMARY KEY,
    market_token_id INTEGER NOT NULL REFERENCES market_tokens(id),
    best_bid TEXT NOT NULL,
    best_ask TEXT NOT NULL,
    spread TEXT NOT NULL,
    source_at INTEGER NOT NULL,
    received_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS market_bid_asks_token_time_idx
    ON market_bid_asks (market_token_id, source_at, id);
CREATE INDEX IF NOT EXISTS market_bid_asks_received_time_idx
    ON market_bid_asks (received_at, id);

CREATE TABLE IF NOT EXISTS market_trades (
    id INTEGER PRIMARY KEY,
    market_token_id INTEGER NOT NULL REFERENCES market_tokens(id),
    price TEXT NOT NULL,
    size TEXT,
    side TEXT CHECK (side IN ('buy', 'sell')),
    fee_rate_bps TEXT,
    source_at INTEGER NOT NULL,
    received_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS market_trades_token_time_idx
    ON market_trades (market_token_id, source_at, id);
CREATE INDEX IF NOT EXISTS market_trades_received_time_idx
    ON market_trades (received_at, id);
