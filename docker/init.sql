-- =============================================================
-- DeFi Revenue Attribution Pipeline — Database Initialization
-- =============================================================
-- Creates raw schema and all landing tables for the ELT pipeline
-- =============================================================

-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;

-- ────────────────────────────────────────────────
-- RAW TABLES (landing zone, minimal transformation)
-- ────────────────────────────────────────────────

-- Etherscan: wallet transactions on DeFi contracts
CREATE TABLE IF NOT EXISTS raw.etherscan_transactions (
    tx_hash             VARCHAR(66)   NOT NULL,
    block_number        BIGINT        NOT NULL,
    block_timestamp     TIMESTAMP     NOT NULL,
    from_address        VARCHAR(42)   NOT NULL,
    to_address          VARCHAR(42),
    contract_address    VARCHAR(42),
    value_wei           VARCHAR(100)  DEFAULT '0',
    gas_used            BIGINT,
    gas_price_wei       VARCHAR(100),
    method_id           VARCHAR(10),
    function_name       VARCHAR(255),
    is_error            BOOLEAN       DEFAULT FALSE,
    protocol_name       VARCHAR(100),
    chain               VARCHAR(50)   DEFAULT 'ethereum',
    _extracted_at       TIMESTAMP     DEFAULT NOW(),
    PRIMARY KEY (tx_hash)
);

CREATE INDEX IF NOT EXISTS idx_etherscan_tx_from ON raw.etherscan_transactions(from_address);
CREATE INDEX IF NOT EXISTS idx_etherscan_tx_timestamp ON raw.etherscan_transactions(block_timestamp);
CREATE INDEX IF NOT EXISTS idx_etherscan_tx_protocol ON raw.etherscan_transactions(protocol_name);

-- DeFiLlama: TVL history per protocol
CREATE TABLE IF NOT EXISTS raw.defillama_tvl (
    id                  SERIAL        PRIMARY KEY,
    protocol_slug       VARCHAR(100)  NOT NULL,
    protocol_name       VARCHAR(255),
    chain               VARCHAR(50),
    date                DATE          NOT NULL,
    tvl_usd             NUMERIC(20,2),
    _extracted_at       TIMESTAMP     DEFAULT NOW(),
    UNIQUE(protocol_slug, chain, date)
);

CREATE INDEX IF NOT EXISTS idx_defillama_tvl_date ON raw.defillama_tvl(date);

-- DeFiLlama: Protocol fees & revenue
CREATE TABLE IF NOT EXISTS raw.defillama_fees (
    id                  SERIAL        PRIMARY KEY,
    protocol_slug       VARCHAR(100)  NOT NULL,
    date                DATE          NOT NULL,
    total_fees_usd      NUMERIC(20,2),
    revenue_usd         NUMERIC(20,2),
    _extracted_at       TIMESTAMP     DEFAULT NOW(),
    UNIQUE(protocol_slug, date)
);

-- Dune Analytics: Wallet labels / segments
CREATE TABLE IF NOT EXISTS raw.dune_wallet_labels (
    wallet_address      VARCHAR(42)   NOT NULL,
    label               VARCHAR(100),
    label_type          VARCHAR(100),
    project             VARCHAR(100),
    first_activity_date DATE,
    total_txs           BIGINT,
    _extracted_at       TIMESTAMP     DEFAULT NOW(),
    PRIMARY KEY (wallet_address)
);

-- CoinGecko: Daily token prices
CREATE TABLE IF NOT EXISTS raw.token_prices (
    id                  SERIAL        PRIMARY KEY,
    token_id            VARCHAR(100)  NOT NULL,
    token_symbol        VARCHAR(20),
    date                DATE          NOT NULL,
    price_usd           NUMERIC(20,8),
    market_cap_usd      NUMERIC(20,2),
    volume_24h_usd      NUMERIC(20,2),
    _extracted_at       TIMESTAMP     DEFAULT NOW(),
    UNIQUE(token_id, date)
);

CREATE INDEX IF NOT EXISTS idx_token_prices_date ON raw.token_prices(date);
CREATE INDEX IF NOT EXISTS idx_token_prices_symbol ON raw.token_prices(token_symbol);

-- ────────────────────────────────────────────────
-- ADVANCED ANALYTICS (Alternative Data)
-- ────────────────────────────────────────────────

-- Li.Fi: Cross-chain activity for Nomad Score
CREATE TABLE IF NOT EXISTS raw.cross_chain_activity (
    wallet_address              VARCHAR(42)   NOT NULL,
    distinct_chains_used        INTEGER,
    total_bridging_volume_usd   NUMERIC(20,2),
    last_bridge_date            DATE,
    _extracted_at               TIMESTAMP     DEFAULT NOW(),
    PRIMARY KEY (wallet_address)
);

-- Zerion/Zapper: Wallet historical win-rates
CREATE TABLE IF NOT EXISTS raw.wallet_enrichment (
    wallet_address              VARCHAR(42)   NOT NULL,
    historical_win_rate         NUMERIC(5,4),
    realized_profit_usd         NUMERIC(20,2),
    _extracted_at               TIMESTAMP     DEFAULT NOW(),
    PRIMARY KEY (wallet_address)
);

-- ────────────────────────────────────────────────
-- PIPELINE METADATA
-- ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw._pipeline_runs (
    id                  SERIAL        PRIMARY KEY,
    run_id              UUID          DEFAULT gen_random_uuid(),
    extractor_name      VARCHAR(100)  NOT NULL,
    status              VARCHAR(20)   NOT NULL,  -- success, failed, partial
    rows_extracted      INTEGER       DEFAULT 0,
    rows_loaded         INTEGER       DEFAULT 0,
    started_at          TIMESTAMP     NOT NULL,
    completed_at        TIMESTAMP,
    error_message       TEXT,
    metadata            JSONB
);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO pipeline;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA raw TO pipeline;
GRANT ALL PRIVILEGES ON SCHEMA staging TO pipeline;
GRANT ALL PRIVILEGES ON SCHEMA intermediate TO pipeline;
GRANT ALL PRIVILEGES ON SCHEMA marts TO pipeline;
