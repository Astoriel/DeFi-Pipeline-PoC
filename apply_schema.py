import os
from sqlalchemy import create_engine, text

# Get URL from environment or fallback
db_url = "postgresql://pipeline:pipeline_secret@127.0.0.1:5433/defi_pipeline"
engine = create_engine(db_url)

with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS raw.cross_chain_activity (
            wallet_address VARCHAR(42) NOT NULL,
            distinct_chains_used INTEGER,
            total_bridging_volume_usd NUMERIC(20,2),
            last_bridge_date DATE,
            _extracted_at TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (wallet_address)
        );
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS raw.wallet_enrichment (
            wallet_address VARCHAR(42) NOT NULL,
            historical_win_rate NUMERIC(5,4),
            realized_profit_usd NUMERIC(20,2),
            _extracted_at TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (wallet_address)
        );
    """))

print("Schema updates applied successfully.")
