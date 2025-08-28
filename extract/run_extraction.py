"""
Pipeline Orchestrator — runs all extractors and loads data into PostgreSQL.

Usage:
    python extract/run_extraction.py                  # all sources
    python extract/run_extraction.py --source etherscan
    python extract/run_extraction.py --source defillama
    python extract/run_extraction.py --source dune
    python extract/run_extraction.py --source coingecko
"""
from __future__ import annotations

import sys
from datetime import datetime
from typing import Optional

import typer
from loguru import logger
from rich.console import Console
from rich.table import Table

from .config import settings
from .coingecko_extractor import CoinGeckoExtractor
from .defillama_extractor import DeFiLlamaExtractor
from .dune_extractor import DuneExtractor
from .etherscan_extractor import EtherscanExtractor
from .lifi_extractor import LiFiExtractor
from .portfolio_extractor import PortfolioExtractor
from .loader import PostgresLoader

app = typer.Typer(help="DeFi Revenue Attribution — Extract Pipeline")
console = Console()


def _configure_logging() -> None:
    logger.remove()
    logger.add(
        sys.stderr,
        level=settings.log_level,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
        colorize=True,
    )
    logger.add(
        "logs/pipeline_{time:YYYY-MM-DD}.log",
        level="DEBUG",
        rotation="1 day",
        retention="7 days",
    )


def _run_extractor(extractor, loader: PostgresLoader, source_name: str) -> dict:
    """Run a single extractor and return result metadata."""
    started = datetime.utcnow()
    try:
        df = extractor.extract()
        df = extractor.validate(df)

        if df.empty:
            loader.log_run(source_name, "partial", 0, 0, started)
            return {"source": source_name, "status": "⚠️ empty", "rows": 0}

        # Each extractor has its own conflict column logic
        conflict_map = {
            "etherscan": ["tx_hash"],
            "defillama": ["protocol_slug", "chain", "date"],
            "dune": ["wallet_address"],
            "coingecko": ["token_id", "date"],
            "lifi": ["wallet_address"],
            "portfolio": ["wallet_address"],
        }
        conflict_cols = conflict_map.get(source_name)

        rows = loader.upsert(df, extractor.target_table, conflict_columns=conflict_cols)
        loader.log_run(source_name, "success", len(df), rows, started)
        return {"source": source_name, "status": "✅ success", "rows": rows}

    except Exception as e:
        loader.log_run(source_name, "failed", 0, 0, started, str(e))
        logger.error(f"Extractor {source_name} failed: {e}")
        return {"source": source_name, "status": "❌ failed", "rows": 0, "error": str(e)}


@app.command()
def run(
    source: Optional[str] = typer.Option(
        None,
        "--source", "-s",
        help="Source to extract: etherscan | defillama | dune | coingecko | lifi | portfolio | all",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Extract only, do not load to database"
    ),
) -> None:
    """Run the DeFi data extraction pipeline."""
    _configure_logging()
    console.print("\n[bold cyan]🚀 DeFi Revenue Attribution — Extraction Pipeline[/bold cyan]\n")

    loader = PostgresLoader()

    extractors_to_run = {
        "etherscan":  EtherscanExtractor(),
        "defillama":  DeFiLlamaExtractor(),
        "dune":       DuneExtractor(),
        "coingecko":  CoinGeckoExtractor(),
        "lifi":       LiFiExtractor(),
        "portfolio":  PortfolioExtractor(),
    }

    # Filter by source if specified
    if source and source != "all":
        if source not in extractors_to_run:
            console.print(f"[red]Unknown source: {source}[/red]")
            console.print(f"Available: {', '.join(extractors_to_run.keys())}")
            raise typer.Exit(1)
        extractors_to_run = {source: extractors_to_run[source]}

    results = []
    for name, extractor in extractors_to_run.items():
        console.print(f"[bold]▶ Running {name} extractor...[/bold]")
        result = _run_extractor(extractor, loader if not dry_run else None, name)
        results.append(result)

    # Print summary table
    table = Table(title="\n📊 Extraction Summary", show_header=True)
    table.add_column("Source", style="cyan")
    table.add_column("Status")
    table.add_column("Rows Loaded", justify="right")
    for r in results:
        table.add_row(r["source"], r["status"], f"{r['rows']:,}")
    console.print(table)

    # Check if any failed
    failed = [r for r in results if "failed" in r["status"]]
    if failed:
        console.print(f"\n[red]❌ {len(failed)} extractor(s) failed[/red]")
        raise typer.Exit(1)
    else:
        console.print("\n[green]✅ All extractors completed successfully![/green]\n")


if __name__ == "__main__":
    app()
