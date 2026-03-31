"""
Person A - Week 1: Fetch and process stock price data

Downloads historical stock prices for defense companies and market index,
computes log returns, and saves to data/raw/
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime
import logging
from utils import load_config, setup_logging, calculate_log_returns, save_dataset


def fetch_prices(tickers: list, start_date: str, end_date: str = None) -> pd.DataFrame:
    """
    Fetch adjusted close prices for given tickers.

    Args:
        tickers: List of ticker symbols
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format (default: today)

    Returns:
        DataFrame with columns: date, ticker, adj_close, log_return
    """
    if end_date is None:
        end_date = datetime.today().strftime('%Y-%m-%d')

    logging.info(f"Fetching prices for {len(tickers)} tickers from {start_date} to {end_date}")

    all_data = []

    for ticker in tickers:
        logging.info(f"Fetching {ticker}...")

        try:
            # Download data
            stock = yf.Ticker(ticker)
            df = stock.history(start=start_date, end=end_date)

            if df.empty:
                logging.warning(f"No data found for {ticker}")
                continue

            # Keep only adjusted close
            df = df[['Close']].copy()
            df.columns = ['adj_close']
            df['ticker'] = ticker
            df['date'] = df.index
            df = df.reset_index(drop=True)

            # Calculate log returns
            df = df.sort_values('date')
            df['log_return'] = calculate_log_returns(df['adj_close'])

            all_data.append(df)

            logging.info(f"  Retrieved {len(df)} days for {ticker}")
            logging.info(f"  Date range: {df['date'].min()} to {df['date'].max()}")

        except Exception as e:
            logging.error(f"Error fetching {ticker}: {e}")
            continue

    if not all_data:
        raise ValueError("No data was successfully retrieved")

    # Combine all tickers
    combined = pd.concat(all_data, ignore_index=True)
    combined = combined[['date', 'ticker', 'adj_close', 'log_return']]
    combined = combined.sort_values(['ticker', 'date'])

    logging.info(f"Total records: {len(combined)}")
    logging.info(f"Tickers retrieved: {combined['ticker'].nunique()}")

    return combined


def validate_price_data(df: pd.DataFrame, config: dict):
    """
    Validate price data and check for issues.

    Args:
        df: Price DataFrame
        config: Configuration dictionary
    """
    logging.info("\n=== Data Validation ===")

    # Check each ticker
    for ticker in config['tickers'] + [config['market_index']]:
        ticker_data = df[df['ticker'] == ticker]

        if ticker_data.empty:
            logging.warning(f"{ticker}: NO DATA")
            continue

        # Check date range
        start = ticker_data['date'].min()
        end = ticker_data['date'].max()
        n_days = len(ticker_data)

        logging.info(f"{ticker}: {n_days} days, {start.date()} to {end.date()}")

        # Check for missing values
        missing = ticker_data['log_return'].isna().sum()
        if missing > 1:  # First return is always NaN
            logging.warning(f"  {missing} missing returns")

        # Check for extreme returns (potential data errors)
        returns = ticker_data['log_return'].dropna()
        if len(returns) > 0:
            extreme = returns[(returns.abs() > 0.2)]  # >20% daily move
            if len(extreme) > 0:
                logging.warning(f"  {len(extreme)} extreme daily returns (>20%)")

    # Check ticker start dates from config
    if 'ticker_start_dates' in config:
        logging.info("\n=== Ticker IPO Dates ===")
        for ticker, ipo_date in config['ticker_start_dates'].items():
            ticker_data = df[df['ticker'] == ticker]
            if not ticker_data.empty:
                actual_start = ticker_data['date'].min()
                logging.info(f"{ticker}: Expected {ipo_date}, Actual {actual_start.date()}")


def main():
    """Main execution function."""
    # Setup
    setup_logging()
    config = load_config()

    # Get all tickers including market index
    all_tickers = config['tickers'] + [config['market_index']]

    # Fetch prices
    prices_df = fetch_prices(
        tickers=all_tickers,
        start_date=config['start_date'],
        end_date=config['end_date']
    )

    # Validate
    validate_price_data(prices_df, config)

    # Save
    save_dataset(prices_df, 'stock_prices', processed=False)

    logging.info("\n=== Price data fetch complete ===")
    logging.info(f"Saved {len(prices_df)} records to data/raw/stock_prices.parquet")

    # Print summary statistics
    logging.info("\n=== Summary Statistics ===")
    summary = prices_df.groupby('ticker').agg({
        'date': ['min', 'max', 'count'],
        'log_return': ['mean', 'std']
    }).round(6)
    logging.info(f"\n{summary}")


if __name__ == "__main__":
    main()
