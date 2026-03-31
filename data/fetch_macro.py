"""
Person A - Week 2: Fetch macroeconomic indicators from FRED

Downloads macro data (interest rates, VIX, oil prices, etc.) from FRED API
and aligns to trading days.
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
from fredapi import Fred
from datetime import datetime
import logging
from utils import load_config, load_secrets, setup_logging, save_dataset, align_to_trading_days


def fetch_fred_data(fred_api_key: str, indicators: dict, start_date: str, end_date: str = None) -> pd.DataFrame:
    """
    Fetch macroeconomic indicators from FRED.

    Args:
        fred_api_key: FRED API key
        indicators: Dictionary of {series_id: description}
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format (default: today)

    Returns:
        DataFrame with columns: date, indicator, value
    """
    if end_date is None:
        end_date = datetime.today().strftime('%Y-%m-%d')

    fred = Fred(api_key=fred_api_key)

    logging.info(f"Fetching {len(indicators)} indicators from FRED")
    logging.info(f"Date range: {start_date} to {end_date}")

    all_data = []

    for series_id, description in indicators.items():
        logging.info(f"Fetching {series_id}: {description}")

        try:
            # Fetch series
            series = fred.get_series(series_id, start_date, end_date)

            if series.empty:
                logging.warning(f"  No data found for {series_id}")
                continue

            # Convert to DataFrame
            df = series.to_frame(name='value')
            df['indicator'] = series_id
            df['date'] = df.index
            df = df.reset_index(drop=True)

            all_data.append(df)

            logging.info(f"  Retrieved {len(df)} observations")
            logging.info(f"  Date range: {df['date'].min().date()} to {df['date'].max().date()}")
            logging.info(f"  Missing values: {df['value'].isna().sum()}")

        except Exception as e:
            logging.error(f"Error fetching {series_id}: {e}")
            continue

    if not all_data:
        raise ValueError("No data was successfully retrieved from FRED")

    # Combine all indicators
    combined = pd.concat(all_data, ignore_index=True)
    combined = combined[['date', 'indicator', 'value']]
    combined = combined.sort_values(['indicator', 'date'])

    logging.info(f"\nTotal records: {len(combined)}")
    logging.info(f"Indicators retrieved: {combined['indicator'].nunique()}")

    return combined


def align_macro_to_trading_days(macro_df: pd.DataFrame) -> pd.DataFrame:
    """
    Align macro indicators to trading days and forward-fill missing values.

    Args:
        macro_df: DataFrame with columns: date, indicator, value

    Returns:
        DataFrame aligned to trading days
    """
    logging.info("Aligning macro data to trading days...")

    aligned_data = []

    for indicator in macro_df['indicator'].unique():
        df = macro_df[macro_df['indicator'] == indicator].copy()
        df = df[['date', 'value']].drop_duplicates(subset='date')

        # Convert to daily frequency and forward-fill
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        df = df.asfreq('D', method='ffill')

        # Remove weekends (approximate - doesn't handle holidays)
        df = df[df.index.weekday < 5]

        df['indicator'] = indicator
        df = df.reset_index()

        aligned_data.append(df)

    combined = pd.concat(aligned_data, ignore_index=True)
    combined = combined[['date', 'indicator', 'value']]

    logging.info(f"Aligned to {len(combined)} records")

    return combined


def pivot_macro_data(macro_df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot macro data from long to wide format.

    Args:
        macro_df: DataFrame with columns: date, indicator, value

    Returns:
        DataFrame with columns: date, DGS10, BAMLH0A0HYM2, etc.
    """
    logging.info("Pivoting macro data to wide format...")

    pivoted = macro_df.pivot(index='date', columns='indicator', values='value')
    pivoted = pivoted.reset_index()

    logging.info(f"Pivoted shape: {pivoted.shape}")
    logging.info(f"Columns: {list(pivoted.columns)}")

    # Check for missing values
    missing = pivoted.isna().sum()
    if missing.any():
        logging.warning("Missing values after pivot:")
        logging.warning(missing[missing > 0])

    return pivoted


def validate_macro_data(df: pd.DataFrame, expected_indicators: dict):
    """
    Validate macro data quality.

    Args:
        df: Macro DataFrame (wide format)
        expected_indicators: Dictionary of expected indicators
    """
    logging.info("\n=== Macro Data Validation ===")

    for indicator in expected_indicators.keys():
        if indicator not in df.columns:
            logging.error(f"Missing indicator: {indicator}")
            continue

        series = df[indicator]

        # Basic stats
        logging.info(f"\n{indicator}:")
        logging.info(f"  Mean: {series.mean():.4f}")
        logging.info(f"  Std: {series.std():.4f}")
        logging.info(f"  Min: {series.min():.4f}")
        logging.info(f"  Max: {series.max():.4f}")
        logging.info(f"  Missing: {series.isna().sum()}")

        # Check for anomalies
        if indicator == 'VIXCLS':  # VIX
            if series.max() > 100:
                logging.warning(f"  Extremely high VIX reading: {series.max()}")
        elif indicator == 'DGS10':  # 10Y yield
            if series.min() < 0:
                logging.warning(f"  Negative interest rate: {series.min()}")
        elif indicator == 'DCOILWTICO':  # Oil
            if series.min() < 0:
                logging.warning(f"  Negative oil price: {series.min()}")


def main():
    """Main execution function."""
    # Setup
    setup_logging()
    config = load_config()
    secrets = load_secrets()

    # Get FRED API key
    fred_api_key = secrets.get('fred_api_key')
    if not fred_api_key:
        logging.error("FRED API key not found in secrets.yaml")
        logging.error("Please:")
        logging.error("1. Get a free API key from https://fred.stlouisfed.org/")
        logging.error("2. Copy config/secrets.yaml.example to config/secrets.yaml")
        logging.error("3. Add your API key to config/secrets.yaml")
        return

    # Fetch macro data
    macro_df = fetch_fred_data(
        fred_api_key=fred_api_key,
        indicators=config['fred_indicators'],
        start_date=config['start_date'],
        end_date=config['end_date']
    )

    # Align to trading days
    macro_aligned = align_macro_to_trading_days(macro_df)

    # Save long format
    save_dataset(macro_aligned, 'macro_indicators_long', processed=False)

    # Pivot to wide format
    macro_wide = pivot_macro_data(macro_aligned)

    # Validate
    validate_macro_data(macro_wide, config['fred_indicators'])

    # Save wide format (this is what models will use)
    save_dataset(macro_wide, 'macro_indicators', processed=False)

    logging.info("\n=== Macro data fetch complete ===")
    logging.info(f"Saved {len(macro_wide)} records to data/raw/macro_indicators.parquet")


if __name__ == "__main__":
    main()
