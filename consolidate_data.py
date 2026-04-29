#!/usr/bin/env python3
"""
Consolidate all data sources (stock prices, macro indicators, GDELT events,
macro returns, and stock log returns) into a single organized file by date.

Multiple GDELT events on the same day are rolled up into one record.
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

import pandas as pd
import numpy as np
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_all_data():
    """Load all data sources."""
    logger.info("Loading data sources...")
    
    # Load stock prices
    try:
        df_prices = pd.read_parquet('data/raw/stock_prices.parquet')
        logger.info(f"  Stock prices: {df_prices.shape[0]} rows")
    except Exception as e:
        logger.error(f"  Failed to load stock prices: {e}")
        df_prices = pd.DataFrame()
    
    # Load macro indicators
    try:
        df_macro = pd.read_parquet('data/raw/macro_indicators.parquet')
        logger.info(f"  Macro indicators: {df_macro.shape[0]} rows")
    except Exception as e:
        logger.error(f"  Failed to load macro indicators: {e}")
        df_macro = pd.DataFrame()
    
    # Load stock log returns
    try:
        df_stock_returns = pd.read_csv('data/processed/stock_log_returns.csv')
        logger.info(f"  Stock log returns: {df_stock_returns.shape[0]} rows")
    except Exception as e:
        logger.error(f"  Failed to load stock log returns: {e}")
        df_stock_returns = pd.DataFrame()
    
    # Load macro log returns
    try:
        df_macro_returns = pd.read_csv('data/processed/log_macro_returns.csv')
        logger.info(f"  Macro log returns: {df_macro_returns.shape[0]} rows")
    except Exception as e:
        logger.error(f"  Failed to load macro log returns: {e}")
        df_macro_returns = pd.DataFrame()
    
    # Load GDELT events - prioritize v2_dedup (most comprehensive modern version)
    # v2_dedup: Feb 2015-present, 15-minute resolution
    # v1_dedup: Historical data 1979-Feb 2015
    gdelt_files = [
        ('data/raw/gdelt_events_v2_dedup.parquet', 'GDELT v2 (Feb 2015+, 15-min resolution)'),
        ('data/raw/gdelt_events_v1_dedup.parquet', 'GDELT v1 (1979-Feb 2015)'),
        ('data/raw/gdelt_events_complete.parquet', 'GDELT complete'),
        ('data/raw/gdelt_events.parquet', 'GDELT small dataset'),
    ]
    
    df_gdelt = None
    for gdelt_file, description in gdelt_files:
        try:
            logger.info(f"  Loading {description}...")
            df = pd.read_parquet(gdelt_file)
            if df.shape[0] > 0:  # Only use if not empty
                logger.info(f"  ✓ Loaded {description}: {df.shape[0]:,} rows")
                df_gdelt = df
                break
            else:
                logger.info(f"  ✗ {description}: empty file")
        except Exception as e:
            logger.info(f"  ✗ {description}: {str(e)[:60]}")
            continue
    
    if df_gdelt is None:
        logger.warning("  No GDELT events data found")
        df_gdelt = pd.DataFrame()
    
    return df_prices, df_macro, df_stock_returns, df_macro_returns, df_gdelt


def normalize_dates(df, date_col='date'):
    """Normalize date columns to datetime64[ns]."""
    if df.empty:
        return df
    
    if date_col not in df.columns:
        return df
    
    # Convert to datetime if not already
    try:
        if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
            df[date_col] = pd.to_datetime(df[date_col], utc=True)
    except Exception:
        pass
    
    # Remove timezone info if present (convert to naive)
    if hasattr(df[date_col].dtype, 'tz') and df[date_col].dtype.tz is not None:
        df[date_col] = df[date_col].dt.tz_localize(None)
    
    return df


def safe_get_dates(series):
    """Safely extract dates from a series, handling both aware and naive datetimes."""
    try:
        # If already datetime, use it
        if pd.api.types.is_datetime64_any_dtype(series):
            dates = series
        else:
            # Try to convert, handling mixed timezone cases
            dates = pd.to_datetime(series, utc=True)
        
        # Convert to naive if needed
        if hasattr(dates.dtype, 'tz') and dates.dtype.tz is not None:
            dates = dates.dt.tz_localize(None)
        
        return dates.dt.date
    except Exception as e:
        logger.warning(f"Error extracting dates: {e}")
        return []


def consolidate_gdelt_events(df_gdelt):
    """
    Roll up multiple GDELT events on the same day into one record.
    
    Returns a dataframe with one row per date, with aggregated event information.
    """
    if df_gdelt.empty:
        logger.warning("  No GDELT events to consolidate")
        return pd.DataFrame()
    
    logger.info("  Consolidating GDELT events by date...")
    
    df_gdelt = normalize_dates(df_gdelt, 'date')
    
    # Group by date and aggregate events
    date_grouper = safe_get_dates(df_gdelt['date'])
    
    grouped = df_gdelt.groupby(date_grouper).agg({
        'actor1': lambda x: ' | '.join(x.dropna().unique()),
        'actor2': lambda x: ' | '.join(x.dropna().unique()),
        'event_code': lambda x: ' | '.join(x.dropna().unique()),
        'event_root_code': lambda x: ' | '.join(x.dropna().unique()),
        'event_description': lambda x: ' | '.join(x.dropna().unique()),
        'event_category': lambda x: ' | '.join(x.dropna().unique()),
        'goldstein_scale': 'mean',
        'num_mentions': 'sum',
        'avg_tone': 'mean',
        'source_url': lambda x: ' | '.join(x.dropna().unique()[:5]),  # Top 5 URLs
        'event_id': 'count'  # Count of events per day
    }).reset_index()
    
    # Rename columns
    grouped.columns = ['date', 'actors_1', 'actors_2', 'event_codes', 'event_root_codes',
                       'event_descriptions', 'event_categories', 'avg_goldstein_scale',
                       'total_mentions', 'avg_tone', 'source_urls', 'event_count']
    
    # Convert date back to datetime
    grouped['date'] = pd.to_datetime(grouped['date'])
    
    logger.info(f"  Consolidated {len(df_gdelt)} events into {len(grouped)} days")
    
    return grouped


def consolidate_data(df_prices, df_macro, df_stock_returns, df_macro_returns, df_gdelt):
    """
    Consolidate all data sources into a single dataframe organized by date.
    """
    logger.info("Consolidating data by date...")
    
    # Normalize all dates
    df_prices = normalize_dates(df_prices, 'date')
    df_macro = normalize_dates(df_macro, 'date')
    df_stock_returns = normalize_dates(df_stock_returns, 'date')
    df_macro_returns = normalize_dates(df_macro_returns, 'date')
    
    # Consolidate GDELT events
    df_gdelt_consolidated = consolidate_gdelt_events(df_gdelt)
    
    # Get all unique dates
    all_dates = set()
    for df in [df_prices, df_macro, df_stock_returns, df_macro_returns, df_gdelt_consolidated]:
        if not df.empty and 'date' in df.columns:
            dates = safe_get_dates(df['date'])
            all_dates.update(dates)
    
    all_dates = sorted(list(all_dates))
    logger.info(f"  Total unique dates: {len(all_dates)}")
    
    # Create base dataframe with all dates
    consolidated = pd.DataFrame({'date': [pd.Timestamp(d) for d in all_dates]})
    
    # Aggregate stock prices by date
    if not df_prices.empty:
        logger.info("  Aggregating stock prices by date...")
        date_grouper = safe_get_dates(df_prices['date'])
        
        prices_agg = df_prices.groupby(date_grouper).agg({
            'ticker': lambda x: ','.join(x.unique()),
            'adj_close': lambda x: ','.join(x.astype(str))
        }).reset_index()
        prices_agg.columns = ['date', 'price_tickers', 'price_adj_close']
        prices_agg['date'] = pd.to_datetime(prices_agg['date'])
        consolidated = consolidated.merge(prices_agg, on='date', how='left')
    
    # Aggregate stock log returns by date
    if not df_stock_returns.empty:
        logger.info("  Aggregating stock log returns by date...")
        date_grouper = safe_get_dates(df_stock_returns['date'])
        
        stock_ret_agg = df_stock_returns.groupby(date_grouper).agg({
            'ticker': lambda x: ','.join(x.unique()),
            'log_return_daily': lambda x: ','.join(x.astype(str)),
            'log_return_weekly': lambda x: ','.join(x.astype(str)),
            'log_return_monthly': lambda x: ','.join(x.astype(str)),
            'log_return_yearly': lambda x: ','.join(x.astype(str)),
            'log_return_ytd': lambda x: ','.join(x.astype(str))
        }).reset_index()
        stock_ret_agg.columns = ['date', 'stock_return_tickers', 'stock_log_return_daily',
                                  'stock_log_return_weekly', 'stock_log_return_monthly',
                                  'stock_log_return_yearly', 'stock_log_return_ytd']
        stock_ret_agg['date'] = pd.to_datetime(stock_ret_agg['date'])
        consolidated = consolidated.merge(stock_ret_agg, on='date', how='left')
    
    # Aggregate macro indicators by date
    if not df_macro.empty:
        logger.info("  Aggregating macro indicators by date...")
        macro_cols = [c for c in df_macro.columns if c != 'date']
        date_grouper = safe_get_dates(df_macro['date'])
        
        macro_agg = df_macro.groupby(date_grouper)[macro_cols].first().reset_index()
        macro_agg.columns = ['date'] + [f'macro_{c}' for c in macro_cols]
        macro_agg['date'] = pd.to_datetime(macro_agg['date'])
        consolidated = consolidated.merge(macro_agg, on='date', how='left')
    
    # Aggregate macro log returns by date
    if not df_macro_returns.empty:
        logger.info("  Aggregating macro log returns by date...")
        date_grouper = safe_get_dates(df_macro_returns['date'])
        
        macro_ret_agg = df_macro_returns.groupby(date_grouper).agg({
            'ticker': lambda x: ','.join(x.unique()),
            'log_return_daily': lambda x: ','.join(x.astype(str)),
            'log_return_weekly': lambda x: ','.join(x.astype(str)),
            'log_return_monthly': lambda x: ','.join(x.astype(str)),
            'log_return_yearly': lambda x: ','.join(x.astype(str)),
            'log_return_ytd': lambda x: ','.join(x.astype(str))
        }).reset_index()
        macro_ret_agg.columns = ['date', 'macro_return_tickers', 'macro_log_return_daily',
                                  'macro_log_return_weekly', 'macro_log_return_monthly',
                                  'macro_log_return_yearly', 'macro_log_return_ytd']
        macro_ret_agg['date'] = pd.to_datetime(macro_ret_agg['date'])
        consolidated = consolidated.merge(macro_ret_agg, on='date', how='left')
    
    # Add GDELT consolidated events
    if not df_gdelt_consolidated.empty:
        logger.info("  Adding consolidated GDELT events...")
        consolidated = consolidated.merge(df_gdelt_consolidated, on='date', how='left')
    
    # Sort by date
    consolidated = consolidated.sort_values('date').reset_index(drop=True)
    
    return consolidated


def main():
    """Main entry point."""
    logger.info("Starting data consolidation...")
    
    # Load all data
    df_prices, df_macro, df_stock_returns, df_macro_returns, df_gdelt = load_all_data()
    
    # Consolidate
    consolidated = consolidate_data(df_prices, df_macro, df_stock_returns, df_macro_returns, df_gdelt)
    
    # Save to both CSV and Parquet
    output_csv = 'data/processed/consolidated_data.csv'
    output_parquet = 'data/processed/consolidated_data.parquet'
    
    logger.info(f"Saving consolidated data to {output_csv}...")
    consolidated.to_csv(output_csv, index=False)
    
    logger.info(f"Saving consolidated data to {output_parquet}...")
    consolidated.to_parquet(output_parquet, index=False)
    
    logger.info(f"Consolidation complete!")
    logger.info(f"  Consolidated shape: {consolidated.shape}")
    logger.info(f"  Date range: {consolidated['date'].min()} to {consolidated['date'].max()}")
    logger.info(f"  Columns: {consolidated.shape[1]}")
    
    # Display sample
    logger.info("\nSample of consolidated data (first 5 rows):")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(consolidated.head())
    
    return consolidated


if __name__ == '__main__':
    main()
