"""
Utility functions for the geopolitical defense equity study.
"""

import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional
import pandas as pd
import numpy as np


def load_config(config_path: str = "config/config.yaml") -> Dict[str, Any]:
    """Load configuration from YAML file."""
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config


def load_secrets(secrets_path: str = "config/secrets.yaml") -> Dict[str, Any]:
    """Load secrets from YAML file."""
    try:
        with open(secrets_path, 'r') as f:
            secrets = yaml.safe_load(f)
        return secrets
    except FileNotFoundError:
        logging.warning(f"Secrets file not found at {secrets_path}. Using defaults.")
        return {}


def setup_logging(log_level: str = "INFO", log_dir: str = "logs"):
    """Set up logging configuration."""
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"{log_dir}/geoshader.log"),
            logging.StreamHandler()
        ]
    )


def get_trading_days(start_date: str, end_date: str) -> pd.DatetimeIndex:
    """
    Get trading days between start and end dates.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        DatetimeIndex of trading days
    """
    # Create a date range
    dates = pd.date_range(start=start_date, end=end_date, freq='D')

    # Remove weekends
    dates = dates[dates.weekday < 5]

    # Note: This doesn't account for market holidays
    # For production, use pandas_market_calendars or similar
    return dates


def calculate_log_returns(prices: pd.Series) -> pd.Series:
    """
    Calculate log returns from prices.

    Args:
        prices: Series of prices

    Returns:
        Series of log returns
    """
    return np.log(prices / prices.shift(1))


def winsorize(data: pd.Series, lower: float = 0.01, upper: float = 0.99) -> pd.Series:
    """
    Winsorize data to handle outliers.

    Args:
        data: Series to winsorize
        lower: Lower quantile
        upper: Upper quantile

    Returns:
        Winsorized series
    """
    lower_bound = data.quantile(lower)
    upper_bound = data.quantile(upper)
    return data.clip(lower=lower_bound, upper=upper_bound)


def align_to_trading_days(df: pd.DataFrame, date_column: str = 'date') -> pd.DataFrame:
    """
    Align data to trading days by forward-filling missing values.

    Args:
        df: DataFrame with date column
        date_column: Name of the date column

    Returns:
        DataFrame aligned to trading days
    """
    df = df.copy()
    df[date_column] = pd.to_datetime(df[date_column])
    df = df.set_index(date_column)

    # Create full trading day index
    full_index = get_trading_days(df.index.min(), df.index.max())

    # Reindex and forward fill
    df = df.reindex(full_index)
    df = df.fillna(method='ffill')

    return df.reset_index().rename(columns={'index': date_column})


def validate_data(df: pd.DataFrame, required_columns: list) -> bool:
    """
    Validate that DataFrame has required columns and no NaN values.

    Args:
        df: DataFrame to validate
        required_columns: List of required column names

    Returns:
        True if validation passes

    Raises:
        ValueError: If validation fails
    """
    # Check required columns
    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Check for NaN values
    nan_cols = df[required_columns].columns[df[required_columns].isna().any()].tolist()
    if nan_cols:
        raise ValueError(f"NaN values found in columns: {nan_cols}")

    return True


def create_event_id(row: pd.Series) -> str:
    """
    Create unique event ID from row data.

    Args:
        row: Row from events DataFrame

    Returns:
        Unique event ID string
    """
    date_str = pd.to_datetime(row['date']).strftime('%Y%m%d')
    actor1 = str(row.get('actor1', '')).replace(' ', '_')[:20]
    actor2 = str(row.get('actor2', '')).replace(' ', '_')[:20]
    event_code = str(row.get('event_code', ''))

    return f"{date_str}_{actor1}_{actor2}_{event_code}"


def save_dataset(df: pd.DataFrame, filename: str, processed: bool = True):
    """
    Save dataset to parquet format.

    Args:
        df: DataFrame to save
        filename: Name of file (without extension)
        processed: If True, save to processed/, else save to raw/
    """
    subdir = "processed" if processed else "raw"
    path = Path(f"data/{subdir}/{filename}.parquet")
    path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(path, index=False)
    logging.info(f"Saved {len(df)} rows to {path}")


def load_dataset(filename: str, processed: bool = True) -> pd.DataFrame:
    """
    Load dataset from parquet format.

    Args:
        filename: Name of file (without extension)
        processed: If True, load from processed/, else load from raw/

    Returns:
        DataFrame
    """
    subdir = "processed" if processed else "raw"
    path = Path(f"data/{subdir}/{filename}.parquet")

    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")

    df = pd.read_parquet(path)
    logging.info(f"Loaded {len(df)} rows from {path}")
    return df


def set_random_seed(seed: int = 42):
    """Set random seed for reproducibility."""
    import random
    import torch

    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)

    logging.info(f"Random seed set to {seed}")
