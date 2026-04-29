# Data Consolidation Script

## Overview
`consolidate_data.py` consolidates all data sources (stock prices, macro indicators, GDELT events, macro returns, and stock log returns) into a single organized file by date.

## Data Sources

The script ingests data from:

1. **Stock Prices** (`data/raw/stock_prices.parquet`)
   - Columns: date, ticker, adj_close, log_return
   - Multiple tickers per day (BA, GD, KTOS, LHX, LMT, NOC, RTX, SPY)

2. **Macro Indicators** (`data/raw/macro_indicators.parquet`)
   - Columns: date, BAMLH0A0HYM2, DCOILWTICO, DGS10, DTWEXBGS, VIXCLS
   - Economic indicators indexed by date

3. **Stock Log Returns** (`data/processed/stock_log_returns.csv`)
   - Multiple log return frequencies: daily, weekly, monthly, yearly, YTD
   - One row per stock per date

4. **Macro Log Returns** (`data/processed/log_macro_returns.csv`)
   - Multiple log return frequencies for macro indicators
   - One row per indicator per date

5. **GDELT Events** (`data/raw/gdelt_events*.parquet`)
   - Geopolitical events with event codes, actors, Goldstein scale, etc.
   - Multiple events may occur on the same day

## Data Consolidation Strategy

### Aggregation by Date
- All data is organized with one row per date
- Date range: 2000-01-03 to present

### GDELT Event Roll-Up
Multiple GDELT events on the same day are consolidated into a single record with:
- **actors_1**: All unique actor1 values joined by ` | `
- **actors_2**: All unique actor2 values joined by ` | `
- **event_codes**: All unique event codes joined by ` | `
- **event_root_codes**: All unique root codes joined by ` | `
- **event_descriptions**: All unique descriptions joined by ` | `
- **event_categories**: All unique categories joined by ` | `
- **avg_goldstein_scale**: Mean Goldstein scale across all events
- **total_mentions**: Sum of mentions across all events
- **avg_tone**: Mean tone across all events
- **source_urls**: Top 5 source URLs joined by ` | `
- **event_count**: Number of events on that day

### Stock & Macro Data Aggregation
- **Stock Prices**: All tickers and prices for that day, joined by commas
- **Stock Returns**: All tickers and returns (daily, weekly, monthly, yearly, YTD), joined by commas
- **Macro Indicators**: First value for that day (no time component)
- **Macro Returns**: All indicators and returns, joined by commas

## Output Files

Both CSV and Parquet formats are generated:

1. **consolidated_data.csv** (~10 MB)
   - Human-readable, compatible with Excel and pandas
   - Use for general analysis and inspection

2. **consolidated_data.parquet** (~7 MB)
   - Efficient columnar format
   - Recommended for large-scale analysis and machine learning

## Column Structure (31 total columns)

| Column | Source | Type | Description |
|--------|--------|------|-------------|
| date | All | datetime | Date (daily granularity) |
| price_tickers | Stock Prices | string | Comma-separated ticker symbols |
| price_adj_close | Stock Prices | string | Comma-separated adjusted close prices |
| stock_return_tickers | Stock Returns | string | Ticker symbols for stock returns |
| stock_log_return_daily | Stock Returns | string | Daily log returns (comma-separated) |
| stock_log_return_weekly | Stock Returns | string | Weekly log returns (comma-separated) |
| stock_log_return_monthly | Stock Returns | string | Monthly log returns (comma-separated) |
| stock_log_return_yearly | Stock Returns | string | Yearly log returns (comma-separated) |
| stock_log_return_ytd | Stock Returns | string | Year-to-date log returns (comma-separated) |
| macro_BAMLH0A0HYM2 | Macro Indicators | float | High Yield Option-Adjusted Spread |
| macro_DCOILWTICO | Macro Indicators | float | Oil prices (WTI Crude) |
| macro_DGS10 | Macro Indicators | float | 10-Year Treasury Yield |
| macro_DTWEXBGS | Macro Indicators | float | Broad Dollar Index |
| macro_VIXCLS | Macro Indicators | float | VIX Volatility Index |
| macro_return_tickers | Macro Returns | string | Indicator symbols for macro returns |
| macro_log_return_daily | Macro Returns | string | Daily macro returns (comma-separated) |
| macro_log_return_weekly | Macro Returns | string | Weekly macro returns (comma-separated) |
| macro_log_return_monthly | Macro Returns | string | Monthly macro returns (comma-separated) |
| macro_log_return_yearly | Macro Returns | string | Yearly macro returns (comma-separated) |
| macro_log_return_ytd | Macro Returns | string | YTD macro returns (comma-separated) |
| actors_1 | GDELT Events | string | Actor 1 (e.g., country/entity) - pipe-separated |
| actors_2 | GDELT Events | string | Actor 2 - pipe-separated |
| event_codes | GDELT Events | string | CAMEO event codes - pipe-separated |
| event_root_codes | GDELT Events | string | Root event codes - pipe-separated |
| event_descriptions | GDELT Events | string | Event descriptions - pipe-separated |
| event_categories | GDELT Events | string | Event categories - pipe-separated |
| avg_goldstein_scale | GDELT Events | float | Mean Goldstein scale (-10 to 10) |
| total_mentions | GDELT Events | int | Total number of event mentions |
| avg_tone | GDELT Events | float | Mean tone of events |
| source_urls | GDELT Events | string | Top 5 source URLs - pipe-separated |
| event_count | GDELT Events | int | Number of events consolidated on this date |

## Usage

Run the consolidation script:

```bash
python consolidate_data.py
```

The script will:
1. Load all source files
2. Consolidate GDELT events by date
3. Aggregate all data sources
4. Save to both CSV and Parquet formats
5. Print summary statistics and sample data

## Logging

The script includes detailed logging output showing:
- Data source loading progress
- Number of rows loaded from each source
- Consolidation steps and statistics
- Final output shape and date range
- Sample of the first 5 rows

## Key Features

- **Robust date handling**: Handles mixed timezone-aware and naive datetimes
- **Multiple data formats**: Supports parquet and CSV input/output
- **Graceful error handling**: Skips missing or empty data sources
- **Efficient aggregation**: Uses pandas groupby for fast consolidation
- **Comprehensive GDELT roll-up**: Preserves all event information while consolidating by date
- **CSV-friendly formatting**: Multi-value columns use comma or pipe separators for spreadsheet compatibility

## Example Query

To load and use the consolidated data:

```python
import pandas as pd

# Load consolidated data
df = pd.read_parquet('data/processed/consolidated_data.parquet')
# or
df = pd.read_csv('data/processed/consolidated_data.csv')

# Filter to a specific date range
df_2024 = df[(df['date'] >= '2024-01-01') & (df['date'] <= '2024-12-31')]

# Get stock returns for a date
date = '2024-06-15'
row = df[df['date'] == date].iloc[0]
tickers = row['stock_return_tickers'].split(',')
returns = [float(x) for x in row['stock_log_return_daily'].split(',')]
```

## Notes

- NaN values indicate missing data for that date (normal for GDELT events which don't occur every day)
- Multiple values in comma/pipe-separated columns are in the same order as their corresponding tickers
- For GDELT events, only dates with events will have non-NaN values in event columns
- All dates from the union of all data sources are included (outer join behavior)
