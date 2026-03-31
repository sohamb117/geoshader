import numpy as np
import pandas as pd

# Assume `adj_close` is a DataFrame with dates as index, tickers as columns:
#
#             LMT     RTX     SPY  ...
# 2000-01-03  20.00   12.00  146.0
# 2000-01-04  19.46   11.80  144.2
# ...

def compute_log_returns(adj_close):
    """
    Compute log returns at multiple frequencies from adjusted close prices.

    Args:
        adj_close: DataFrame with dates as index and tickers as columns
    """

    # 1. Compute log returns at multiple frequencies
    # Daily log returns
    log_ret_daily = pd.DataFrame(
        np.log(adj_close / adj_close.shift(1)),
        index=adj_close.index,
        columns=adj_close.columns
    )

    # Weekly log returns (shift by 5 business days)
    log_ret_weekly = pd.DataFrame(
        np.log(adj_close / adj_close.shift(5)),
        index=adj_close.index,
        columns=adj_close.columns
    )

    # Monthly log returns (shift by ~21 business days)
    log_ret_monthly = pd.DataFrame(
        np.log(adj_close / adj_close.shift(21)),
        index=adj_close.index,
        columns=adj_close.columns
    )

    # Yearly log returns (shift by ~252 business days)
    log_ret_yearly = pd.DataFrame(
        np.log(adj_close / adj_close.shift(252)),
        index=adj_close.index,
        columns=adj_close.columns
    )

    # YTD log returns - compute from start of each calendar year
    def compute_ytd_returns(df):
        result = df.copy()
        for year in df.index.year.unique():
            year_mask = df.index.year == year
            year_data = df[year_mask]
            first_date = year_data.index[0]
            first_prices = df.loc[first_date]
            result.loc[year_mask] = np.log(df.loc[year_mask] / first_prices)
        return result

    log_ret_ytd = compute_ytd_returns(adj_close)

    # 2. Melt adj_close from wide → long
    adj_long = (
        adj_close
        .reset_index()                                   # date becomes a column
        .melt(id_vars="date", var_name="ticker", value_name="adj_close")
    )

    # 3. Melt log returns for each frequency
    ret_daily = (
        log_ret_daily
        .reset_index()
        .melt(id_vars="date", var_name="ticker", value_name="log_return_daily")
    )

    ret_weekly = (
        log_ret_weekly
        .reset_index()
        .melt(id_vars="date", var_name="ticker", value_name="log_return_weekly")
    )

    ret_monthly = (
        log_ret_monthly
        .reset_index()
        .melt(id_vars="date", var_name="ticker", value_name="log_return_monthly")
    )

    ret_yearly = (
        log_ret_yearly
        .reset_index()
        .melt(id_vars="date", var_name="ticker", value_name="log_return_yearly")
    )

    ret_ytd = (
        log_ret_ytd
        .reset_index()
        .melt(id_vars="date", var_name="ticker", value_name="log_return_ytd")
    )

    # 4. Join them together
    df = pd.merge(adj_long, ret_daily, on=["date", "ticker"])
    df = pd.merge(df, ret_weekly, on=["date", "ticker"])
    df = pd.merge(df, ret_monthly, on=["date", "ticker"])
    df = pd.merge(df, ret_yearly, on=["date", "ticker"])
    df = pd.merge(df, ret_ytd, on=["date", "ticker"])

    # 5. Drop rows where adj_close is NaN (pre-IPO dates for PLTR, KTOS, etc.)
    df = df.dropna(subset=["adj_close"])

    # 6. Reorder columns, sort, and save
    df = df[["date", "ticker", "adj_close", "log_return_daily", "log_return_weekly", 
            "log_return_monthly", "log_return_yearly", "log_return_ytd"]]
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    return df 

df_long = pd.read_parquet("C:/Users/daind/Documents/GitHub/geoshader/data/raw/macro_indicators_long.parquet")

adj_close = df_long.pivot(index = "date", columns = "indicator", values = "value")

log_returns_df = compute_log_returns(adj_close)
log_returns_df.to_csv("data/processed/log_macro_returns.csv", index=False)

adj_close = pd.read_parquet("C:/Users/daind/Documents/GitHub/geoshader/data/raw/stock_prices.parquet")
adj_close = adj_close.pivot(index="date", columns="ticker", values="adj_close")

log_returns_df = compute_log_returns(adj_close)
log_returns_df.to_csv("data/processed/stock_log_returns.csv", index=False)