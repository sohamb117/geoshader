
from pathlib import Path
import numpy as np
import pandas as pd
import statsmodels.api as sm

# Load pilot price data
folder = Path("data/processed/rq1_pilot")
df = pd.read_csv(folder / "pilot_prices.csv")

# Preserve the US trading date despite daylight-saving offsets
df["date"] = pd.to_datetime(df["date"].str[:10])

tickers = ["LMT", "NOC", "GD"]
event_date = pd.Timestamp("2022-02-24")

# Verify that log returns are calculated correctly
for ticker in tickers + ["SPY"]:
    stock = df[df["ticker"] == ticker].sort_values("date")
    expected = np.log(
        stock["adj_close"] / stock["adj_close"].shift(1)
    )
    actual = stock["log_return"]

    if not np.allclose(actual, expected, equal_nan=True):
        raise ValueError(f"Return calculation mismatch: {ticker}")

# Align all stocks by trading date
returns = df.pivot(
    index="date",
    columns="ticker",
    values="log_return"
).sort_index()

returns = returns.dropna(subset=["SPY"])

if event_date not in returns.index:
    raise ValueError("Event date is missing!")

t0 = returns.index.get_loc(event_date)

# Estimation window: trading days -180 to -31
estimation = returns.iloc[t0-180:t0-30]

# Event window: trading days -1 to +10
event_window = returns.iloc[t0-1:t0+11]

if len(estimation) != 150 or len(event_window) != 12:
    raise ValueError("Insufficient trading-day coverage!")

results = []
daily_results = []

for ticker in tickers:

    train = estimation[[ticker, "SPY"]].dropna()

    if len(train) < 120:
        raise ValueError(f"Insufficient observations: {ticker}")

    X = sm.add_constant(train["SPY"])
    y = train[ticker]

    model = sm.OLS(y, X).fit()

    test = event_window[[ticker, "SPY"]].copy()

    if test.isna().any().any():
        raise ValueError(f"Missing event-window data: {ticker}")

    expected = (
        model.params["const"]
        + model.params["SPY"] * test["SPY"]
    )

    abnormal = test[ticker] - expected

    # Event day is position 1 in [-1, +10]
    car_01 = abnormal.iloc[1:3].sum()
    car_m11 = abnormal.iloc[0:3].sum()

    results.append({
        "ticker": ticker,
        "alpha": model.params["const"],
        "beta": model.params["SPY"],
        "estimation_r2": model.rsquared,
        "n_estimation": len(train),
        "CAR_0_1_pct": car_01 * 100,
        "CAR_minus1_1_pct": car_m11 * 100
    })

    for day, ar in abnormal.items():
        daily_results.append({
            "ticker": ticker,
            "date": day,
            "relative_day": returns.index.get_loc(day) - t0,
            "abnormal_return": ar
        })

summary = pd.DataFrame(results)

summary.to_csv(folder / "pilot_car_results.csv", index=False)
pd.DataFrame(daily_results).to_csv(
    folder / "pilot_daily_abnormal_returns.csv",
    index=False
)

print("\n=== RQ1 PILOT: MARKET MODEL RESULTS ===")
print(summary.round(5).to_string(index=False))

print("\nEstimation window:",
      estimation.index.min().date(),
      "to", estimation.index.max().date())

print("\nEvent date:", event_date.date())
print("\nCAR computation completed successfully!")
