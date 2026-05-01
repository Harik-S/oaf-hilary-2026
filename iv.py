# Task 4 and 5, adding atm_iv and iv_rank and signal generation to session_signals data
# run after data collection

import pandas as pd
import numpy as np
session_signals = pd.read_csv("data/sessions.csv")
options_df = pd.read_csv("data/options_df.csv")

session_start=session_signals["session_start"].to_list()
session_type=session_signals["session_type"].to_list()
session_dow=session_signals["session_dow"].to_list()
iv=options_df["entry_iv"].to_list()
atm_iv=[]

# Options considered loop through: ATM call, ATM put, OTM call, OTM put 
# so 4i and 4i + 1 are ATM indices
# The IVs should be nearby (put-call parity)
for i in range(len(session_start)):
    atm_iv.append((iv[4*i]+iv[4*i+1])/2)

# IV Rank
# For each row, compute iv_52w_low and iv_52w_high as the min/max of atm_iv
# over the trailing 52 weeks of same-session-type rows.
# Weekend IVR benchmarks against past weekend IVs only, not weekday.

session_signals["session_start"] = pd.to_datetime(session_signals["session_start"], utc=True)
session_signals["session_close"] = pd.to_datetime(session_signals["session_close"], utc=True)
session_signals["atm_iv"] = atm_iv

session_signals = session_signals.sort_values("session_start")

session_signals["iv_52w_low"] = pd.NA
session_signals["iv_52w_high"] = pd.NA
session_signals["iv_rank"] = pd.NA

for stype in session_signals["session_type"].unique():
    mask = session_signals["session_type"] == stype
    subset = session_signals.loc[mask].copy()
    subset = subset.set_index("session_start").sort_index()

    stats = subset["atm_iv"].rolling("364D", min_periods=1).agg(["min", "max"])
    stats.columns = ["iv_52w_low", "iv_52w_high"]

    session_signals.loc[mask, ["iv_52w_low", "iv_52w_high"]] = stats[["iv_52w_low", "iv_52w_high"]].to_numpy()

# EPS x1 in num and x2 in denom to make default value 50%
EPS = 1e-6
session_signals["iv_rank"] = (session_signals["atm_iv"] - session_signals["iv_52w_low"] + EPS) / \
    (session_signals["iv_52w_high"] - session_signals["iv_52w_low"] + 2 * EPS) * 100.

# VRP (atm_iv - rv_at_open)
# RV at open comes from btc_prices.csv, want to merge using some sort of btc_prices.loc[time]
ALPHA = 0.2
btc_prices = pd.read_csv("data/btc_prices.csv")
btc_prices.rename(columns={"Unnamed: 0": "session_start"}, inplace=True)
btc_prices["session_start"] = pd.to_datetime(btc_prices["session_start"], utc=True)

btc_prices["log_return"] = np.log(
    btc_prices["close"] / btc_prices["close"].shift(1)
)
btc_prices["squared_log_return"] = btc_prices["log_return"] ** 2


# Step 2: 1-day realized volatility (24 hours)
btc_prices["rv_1d"] = (
    np.sqrt((btc_prices["log_return"]**2).rolling(24).mean())
    * np.sqrt(365 * 24)
)
btc_prices["rv_1d_forward"] = (
    np.sqrt(
        btc_prices["squared_log_return"]
        .shift(-1)
        .rolling(24)
        .mean()
        .shift(-23)
    )
    * np.sqrt(365 * 24)
)
btc_prices["session_hour"] = btc_prices["session_start"].dt.hour
btc_prices["day_of_week"] = btc_prices["session_start"].dt.dayofweek

btc_prices["session_hour"] = btc_prices["session_start"].dt.hour
btc_prices["day_of_week"] = btc_prices["session_start"].dt.dayofweek

HIST_WINDOW = 5
MIN_HIST_PERIODS = 4

btc_prices["rv_historical_session_avg"] = (
    btc_prices
    .groupby(["session_hour", "day_of_week"])["rv_1d_forward"]
    .transform(
        lambda x: x.shift(1).rolling(
            HIST_WINDOW,
            min_periods=MIN_HIST_PERIODS
        ).mean()
    )
)
# Merge rv_5d from btc_prices into session_signals based on matching session_start
btc_prices["rv_benchmark"] = (
    ALPHA * btc_prices["rv_1d"]
    + (1 - ALPHA) * btc_prices["rv_historical_session_avg"]
)
session_signals = session_signals.merge(btc_prices[["session_start", "rv_5d", "rv_1d", "rv_historical_session_avg", "rv_benchmark"]], on="session_start", how="left")

session_signals["vrp"] = session_signals["atm_iv"] - session_signals["rv_benchmark"]
session_signals["vrp"]
VRP_RATIO_THRESHOLD = 1.20

session_signals["vrp_ratio"] = (
    session_signals["atm_iv"]
    / session_signals["rv_benchmark"]
)
# Trade condition 1: VRP > THRESHOLD
THRESHOLD = 0.05
session_signals['signal'] = (session_signals['vrp'] > THRESHOLD) & (session_signals['iv_rank'] > 30)

print(session_signals.describe())

# write results back (at end)
session_signals.to_csv("data/session_signals.csv", index=False)

