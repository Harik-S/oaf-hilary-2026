# Task 4 and 5, adding atm_iv and iv_rank and signal generation to session_signals data
# run after data collection
import pandas as pd
import numpy as np
import statsmodels.api as sm

session_signals = pd.read_csv("data/sessions.csv")
options_df = pd.read_csv("data/options_df.csv")
btc_prices = pd.read_csv("data/btc_prices.csv")

for df in (session_signals, options_df, btc_prices):
    if "session_start" in df.columns:
        df["session_start"] = pd.to_datetime(df["session_start"], utc=True, errors="coerce")

if "session_close" in session_signals.columns:
    session_signals["session_close"] = pd.to_datetime(session_signals["session_close"], utc=True, errors="coerce")

if "Unnamed: 0" in btc_prices.columns:
    btc_prices = btc_prices.rename(columns={"Unnamed: 0": "session_start"})

session_signals = session_signals.sort_values("session_start").reset_index(drop=True)
options_df = options_df.sort_values("session_start").reset_index(drop=True)
btc_prices = btc_prices.sort_values("session_start").reset_index(drop=True)

if session_signals["session_start"].isna().any():
    raise ValueError("session_signals contains invalid session_start values.")
if options_df["session_start"].isna().any():
    raise ValueError("options_df contains invalid session_start values.")
if btc_prices["session_start"].isna().any():
    raise ValueError("btc_prices contains invalid session_start values.")

required_option_cols = {"session_start", "leg", "entry_iv"}
missing_option_cols = required_option_cols - set(options_df.columns)
if missing_option_cols:
    raise ValueError(f"options_df missing required columns: {missing_option_cols}")

atm_mask = options_df["leg"].isin(["atm_call", "atm_put"])

atm_iv = (
    options_df.loc[atm_mask]
    .groupby("session_start", as_index=True)["entry_iv"]
    .mean()
    .rename("atm_iv")
)

session_signals = session_signals.merge(
    atm_iv,
    on="session_start",
    how="left",
    validate="one_to_one"
)

if session_signals["atm_iv"].isna().any():
    missing = session_signals.loc[session_signals["atm_iv"].isna(), "session_start"].unique()
    raise ValueError(f"Missing ATM IV for session_start values: {missing}")

def add_iv_rank(group: pd.DataFrame) -> pd.DataFrame:
    g = group.sort_values("session_start").copy()
    g = g.set_index("session_start")

    # History only: shift(1) before rolling
    hist = g["atm_iv"].shift(1).rolling("364D", min_periods=20)
    g["iv_52w_low"] = hist.min()
    g["iv_52w_high"] = hist.max()

    denom = g["iv_52w_high"] - g["iv_52w_low"]
    g["iv_rank"] = np.where(
        denom.abs() < 1e-12,
        50.0,
        (g["atm_iv"] - g["iv_52w_low"]) / denom * 100.0
    )

    return g.reset_index()

if "session_type" not in session_signals.columns:
    raise ValueError("session_signals must contain 'session_type'.")

session_signals = (
    session_signals
    .groupby("session_type", group_keys=False)
    .apply(add_iv_rank)
    .sort_values("session_start")
    .reset_index(drop=True)
)

required_btc_cols = {"session_start", "close"}
missing_btc_cols = required_btc_cols - set(btc_prices.columns)
if missing_btc_cols:
    raise ValueError(f"btc_prices missing required columns: {missing_btc_cols}")

btc_prices = btc_prices.sort_values("session_start").reset_index(drop=True)

if "log_returns" not in btc_prices.columns:
    btc_prices["log_returns"] = np.log(btc_prices["close"] / btc_prices["close"].shift(1))

btc_prices["squared_log_returns"] = btc_prices["log_returns"] ** 2

# Daily realised volatility from hourly data (annualised)

btc_prices_dt = btc_prices.set_index("session_start")
if not isinstance(btc_prices_dt.index, pd.DatetimeIndex):
    btc_prices_dt.index = pd.to_datetime(btc_prices_dt.index, utc=True, errors="coerce")

daily_sq = (
    btc_prices_dt["squared_log_returns"]
    .resample("1D")
    .sum()
    .rename("rv2_daily")
)

har_df = pd.DataFrame(index=daily_sq.index)
har_df["rv_daily"] = np.sqrt(daily_sq) * np.sqrt(365)

# HAR features: trailing daily, weekly, monthly averages
har_df["rv_d"] = har_df["rv_daily"].shift(1)
har_df["rv_w"] = har_df["rv_daily"].shift(1).rolling(7, min_periods=7).mean()
har_df["rv_m"] = har_df["rv_daily"].shift(1).rolling(30, min_periods=30).mean()

# One-step-ahead target
har_df["rv_next_1d"] = har_df["rv_daily"].shift(-1)
har_df["rv_har_forecast_1d"] = np.nan

MIN_TRAIN = 90

# Expanding HAR fit: forecast each day using only historical data
for i in range(MIN_TRAIN, len(har_df) - 1):
    train = har_df.iloc[:i + 1].dropna(subset=["rv_d", "rv_w", "rv_m", "rv_next_1d"]).copy()
    if len(train) < MIN_TRAIN:
        continue

    X_train = sm.add_constant(train[["rv_d", "rv_w", "rv_m"]], has_constant="add")
    y_train = train["rv_next_1d"]

    model = sm.OLS(y_train, X_train).fit()

    x_pred = sm.add_constant(
        har_df[["rv_d", "rv_w", "rv_m"]].iloc[[i]],
        has_constant="add"
    )

    forecast = float(model.predict(x_pred).iloc[0])
    har_df.iloc[i + 1, har_df.columns.get_loc("rv_har_forecast_1d")] = max(forecast, 0.0)

har_df = har_df.reset_index()
har_df = har_df.rename(columns={har_df.columns[0]: "forecast_date"})

# Merge HAR forecast onto session dates
session_signals["forecast_date"] = session_signals["session_start"].dt.floor("D")

session_signals = session_signals.merge(
    har_df[["forecast_date", "rv_har_forecast_1d", "rv_daily", "rv_next_1d"]],
    on="forecast_date",
    how="left",
    validate="many_to_one"
)

session_signals["rv_signal"] = session_signals["rv_har_forecast_1d"].fillna(session_signals["rv_daily"])

session_signals["vrp"] = session_signals["atm_iv"] - session_signals["rv_signal"]

session_signals["vrp_real"] = session_signals["atm_iv"] - session_signals["rv_next_1d"]

session_signals["vrp_ratio"] = np.where(
    session_signals["rv_signal"].abs() < 1e-12,
    np.nan,
    session_signals["atm_iv"] / session_signals["rv_signal"]
)

VRP_Z_WINDOW = 50

session_signals["vrp_mean"] = (
    session_signals["vrp"]
    .rolling(VRP_Z_WINDOW, min_periods=20)
    .mean()
    .shift(1)
)
session_signals["vrp_std"] = (
    session_signals["vrp"]
    .rolling(VRP_Z_WINDOW, min_periods=20)
    .std(ddof=1)
    .shift(1)
)

session_signals["vrp_z"] = np.where(
    session_signals["vrp_std"].abs() < 1e-12,
    np.nan,
    (session_signals["vrp"] - session_signals["vrp_mean"]) / session_signals["vrp_std"]
)

Z_THRESHOLD = 1.0

session_signals["signal_short"] = (
    (session_signals["vrp_z"] > Z_THRESHOLD)
)

session_signals["signal_long"] = (
    (session_signals["vrp_z"] < -Z_THRESHOLD)
)

# ------------------------------------------------------------
# 5) Final cleanup and save
# ------------------------------------------------------------
session_signals = session_signals.drop(columns=["forecast_date"], errors="ignore")

print(session_signals[[
    "session_start", "session_type", "atm_iv",
    "iv_52w_low", "iv_52w_high", "iv_rank",
    "rv_signal", "vrp", "vrp_real", "vrp_ratio", "vrp_z",
    "signal_short", "signal_long"
]].tail(20))

session_signals.to_csv("data/session_ivs2.csv", index=False)