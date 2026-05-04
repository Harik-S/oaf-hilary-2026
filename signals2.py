import pandas as pd
import numpy as np

session_signals = pd.read_csv("data/session_ivs.csv")
options_df = pd.read_csv("data/options_df.csv")

VRP_Z_WINDOW = 30

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

Z_THRESHOLD = 1

session_signals["signal_short"] = (
    (session_signals["vrp_z"] > Z_THRESHOLD)
)

session_signals["signal_long"] = (
    (session_signals["vrp_z"] < -Z_THRESHOLD)
)

# write results back (at end)
session_signals.to_csv("data/session_signals2.csv", index=False)
