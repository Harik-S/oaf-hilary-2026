import pandas as pd
import numpy as np

session_signals_short = pd.read_csv("data/session_ivs.csv")
session_signals_long = pd.read_csv("data/session_ivs.csv")
options_df = pd.read_csv("data/options_df.csv")

THRESHOLD = 0.05
session_signals_short['signal'] = (session_signals_long['vrp'] > THRESHOLD)

session_signals_long['signal'] = (session_signals_long['vrp'] < -THRESHOLD)

# write results back (at end)
session_signals_long.to_csv("data/session_signals_long.csv", index=False)
session_signals_short.to_csv("data/session_signals_short.csv", index=False)
