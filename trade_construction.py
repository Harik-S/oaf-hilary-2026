import numpy as np
import pandas as pd

notional_contracts = {
    "atm_call": 1,
    "atm_put": 1,
    "otm_call": 1,
    "otm_put": 1
}

session_signals = pd.read_csv('data/session_signals.csv')
options_df = pd.read_csv("data/options_df.csv")

positive_signals = session_signals[session_signals['signal']]

trade_df = pd.DataFrame()

for session_start in positive_signals['session_start']:
    trade_df = pd.concat([trade_df, options_df[options_df['session_start'] == session_start]])

trade_df['position'] = ['short' if str(leg).startswith('atm') else 'long' if str(leg).startswith('otm') else None for leg in trade_df['leg']]
trade_df['contracts'] = trade_df['leg'].map(notional_contracts) 

trade_df = trade_df[['session_start', 'session_type', 'leg', 'strike', 'position', 'entry_price', 'expiry', 'contracts']]
trade_df.to_csv('data/trade_construction.csv', index=False)