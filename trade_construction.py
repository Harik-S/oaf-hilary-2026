import numpy as np
import pandas as pd

NOTIONAL_CONTRACTS = {
    "atm_call": 1,
    "atm_put": 1,
    "otm_call": 1,
    "otm_put": 1
}

def import_data():
    session_signals = pd.read_csv('data/session_signals.csv')
    options_df = pd.read_csv("data/options_df.csv")
    btc_prices = pd.read_csv('data/btc_prices.csv')
    btc_prices.rename(columns={'Unnamed: 0': 'time'}, inplace=True)
    return session_signals, options_df, btc_prices

def construct_trades(session_signals, options_df):
    positive_signals = session_signals[session_signals['signal']]
    trades_df = options_df[options_df['session_start'].isin(positive_signals['session_start'])].copy()

    trades_df['position'] = np.select(
        [
            trades_df['leg'].str.startswith('atm'),
            trades_df['leg'].str.startswith('otm')
        ],
        ['short', 'long'],
        default=None
    )

    trades_df['contracts'] = trades_df['leg'].map(NOTIONAL_CONTRACTS)

    if trades_df['contracts'].isna().any():
        missing = trades_df.loc[trades_df['contracts'].isna(), 'leg'].unique()
        raise ValueError(f"Unmapped legs found in NOTIONAL_CONTRACTS: {missing}")

    if trades_df['position'].isna().any():
        bad = trades_df.loc[trades_df['position'].isna(), 'leg'].unique()
        raise ValueError(f"Unclassified legs: {bad}")

    cols = [
        'session_start', 'session_type', 'leg', 'strike', 'option_type',
        'position', 'entry_price', 'expiry', 'contracts'
    ]

    trades_df = trades_df[cols]
    trades_df.to_csv('data/trade_construction.csv', index=False)
    print(f"Saved trade_construction.csv ({len(trades_df)} rows)")
    return trades_df

def calculate_exit_price_and_pnl(trades_df, btc_prices):
    btc_prices_keyed = btc_prices[['time', 'close']].rename(
        columns={'time': 'expiry', 'close': 'spot_at_expiry'}
    )
    
    trades_df = trades_df.merge(
        btc_prices_keyed,
        on='expiry',
        how='left'
    )

    if trades_df['spot_at_expiry'].isna().any():
        missing = trades_df.loc[trades_df['spot_at_expiry'].isna(), 'expiry'].unique()
        raise ValueError(f"Missing BTC prices for expiries: {missing}")
    
    spot = trades_df['spot_at_expiry']
    strike = trades_df['strike']

    call_payoff = np.maximum(spot - strike, 0)
    put_payoff  = np.maximum(strike - spot, 0)

    trades_df['exit_price'] = np.select(
        [
            trades_df['option_type'] == 'C',
            trades_df['option_type'] == 'P'
        ],
        [
            call_payoff,
            put_payoff
        ],
        default=np.nan
    )

    trades_df['position_sign'] = trades_df['position'].map({'short': -1, 'long': 1})

    trades_df['pnl'] = (
        (trades_df['entry_price'] * trades_df['spot_at_expiry'] - trades_df['exit_price'])
        * trades_df['contracts']
        * trades_df['position_sign']
    )

    trades_df.drop(columns=['position_sign'], inplace=True)

    return trades_df

def summarise_pnl(trades_df):
    trades_df['premium_component'] = np.where(
        trades_df['position'] == 'short',
        trades_df['entry_price'],
        0
    )

    trades_df['hedge_component'] = np.where(
        trades_df['position'] == 'long',
        trades_df['entry_price'],
        0
    )

    agg = trades_df.groupby('session_start').agg(
        gross_pnl_usd=('pnl', 'sum'),
        premium_sum=('premium_component', 'sum'),
        hedge_sum=('hedge_component', 'sum'),
        spot_at_expiry=('spot_at_expiry', 'first')  # assumes constant per session
    )

    agg = agg.sort_values('session_start')

    agg['premiums_received'] = agg['premium_sum'] * agg['spot_at_expiry']
    agg['hedge_cost'] = agg['hedge_sum'] * agg['spot_at_expiry']
    agg['net_entry_premium'] = agg['premiums_received'] - agg['hedge_cost']

    agg['cumulative_pnl_usd'] = agg['gross_pnl_usd'].cumsum()

    pnl_df = agg.reset_index()[[
        'session_start',
        'gross_pnl_usd',
        'premiums_received',
        'hedge_cost',
        'net_entry_premium',
        'cumulative_pnl_usd'
    ]]

    pnl_df.to_csv('data/pnl_df.csv', index=False)
    print(f"Saved pnl_df.csv ({len(pnl_df)} rows)")
    return pnl_df

def main():
    session_signals, options_df, btc_prices = import_data()
    trades_df = construct_trades(session_signals, options_df)
    trades_df = calculate_exit_price_and_pnl(trades_df, btc_prices)
    summarise_pnl(trades_df)

if __name__ == "__main__":
    main()