"""
Subtask 1B: BTC Options Data Collection via Deribit Public API
No API key required.

Session windows (24h, using Deribit daily expiries):
  Weekend: Saturday 08:00 UTC → Sunday 08:00 UTC
  Weekday: Monday  08:00 UTC → Tuesday 08:00 UTC

Key design decisions:
  - Daily expiries on Deribit are listed and trading from the PREVIOUS day
    at 08:00 UTC, so at session open the target expiry already exists and
    is liquid.
  - We fetch trades only in the FIRST 2 HOURS of the session to find entry
    prices. This ensures we only price legs that were actually trading at
    session open.
  - Exit price = intrinsic value at expiry (computed in Subtask 7 from
    btc_prices, not from trade data).
  - Legs where no trade exists within 2h of session open are flagged with
    data_quality_ok = False and kept in the CSV for auditability.

Output files:
  data/options_df.csv  — one row per leg per session
  data/sessions.csv    — session window metadata

Prerequisites:
  - Run your existing dataCollection.py first to produce data/btc_prices.csv
"""

import requests
import pandas as pd
import numpy as np
import time
import os
from datetime import timedelta

# ── Config ────────────────────────────────────────────────────────────────────

BACKTEST_START      = "2025-01-01"
BACKTEST_END        = "2026-01-01"
DATA_DIR            = "data"
DERIBIT_HISTORY_URL = "https://history.deribit.com/api/v2/public"

# Only look at trades within this many hours of session open for entry pricing.
# Trades outside this window mean the instrument wasn't liquid at session open.
ENTRY_WINDOW_HOURS  = 2.0

os.makedirs(DATA_DIR, exist_ok=True)

# ── Helper: safe GET with retry ───────────────────────────────────────────────

def safe_get(url: str, params: dict, retries: int = 3, delay: float = 2.0) -> dict:
    """GET with retry logic. Returns empty dict on failure."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=20)
            if resp.status_code == 429:
                wait = 10 * (attempt + 1)
                print(f"    Rate limited — sleeping {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            print(f"    Request error (attempt {attempt+1}/{retries}): {e}")
            time.sleep(delay)
    return {}

# ── Step 1: Generate session windows ─────────────────────────────────────────

def generate_session_windows(start: str, end: str) -> pd.DataFrame:
    """
    Generates all 24h session windows between start and end.

    Weekend: Saturday 08:00 UTC → Sunday 08:00 UTC
    Weekday: Monday  08:00 UTC → Tuesday 08:00 UTC

    Returns DataFrame with columns:
      session_start (UTC), session_close (UTC), session_type (str)
    """
    sessions = []
    current  = pd.Timestamp(start, tz="UTC")
    end_ts   = pd.Timestamp(end,   tz="UTC")

    while current < end_ts:
        dow = current.dayofweek  # 0=Mon, 5=Sat

        """
        if dow == 5 and current.hour == 8:   # Saturday 08:00 → Sunday 08:00
            sessions.append({
                "session_start": current,
                "session_close": current + timedelta(hours=24),
                "session_type":  "weekend"
            })
        """
        if dow == 0 and current.hour == 8:  # Monday 08:00 → Tuesday 08:00
            sessions.append({
                "session_start": current,
                "session_close": current + timedelta(hours=24),
                "session_type":  "weekday",
                "session_dow":  "monday"
            })
        elif dow == 1 and current.hour == 8:  # Tuesday 08:00 → Wednesday 08:00
            sessions.append({
                "session_start": current,
                "session_close": current + timedelta(hours=24),
                "session_type":  "weekday",
                "session_dow":  "tuesday"
            })
        elif dow == 2 and current.hour == 8:  # Wednesday 08:00 → Thursday 08:00
            sessions.append({
                "session_start": current,
                "session_close": current + timedelta(hours=24),
                "session_type":  "weekday",
                "session_dow":  "wednesday"
            })
        elif dow == 3 and current.hour == 8:  # Thursday 08:00 → Friday 08:00
            sessions.append({
                "session_start": current,
                "session_close": current + timedelta(hours=24),
                "session_type":  "weekday",
                "session_dow":  "thursday"
            })
        elif dow == 4 and current.hour == 8:  # Friday 08:00 → Saturday 08:00
            sessions.append({
                "session_start": current,
                "session_close": current + timedelta(hours=24),
                "session_type":  "weekday",
                "session_dow":  "friday"
            })
        elif dow == 5 and current.hour == 8:  # Saturday 08:00 → Sunday 08:00
            sessions.append({
                "session_start": current,
                "session_close": current + timedelta(hours=24),
                "session_type":  "weekday",
                "session_dow":  "saturday"
            })
        elif dow == 6 and current.hour == 8:  # Sunday 08:00 → Monday 08:00
            sessions.append({
                "session_start": current,
                "session_close": current + timedelta(hours=24),
                "session_type":  "weekend",
                "session_dow":  "sunday"
            })

        current += timedelta(hours=1)

    df        = pd.DataFrame(sessions)
    n_weekend = len(df[df.session_type == "weekend"])
    n_weekday = len(df[df.session_type == "weekday"])
    print(f"Generated {len(df)} session windows "
          f"({n_weekend} weekend, {n_weekday} weekday)")
    return df

# ── Step 2: Fetch BTC options trades for a time window ───────────────────────

def fetch_trades(start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    """
    Fetches all BTC options trades between start and end from Deribit.
    Paginates automatically if > 1000 trades.

    Returns DataFrame with columns:
      timestamp, instrument_name, price, iv, amount,
      direction, index_price, underlying_price
    """
    start_ms      = int(start.timestamp() * 1000)
    end_ms        = int(end.timestamp() * 1000)
    all_trades    = []
    current_start = start_ms

    while True:
        params = {
            "currency":        "BTC",
            "kind":            "option",
            "start_timestamp": current_start,
            "end_timestamp":   end_ms,
            "count":           1000,
            "include_old":     "true"
        }
        data   = safe_get(
            f"{DERIBIT_HISTORY_URL}/get_last_trades_by_currency_and_time",
            params
        )
        result = data.get("result", {})
        trades = result.get("trades", [])

        if not trades:
            break

        all_trades.extend(trades)

        if len(trades) < 1000:
            break

        last_ts       = trades[-1]["timestamp"]
        current_start = last_ts + 1

        if current_start >= end_ms:
            break

        time.sleep(0.2)

    if not all_trades:
        return pd.DataFrame()

    df = pd.DataFrame(all_trades)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)

    keep = [c for c in [
        "timestamp", "instrument_name", "price", "iv",
        "amount", "direction", "index_price", "underlying_price"
    ] if c in df.columns]

    return df[keep].sort_values("timestamp").reset_index(drop=True)

# ── Step 3: Parse instrument name ─────────────────────────────────────────────

def parse_instrument(name: str) -> dict:
    """
    Parses a Deribit instrument name e.g. 'BTC-28MAR25-80000-C'.
    Returns dict: strike (float), expiry (Timestamp), option_type ('C'/'P')
    Returns NaNs on failure.
    """
    try:
        parts       = name.split("-")
        expiry_str  = parts[1]
        strike      = float(parts[2])
        option_type = parts[3]
        expiry      = pd.to_datetime(expiry_str, format="%d%b%y", utc=True)
        return {"strike": strike, "expiry": expiry, "option_type": option_type}
    except Exception:
        return {"strike": np.nan, "expiry": pd.NaT, "option_type": None}

# ── Step 4: Identify the 4 trade legs ────────────────────────────────────────

def identify_legs(entry_trades_df: pd.DataFrame,
                  spot:            float,
                  session_close:   pd.Timestamp) -> pd.DataFrame:
    """
    From trades in the ENTRY WINDOW ONLY (first 2h of session), identifies
    the 4 strategy legs:
      atm_call — call with strike closest to spot
      atm_put  — put with strike closest to spot
      otm_call — call with strike closest to spot * 1.15
      otm_put  — put with strike closest to spot * 0.85

    Only uses instruments expiring at or after session_close.
    Picks the nearest available expiry (should be session_close itself
    for daily expiries).

    Using entry_trades_df (not full window) guarantees that any leg
    identified here was actually trading at session open.

    Returns DataFrame with columns:
      leg, instrument_name, strike, option_type, expiry
    """
    if entry_trades_df.empty:
        return pd.DataFrame()

    unique_names = entry_trades_df["instrument_name"].unique()
    parsed = []
    for name in unique_names:
        meta = parse_instrument(name)
        meta["instrument_name"] = name
        parsed.append(meta)

    catalog = pd.DataFrame(parsed).dropna(subset=["strike", "expiry"])

    # Only instruments expiring at or after session close
    catalog = catalog[catalog["expiry"].dt.date >= session_close.date()]


    if catalog.empty:
        return pd.DataFrame()

    # Use nearest expiry — for daily expiries this should == session_close
    # Pick expiry closest to session_close that has actual trades.
    # Proximity is primary key (Mon->Tue picks Tue over Fri),
    # trade count is tiebreaker.
    expiry_counts = {}
    for name in entry_trades_df["instrument_name"].unique():
        parsed = parse_instrument(name)
        exp    = parsed["expiry"]
        if pd.isna(exp):
            continue
        if exp.date() < session_close.date():
            continue
        count = int((entry_trades_df["instrument_name"] == name).sum())
        expiry_counts[exp] = expiry_counts.get(exp, 0) + count

    if not expiry_counts:
        return pd.DataFrame()

    best_expiry = min(
        expiry_counts.keys(),
        key=lambda e: (
            abs((e.date() - session_close.date()).days),
            -expiry_counts[e]
        )
    )
    catalog = catalog[catalog["expiry"] == best_expiry]
    targets = {
        "atm_call": ("C", spot),
        "atm_put":  ("P", spot),
        "otm_call": ("C", spot * 1.15),
        "otm_put":  ("P", spot * 0.85),
    }

    legs = []
    for leg_name, (opt_type, target_strike) in targets.items():
        subset = catalog[catalog["option_type"] == opt_type]
        if subset.empty:
            continue
        idx     = (subset["strike"] - target_strike).abs().idxmin()
        matched = subset.loc[idx].copy()
        matched["leg"] = leg_name
        legs.append(matched)

    if not legs:
        return pd.DataFrame()

    return pd.DataFrame(legs)[
        ["leg", "instrument_name", "strike", "option_type", "expiry"]
    ].reset_index(drop=True)

# ── Step 5: Extract entry price per leg ──────────────────────────────────────


def extract_entry_prices(entry_trades_df: pd.DataFrame,
                         legs_df:         pd.DataFrame,
                         session_start:   pd.Timestamp) -> pd.DataFrame:
    """
    For each leg, finds the trade closest to session_start within the
    entry window trades.

    New columns added to legs_df:
      entry_price      — option price in BTC at entry
      entry_iv         — implied vol at entry (annualised decimal e.g. 0.85)
      entry_underlying — BTC spot price recorded at time of entry trade
      entry_gap_hours  — hours between session_start and nearest entry trade
      data_quality_ok  — True if entry_gap_hours <= ENTRY_WINDOW_HOURS (2h)

    NOTE: Exit price = intrinsic value at expiry, computed in Subtask 7.
      Call intrinsic: max(spot_at_expiry - strike, 0)
      Put intrinsic:  max(strike - spot_at_expiry, 0)
    """

    if entry_trades_df.empty or legs_df.empty:
        return legs_df

    results = []
    for _, leg in legs_df.iterrows():
        inst   = leg["instrument_name"]
        subset = entry_trades_df[
            entry_trades_df["instrument_name"] == inst
        ].copy()
        print(f"    DEBUG {inst}: {len(subset)} trades found") 

        if subset.empty:
            # Instrument was in catalog but no trade found in entry window
            print(f"    ⚠ No entry window trades for {inst} — flagged")
            results.append({
                "entry_price":      np.nan,
                "entry_iv":         np.nan,
                "entry_underlying": np.nan,
                "entry_gap_hours":  np.nan,
                "data_quality_ok":  False,
            })
            continue

        # Trade nearest to session_start
        subset = subset.copy()
        subset["dist"]  = (subset["timestamp"] - session_start).abs()
        entry_row       = subset.loc[subset["dist"].idxmin()]
        entry_gap_hours = entry_row["dist"].total_seconds() / 3600
        data_quality_ok = entry_gap_hours <= ENTRY_WINDOW_HOURS

        if not data_quality_ok:
            print(f"    ⚠ {inst}: nearest trade {entry_gap_hours:.1f}h "
                  f"from open — flagged")

        results.append({
        "entry_price":      entry_row["price"]            if "price"            in entry_row.index else np.nan,
        "entry_iv":         entry_row["iv"]/100               if "iv"               in entry_row.index else np.nan,
        "entry_underlying": entry_row["underlying_price"] if "underlying_price" in entry_row.index else np.nan,
        "entry_gap_hours":  round(entry_gap_hours, 2),
        "data_quality_ok":  data_quality_ok,})
        print(f"    DEBUG entry_row type: {type(entry_row)}")
        print(f"    DEBUG entry_row:\n{entry_row}")
        print(f"    DEBUG price value: {entry_row['price']}")
        

    price_df = pd.DataFrame(results, index=legs_df.index)
    return pd.concat(
        [legs_df.reset_index(drop=True), price_df.reset_index(drop=True)],
        axis=1
    )

# ── Step 6: Main collection loop ──────────────────────────────────────────────

def collect_options_data(sessions:   pd.DataFrame,
                         btc_prices: pd.DataFrame) -> pd.DataFrame:
    """
    Main loop over all session windows.

    For each session:
      1. Gets BTC spot at session open from btc_prices
      2. Fetches trades from session_start to session_start + 2h (entry window)
      3. Identifies 4 legs from instruments trading in that entry window
      4. Extracts entry prices and IVs

    Returns master options_df with one row per leg per session.
    """
    all_legs = []

    for i, session in sessions.iterrows():
        s_start = session["session_start"]
        s_close = session["session_close"]
        s_type  = session["session_type"]
        s_dow   = session["session_dow"] 

        # Get BTC spot at session open
        try:
            spot = float(btc_prices.loc[s_start, "close"])
        except KeyError:
            idx  = btc_prices.index.get_indexer([s_start], method="nearest")[0]
            spot = float(btc_prices.iloc[idx]["close"])

        print(f"\n[{i+1}/{len(sessions)}] {s_type.upper()} | {s_dow.upper()} | "
              f"{s_start.strftime('%Y-%m-%d %H:%M')} → "
              f"{s_close.strftime('%Y-%m-%d %H:%M')} | "
              f"BTC=${spot:,.0f}")

        # Fetch trades in entry window only (session_start → +2h)
        entry_end     = s_start + timedelta(hours=ENTRY_WINDOW_HOURS)
        entry_trades  = fetch_trades(s_start, entry_end)

        if entry_trades.empty:
            print("  No trades in entry window — skipping")
            continue

        print(f"  Entry window: {len(entry_trades)} trades | "
              f"{entry_trades['instrument_name'].nunique()} instruments")

        # Identify the 4 legs from entry window instruments only
        legs_df = identify_legs(entry_trades, spot, s_close)

        if legs_df.empty:
            print("  No legs found expiring at session close — skipping")
            print("  (daily expiry may not have been listed yet at session open)")
            continue

        print(f"  Legs identified:")
        for _, leg in legs_df.iterrows():
            print(f"    {leg['leg']:12s} {leg['instrument_name']:35s} "
                  f"strike=${leg['strike']:,.0f}  "
                  f"expiry={leg['expiry'].strftime('%Y-%m-%d %H:%M UTC')}")

        # Extract entry prices
        legs_df = extract_entry_prices(entry_trades, legs_df, s_start)

        # Attach session metadata
        legs_df["session_start"] = s_start
        legs_df["session_close"] = s_close
        legs_df["session_type"]  = s_type
        legs_df["session_dow"]   = s_dow 
        legs_df["spot_at_open"]  = spot

        all_legs.append(legs_df)
        time.sleep(0.5)

    if not all_legs:
        print("\nWARNING: No data collected across any session")
        return pd.DataFrame()
    print(f"    DEBUG all_legs length: {len(all_legs)}")
    options_df = pd.concat(all_legs, ignore_index=True)

    # Drop legs with no entry price at all
    before     = len(options_df)
    options_df = options_df.dropna(subset=["entry_price"])
    dropped    = before - len(options_df)
    if dropped:
        print(f"\nDropped {dropped} legs with no entry price")

    # Drop IV data errors (> 500% annualised is clearly wrong)
    if "entry_iv" in options_df.columns:
        options_df = options_df[
            options_df["entry_iv"].isna() | (options_df["entry_iv"] <= 5.0)
        ]

    options_df = options_df.sort_values(
        ["session_start", "leg"]
    ).reset_index(drop=True)

    return options_df

# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Subtask 1B: BTC Options Data Collection")
    print(f"Period  : {BACKTEST_START} to {BACKTEST_END}")
    print(f"Source  : Deribit public API (no API key required)")
    print(f"Weekend : Sat 08:00 UTC → Sun 08:00 UTC (24h)")
    print(f"Weekday : Mon 08:00 UTC → Tue 08:00 UTC (24h)")
    print(f"Entry window: first {ENTRY_WINDOW_HOURS}h of each session\n")

    # 1. Generate session windows
    sessions = generate_session_windows(BACKTEST_START, BACKTEST_END)
    sessions.to_csv(f"{DATA_DIR}/sessions.csv", index=False)
    print(f"Saved sessions.csv ({len(sessions)} rows)\n")

    # 2. Load BTC spot prices (from existing dataCollection.py)
    btc_prices = pd.read_csv(
        f"{DATA_DIR}/btc_prices.csv", index_col=0, parse_dates=True
    )
    btc_prices.index = pd.to_datetime(btc_prices.index, utc=True)
    btc_prices = btc_prices[~btc_prices.index.duplicated(keep="first")]
    print(f"Loaded btc_prices.csv ({len(btc_prices)} rows)")

    # 3. Collect options data
    options_df = collect_options_data(sessions, btc_prices)

    if options_df.empty:
        print("\nNo data collected — check logs above")
    else:
        total       = len(options_df)
        quality_ok  = int(options_df["data_quality_ok"].sum())
        quality_bad = total - quality_ok

        print(f"\n{'='*55}")
        print(f"COLLECTION COMPLETE")
        print(f"{'='*55}")
        print(f"Sessions collected : {options_df['session_start'].nunique()}")
        print(f"Total legs         : {total}")
        print(f"Quality OK (<2h)   : {quality_ok}  ({100*quality_ok/total:.1f}%)")
        print(f"Flagged (>2h gap)  : {quality_bad} ({100*quality_bad/total:.1f}%)")
        print(f"\nIn downstream subtasks, filter with:")
        print(f"  options_df = options_df[options_df['data_quality_ok']]")
        print(f"{'='*55}")

        options_df.to_csv(f"{DATA_DIR}/options_df.csv", index=False)
        print(f"\nSaved options_df.csv ({len(options_df)} rows)")
        print(f"Columns: {list(options_df.columns)}")

    print("\nDone.")