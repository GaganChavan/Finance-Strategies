# ── Config ─────────────────────────────────────────────────────────────────────
CREDS_PATH = r'/Users/gagankumarchavan/Documents/API Cred/noble-aquifer-437514-k4-a50658fe7247.json'
SHEET_NAME = "My ETF Portfolio tracker"
WORKSHEET_NAME = "US500"

"""
US500 WEEKLY MACD SCANNER — what this script does
=================================================
Signal scanner only: it does NOT place trades. It reads tickers from the US500
tab, computes indicators from Yahoo Finance prices and writes the results to H:O.

FLOW
  1. Read tickers from column B ........................................ L328
  2. Download prices in batches of 50 (full history) .................. L371
  3. Missing ticker in batch → individual download, 3 retries ......... L165
  4. Compute metrics per ticker ....................................... L230
  5. Write each batch to the sheet before the next one starts ......... L400
     (Sheets rate-limit errors → wait 15/30/45/60s and retry .......... L341)

COLUMNS WRITTEN
  H  1 year returns   % change, latest close vs ~365 days ago ......... L254
  I  Daily volatility std dev of daily % returns, last 1 year ......... L261
                      (single-day moves >50% ignored as split noise)
  J  Weekly Returns   last completed week, Fri close vs prior Fri ..... L243
  K  % from ATH       latest close vs highest-ever intraday high ...... L236
                      (0 = at all-time high, -25 = 25% below it)
  L  Weekly MACD      histogram = MACD line − signal line ............. L280
  M  MACD Signal      label below ...................................... L296
  N  Higher Highs     weekly swing-structure label ..................... L184

MACD SETUP ............................................................. L83
  • Weekly bars, Friday close; current unfinished week excluded ....... L116
  • MACD line   = EMA(12) − EMA(24) of weekly close
  • Signal line = EMA(3) of MACD line
  • Histogram   = MACD line − Signal line
  • Uses the last 3 years of data; needs ≥ 27 weekly bars, else
    "Insufficient Data" ................................................ L273

ENTRY / EXIT RULES (column M)
  ENTRY  "Strong Buy"     histogram turned positive THIS week (week 1)
  HOLD   "Green N weeks"  histogram positive N weeks in a row
  EXIT   "Strong Sell"    histogram turned negative THIS week (week 1)
  AVOID  "Red N weeks"    histogram negative N weeks in a row
  • Streak counted backwards from latest week, capped at 52+ .......... L286
  • Labels assigned ................................................... L296

HIGHER HIGHS FILTER (column N) ......................................... L184
  • Weekly bars, last 52 completed weeks; settings ..................... L88
  • A swing counts as higher/lower only if it differs by more than 1%
  • Swing high = weekly high above 2 weeks before & not beaten 2 weeks after
  • Swing low  = weekly low  below 2 weeks before & not broken 2 weeks after
  • Swings are forced to alternate H/L/H/L: if two swing highs have no swing
    low between them, the lowest weekly low between them is used (and vice versa)
  • "Uptrend: HH+HL"       last swing high > previous AND last swing low > previous
  • "Downtrend: LH+LL"     lower swing high AND lower swing low
  • "Expanding: HH+LL"     higher swing high but lower swing low
  • "Contracting: LH+HL"   lower swing high but higher swing low
  • "Higher Highs only" / "Higher Lows only" / "Lower Highs only" / "Lower Lows only"
                           one side moved >1%, the other stayed within ±1%
  • "Sideways"             both sides within ±1% of the previous swing
  • "+ Breakout"           latest weekly close above the last swing high
  • "+ Breakdown"          latest weekly close below the last swing low (structure broken)
  • Suggested combo: MACD "Strong Buy" + "Uptrend: HH+HL" WITHOUT "+ Breakdown"

ACTION (column O) — combines M (timing) + N (trend), first matching rule wins
  • "EXIT / AVOID"  structure has "+ Breakdown" (close below last swing low)
  • "EXIT"          MACD "Strong Sell"
  • "BUY"           Uptrend: HH+HL + MACD "Strong Buy"
  • "BUY (late)"    Uptrend: HH+HL + MACD Green 2–4 weeks
  • "HOLD"          Uptrend: HH+HL + MACD Green 5+ weeks
  • "WAIT"          Uptrend: HH+HL + MACD Red (pullback)
  • "WATCH"         Downtrend + Breakout + MACD green (reversal attempt), or
                    mixed structure with MACD green
  • "EARLY BUY"     mixed structure + Breakout + MACD "Strong Buy" (small size)
  • "AVOID"         Downtrend without breakout, or mixed structure + MACD red
"""

import yfinance as yf
import pandas as pd
from google.oauth2.service_account import Credentials
import gspread
import gspread.utils
from datetime import date, timedelta
from gspread.exceptions import APIError
import time

# ── Config ─────────────────────────────────────────────────────────────────────
CREDS_PATH = r'/Users/gagankumarchavan/Documents/API Cred/noble-aquifer-437514-k4-a50658fe7247.json'
SPREADSHEET_KEY = "1eK9hzfu6LvIhWCg1D1tVN5YOnYtTwJGJ5B451TjNqek"
WORKSHEET_NAME = "US500"

HEADER_ROW = 1
START_ROW = 2
COL_SYMBOL = 2       # B  (Ticker)

COL_1Y_RETURN = 8    # H  1 year returns
COL_VOLATILITY = 9   # I  Daily volatility
COL_WEEKLY_RET = 10  # J  Weekly Returns
COL_ATH = 11         # K  % from ATH
COL_MACD_DIFF = 12   # L  Weekly MACD
COL_SIGNAL = 13      # M  MACD Signal
COL_HH = 14          # N  Higher Highs (swing-structure filter)
COL_ACTION = 15      # O  Action (combines M + N into one decision)
# H:O must stay contiguous — one row is written as a single 8-cell range

MACD_FAST = 12
MACD_SLOW = 24
MACD_SIG = 3
MAX_WEEKS = 52

PIVOT_WINDOW = 2          # a swing high/low must beat 2 weekly bars on each side
HH_LOOKBACK_WEEKS = 52    # look for swing points within the last 52 completed weeks
HH_TOLERANCE = 0.01       # swing must be >1% above/below the previous one to count

BATCH_SIZE = 50               # tickers per yfinance batch call (500 stocks → 10 batches)
SLEEP_BETWEEN_BATCHES = 3.0   # seconds between batch downloads
SLEEP_INDIVIDUAL = 2.0        # seconds before an individual retry download
MAX_RETRIES = 3               # retry attempts per individual ticker
WRITE_MAX_RETRIES = 5         # retries if Google Sheets returns a rate-limit / server error


def get_yf_ticker(symbol):
    """US share classes use a dot on exchanges but a dash on Yahoo (BRK.B → BRK-B)."""
    return symbol.strip().upper().replace('.', '-')


def extract_price_col(df, ticker):
    if isinstance(df.columns, pd.MultiIndex):
        for label in ['Adj Close', 'Close']:
            if (label, ticker) in df.columns:
                return (label, ticker)
        return None
    for label in ['Adj Close', 'Close']:
        if label in df.columns:
            return label
    return None


def last_completed_friday():
    """Most recent Friday whose market session is fully closed."""
    today = date.today()
    days_back = (today.weekday() - 4) % 7
    if days_back == 0:   # today IS Friday — session may still be open
        days_back = 7
    return today - timedelta(days=days_back)


def download_individual(ticker, symbol):
    """Download a single ticker with exponential backoff retry."""
    for attempt in range(MAX_RETRIES):
        try:
            df = yf.download(ticker, period="max", interval="1d", progress=False, auto_adjust=False)
            if not df.empty:
                return df
            print(f"  ⚠️  {symbol} ({ticker}): empty response")
            return pd.DataFrame()
        except Exception as e:
            wait = 2 ** (attempt + 1)   # 2s, 4s, 8s
            if attempt < MAX_RETRIES - 1:
                print(f"  ↩️  {symbol} retry {attempt + 1}/{MAX_RETRIES - 1} in {wait}s — {e}")
                time.sleep(wait)
            else:
                print(f"  ❌ {symbol}: all retries exhausted — {e}")
    return pd.DataFrame()


def _col(df, label, ticker, multi):
    """Return a clean Series for one OHLC field, or None."""
    key = (label, ticker) if multi else label
    if key not in df.columns:
        return None
    col = df[key]
    if isinstance(col, pd.DataFrame):   # single-ticker download can still be MultiIndex
        col = col.iloc[:, 0]
    col = col.dropna()
    return col if len(col) > 0 else None


def _split(df, ticker, multi):
    """(adj_close, high, low, close) from a downloaded frame; adj_close None if unusable."""
    adj = _col(df, 'Adj Close', ticker, multi)
    if adj is None:
        adj = _col(df, 'Close', ticker, multi)
    return (adj, _col(df, 'High', ticker, multi), _col(df, 'Low', ticker, multi),
            _col(df, 'Close', ticker, multi))


def get_price_data(batch_df, ticker, batch_tickers, symbol):
    """Use the batch result when valid; fall back to an individual download."""
    if not batch_df.empty:
        multi = isinstance(batch_df.columns, pd.MultiIndex) and len(batch_tickers) > 1
        if not multi and isinstance(batch_df.columns, pd.MultiIndex):
            multi = (('Close', ticker) in batch_df.columns)
        adj, high, low, close = _split(batch_df, ticker, multi)
        if adj is not None:
            return adj, high, low, close

    print(f"  ↩️  {symbol} ({ticker}): not in batch, downloading individually...")
    time.sleep(SLEEP_INDIVIDUAL)
    ind_df = download_individual(ticker, symbol)
    if ind_df.empty:
        return None, None, None, None
    multi = isinstance(ind_df.columns, pd.MultiIndex) and ('Close', ticker) in ind_df.columns
    return _split(ind_df, ticker, multi)


def higher_highs_status(highs, lows, closes, completed_fri):
    """
    Swing-structure filter on completed weekly bars (classic Dow-theory definition).
      Swing high = weekly high above the 2 weeks before it and not exceeded by the 2 weeks after.
      Swing low  = weekly low below the 2 weeks before it and not undercut by the 2 weeks after.
      HH = latest swing high >1% above the previous one; HL = latest swing low >1% above the previous one.
      "+ Breakout" = latest weekly close is above the latest swing high (a new HH is forming,
      not yet confirmed because a swing needs 2 more weeks to be confirmed).
    """
    if highs is None or lows is None or closes is None:
        return 'Insufficient Data'
    wh = highs.resample('W-FRI').max().dropna()
    wl = lows.resample('W-FRI').min().dropna()
    wc = closes.resample('W-FRI').last().dropna()
    wh = wh[wh.index.date <= completed_fri].iloc[-HH_LOOKBACK_WEEKS:].values
    wl = wl[wl.index.date <= completed_fri].iloc[-HH_LOOKBACK_WEEKS:].values
    wc = wc[wc.index.date <= completed_fri]
    k = PIVOT_WINDOW

    # Raw pivot candidates: (bar index, 'H'/'L', price)
    is_ph = {i for i in range(k, len(wh) - k)
             if wh[i] > wh[i - k:i].max() and wh[i] >= wh[i + 1:i + k + 1].max()}
    is_pl = {i for i in range(k, len(wl) - k)
             if wl[i] < wl[i - k:i].min() and wl[i] <= wl[i + 1:i + k + 1].min()}

    # Build a strictly alternating H/L sequence (as a chart reader would): between two
    # swing highs there is always a swing low (the lowest low between them), and vice versa.
    seq = []
    for i in sorted(is_ph | is_pl):
        if i in is_ph and i in is_pl:          # outside week — use only the side that alternates
            kinds = ['L'] if seq and seq[-1][1] == 'H' else ['H']
        else:
            kinds = ['H'] if i in is_ph else ['L']
        for kind in kinds:
            price = wh[i] if kind == 'H' else wl[i]
            if seq and seq[-1][0] == i:
                continue
            if seq and seq[-1][1] == kind:
                j0 = seq[-1][0]
                if i - j0 > 1:                  # insert the missing opposite swing in between
                    if kind == 'H':
                        j = j0 + 1 + int(wl[j0 + 1:i].argmin())
                        seq.append((j, 'L', wl[j]))
                    else:
                        j = j0 + 1 + int(wh[j0 + 1:i].argmax())
                        seq.append((j, 'H', wh[j]))
                    seq.append((i, kind, price))
                elif (kind == 'H' and price > seq[-1][2]) or (kind == 'L' and price < seq[-1][2]):
                    seq[-1] = (i, kind, price)  # adjacent bars: keep the more extreme one
            else:
                seq.append((i, kind, price))

    swing_highs = [p for _, t, p in seq if t == 'H']
    swing_lows = [p for _, t, p in seq if t == 'L']
    if len(swing_highs) < 2 or len(swing_lows) < 2 or wc.empty:
        return 'Insufficient Data'

    tol = HH_TOLERANCE
    hh = swing_highs[-1] > swing_highs[-2] * (1 + tol)   # higher high
    lh = swing_highs[-1] < swing_highs[-2] * (1 - tol)   # lower high
    hl = swing_lows[-1] > swing_lows[-2] * (1 + tol)     # higher low
    ll = swing_lows[-1] < swing_lows[-2] * (1 - tol)     # lower low
    if hh and hl:
        label = 'Uptrend: HH+HL'
    elif lh and ll:
        label = 'Downtrend: LH+LL'
    elif hh and ll:
        label = 'Expanding: HH+LL'
    elif lh and hl:
        label = 'Contracting: LH+HL'
    elif hh:
        label = 'Higher Highs only'
    elif hl:
        label = 'Higher Lows only'
    elif lh:
        label = 'Lower Highs only'
    elif ll:
        label = 'Lower Lows only'
    else:
        label = 'Sideways'

    last_close = float(wc.iloc[-1])
    if last_close > swing_highs[-1]:
        label += ' + Breakout'        # closed above the last confirmed swing high
    elif last_close < swing_lows[-1]:
        label += ' + Breakdown'       # closed below the last confirmed swing low — structure broken
    return label


def action_status(rsig, rhh):
    """
    Combine MACD signal (column M = timing) with swing structure (column N = trend)
    into one plain action. Rules are checked top to bottom; the first match wins.
    """
    if not rsig or not rhh or 'Insufficient' in rsig or 'Insufficient' in rhh:
        return 'NO DATA'

    uptrend = rhh.startswith('Uptrend')
    downtrend = rhh.startswith('Downtrend')
    breakout = 'Breakout' in rhh
    breakdown = 'Breakdown' in rhh
    strong_buy = rsig == 'Strong Buy'
    strong_sell = rsig == 'Strong Sell'
    green = rsig.startswith('Green')
    green_weeks = 0
    if green:
        try:
            green_weeks = int(rsig.split()[1].rstrip('+'))
        except (IndexError, ValueError):
            green_weeks = 99

    # 1. Exits / hard avoids
    if breakdown:
        return 'EXIT / AVOID – broke last swing low'
    if strong_sell:
        return 'EXIT – momentum turned down'

    # 2. Confirmed uptrend
    if uptrend:
        if strong_buy:
            return 'BUY – uptrend + fresh MACD cross'
        if green and green_weeks <= 4:
            return 'BUY (late entry) – uptrend, momentum up'
        if green:
            return 'HOLD – uptrend, extended move'
        return 'WAIT – pullback in uptrend'          # red weeks

    # 3. Confirmed downtrend
    if downtrend:
        if breakout and (strong_buy or green):
            return 'WATCH – reversal attempt, not confirmed'
        return 'AVOID – downtrend'

    # 4. Mixed / transitional structures
    if breakout and strong_buy:
        return 'EARLY BUY – small size, trend not confirmed'
    if strong_buy or green:
        return 'WATCH – momentum up, no trend yet'
    return 'AVOID – no trend, momentum down'


def compute_metrics(prices_all, highs, lows, closes, completed_fri):
    """Return (1y_ret%, daily_vol%, weekly_ret%, pct_from_ath%, macd_diff, signal, hh_status)."""
    r1y = rvol = rwk = rath = rmacd = rsig = ''
    rhh = higher_highs_status(highs, lows, closes, completed_fri)
    last_date = prices_all.index.max()

    # % from All-Time High — highest split-adjusted intraday High over full history
    # vs latest Close (same split-adjusted basis). 0 = at ATH, -25 = 25% below ATH.
    if highs is not None and closes is not None and len(highs) and len(closes):
        ath = float(highs.max())
        if ath > 0:
            rath = round((float(closes.iloc[-1]) / ath - 1) * 100, 2)

    # Weekly Return — last completed week (Friday close vs previous Friday close),
    # same week boundary the MACD uses
    weekly_all = prices_all.resample('W-FRI').last().dropna()
    weekly_all = weekly_all[weekly_all.index.date <= completed_fri]
    if len(weekly_all) >= 2:
        rwk = round((float(weekly_all.iloc[-1]) / float(weekly_all.iloc[-2]) - 1) * 100, 2)

    # Everything below uses the last 3 years only, so 1Y / volatility / MACD
    # values are identical to the previous version of this script
    prices = prices_all[prices_all.index >= last_date - pd.DateOffset(years=3)]

    # 1-Year Return
    year_ago = last_date - pd.Timedelta(days=365)
    idx = prices.index.get_indexer([year_ago], method='nearest')[0]
    if idx >= 0:
        ret = float(prices.iloc[-1]) / float(prices.iloc[idx]) - 1
        r1y = round(ret * 100, 2)

    # Daily Volatility (1 year) — ignore >50% single-day moves (split artefacts)
    prices_1y = prices[prices.index >= year_ago]
    if len(prices_1y) > 1:
        daily_rets = prices_1y.pct_change().dropna()
        daily_rets = daily_rets[daily_rets.abs() <= 0.50]
        if len(daily_rets) > 1:
            rvol = round(float(daily_rets.std()) * 100, 4)

    # Weekly MACD (12, 24, 3) — completed weeks only
    weekly = prices.resample('W-FRI').last().dropna()
    weekly = weekly[weekly.index.date <= completed_fri]

    if len(weekly) < MACD_SLOW + MACD_SIG:
        rsig = 'Insufficient Data'
    else:
        ema_fast = weekly.ewm(span=MACD_FAST, adjust=False).mean()
        ema_slow = weekly.ewm(span=MACD_SLOW, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=MACD_SIG, adjust=False).mean()
        diff = macd_line - signal_line

        current_diff = float(diff.iloc[-1])
        rmacd = round(current_diff, 4)
        is_bullish = current_diff > 0

        weeks = 0
        for j in range(len(diff) - 1, -1, -1):
            val = float(diff.iloc[j])
            if (is_bullish and val > 0) or (not is_bullish and val <= 0):
                weeks += 1
            else:
                break
            if weeks >= MAX_WEEKS:
                break

        w_str = f"{weeks}+" if weeks >= MAX_WEEKS else str(weeks)
        if is_bullish:
            rsig = "Strong Buy" if weeks == 1 else f"Green {w_str} weeks"
        else:
            rsig = "Strong Sell" if weeks == 1 else f"Red {w_str} weeks"

    return r1y, rvol, rwk, rath, rmacd, rsig, rhh


# ── Google Sheets Setup ────────────────────────────────────────────────────────
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file(CREDS_PATH, scopes=scope)
client = gspread.authorize(creds)
sheet = client.open_by_key(SPREADSHEET_KEY).worksheet(WORKSHEET_NAME)

header_row_vals = sheet.row_values(HEADER_ROW)

def ensure_header(col_idx, label):
    if len(header_row_vals) < col_idx or not header_row_vals[col_idx - 1].strip():
        cell = gspread.utils.rowcol_to_a1(HEADER_ROW, col_idx)
        sheet.update(range_name=cell, values=[[label]])
        print(f"  Header written: {label} → {cell}")

ensure_header(COL_1Y_RETURN, '1 year returns')
ensure_header(COL_VOLATILITY, 'Daily volatility')
ensure_header(COL_WEEKLY_RET, 'Weekly Returns')
ensure_header(COL_ATH, '% from ATH')
ensure_header(COL_MACD_DIFF, 'Weekly MACD')
ensure_header(COL_SIGNAL, 'MACD Signal')
ensure_header(COL_HH, 'Higher Highs')
ensure_header(COL_ACTION, 'Action')

# ── Read stock rows ────────────────────────────────────────────────────────────
all_rows = sheet.get_all_values()
stocks = []
for i, row in enumerate(all_rows[START_ROW - 1:], start=START_ROW):
    symbol = row[COL_SYMBOL - 1].strip() if len(row) >= COL_SYMBOL else ''
    if not symbol:
        continue
    stocks.append({'row': i, 'symbol': symbol, 'ticker': get_yf_ticker(symbol)})

completed_fri = last_completed_friday()
total_batches = (len(stocks) - 1) // BATCH_SIZE + 1
print(f"Found {len(stocks)} stocks across {total_batches} batches  |  Completed weeks up to: {completed_fri}")
print("─" * 65)

def write_batch(updates, b_num):
    """Write one batch's rows in a single API call, retrying on rate limits."""
    for attempt in range(WRITE_MAX_RETRIES):
        try:
            sheet.batch_update(updates, value_input_option='USER_ENTERED')
            print(f"   📤 Batch {b_num}: wrote {len(updates)} rows to sheet")
            return True
        except APIError as e:
            code = getattr(e.response, 'status_code', None)
            if code in (429, 500, 502, 503) and attempt < WRITE_MAX_RETRIES - 1:
                wait = 15 * (attempt + 1)   # 15s, 30s, 45s, 60s
                print(f"   ⏳ Sheets API {code} on batch {b_num} — retrying in {wait}s")
                time.sleep(wait)
            else:
                print(f"   ❌ Batch {b_num} write failed: {e}")
                return False
    return False


# ── Main loop: download batch → compute → write batch → next ───────────────────
failed_writes = []

for b_start in range(0, len(stocks), BATCH_SIZE):
    batch = stocks[b_start:b_start + BATCH_SIZE]
    b_num = b_start // BATCH_SIZE + 1
    batch_tickers = [item['ticker'] for item in batch]
    batch_updates = []

    print(f"\n⬇️  Batch {b_num}/{total_batches} — downloading {len(batch_tickers)} tickers...")
    try:
        batch_df = yf.download(
            batch_tickers if len(batch_tickers) > 1 else batch_tickers[0],
            period="max", interval="1d", progress=False, auto_adjust=False, threads=True,
        )
        print(f"   Batch {b_num} downloaded — {len(batch_df)} daily rows")
    except Exception as e:
        print(f"   ⚠️  Batch {b_num} failed ({e}) — will fall back to individual downloads")
        batch_df = pd.DataFrame()

    for item in batch:
        symbol, ticker, row_num = item['symbol'], item['ticker'], item['row']
        r1y = rvol = rwk = rath = rmacd = rsig = rhh = ''
        ract = 'NO DATA'
        try:
            prices, highs, lows, closes = get_price_data(batch_df, ticker, batch_tickers, symbol)
            if prices is None or prices.empty:
                print(f"   ⚠️  {symbol:8}: no usable price data")
            else:
                r1y, rvol, rwk, rath, rmacd, rsig, rhh = compute_metrics(prices, highs, lows, closes, completed_fri)
                ract = action_status(rsig, rhh)
                print(f"   ✅ {symbol:8} | 1Y={str(r1y)+'%':>9}  Vol={str(rvol)+'%':>8}  "
                      f"Wk={str(rwk)+'%':>7}  ATH={str(rath)+'%':>8}  MACD={str(rmacd):>10}  → {rsig} | {rhh} | {ract}")
        except Exception as e:
            print(f"   ❌ {symbol:8}: {e}")

        start_cell = gspread.utils.rowcol_to_a1(row_num, COL_1Y_RETURN)
        end_cell = gspread.utils.rowcol_to_a1(row_num, COL_ACTION)
        batch_updates.append({'range': f"{start_cell}:{end_cell}",
                              'values': [[r1y, rvol, rwk, rath, rmacd, rsig, rhh, ract]]})

    # Write this batch now, before downloading the next one
    if not write_batch(batch_updates, b_num):
        failed_writes.extend(batch_updates)

    if b_start + BATCH_SIZE < len(stocks):
        print(f"   ⏳ Waiting {SLEEP_BETWEEN_BATCHES}s before next batch...")
        time.sleep(SLEEP_BETWEEN_BATCHES)

# ── Final retry for any batch whose write failed ───────────────────────────────
if failed_writes:
    print(f"\n🔁 Retrying {len(failed_writes)} rows that failed to write...")
    time.sleep(60)
    write_batch(failed_writes, 'retry')

print(f"\n✅ Done! {len(stocks)} stocks processed in '{WORKSHEET_NAME}'.")