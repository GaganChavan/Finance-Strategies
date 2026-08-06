import yfinance as yf
import pandas as pd
from google.oauth2.service_account import Credentials
import gspread
import gspread.utils
from datetime import datetime, timedelta
import time

# Google Sheets Integration
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file(r'/Users/gagankumarchavan/Documents/API Cred/noble-aquifer-437514-k4-a50658fe7247.json', scopes=scope)
client = gspread.authorize(creds)
sheet = client.open("Momentum Watch list - Harish").worksheet("Momentum Index")

# Fetch column names from row 2
column_names = sheet.row_values(2)  # Fetch all column names in row 2
#print(f"Fetched column names: {column_names}")

# Find column indices for required fields
required_columns = [
    'CMP','ATH','% from ATH','mMACD', 'mSignal', 'mMACD-Signal', 'mMACD-Signal 1', 'mMACD-Signal 2','mROC',
    'wMACD', 'wSignal', 'wMACD-Signal', 'wMACD-Signal 1', 'wMACD-Signal 2',
    'wROC','52W % Return', '26W % Return', '13W % Return','Entry', 'Pre-Rankings', 'W Rankings', 'M Rankings', ] 

# Find column indices for all required fields
column_indices = {}
for col in required_columns:
    try:
        column_indices[col] = column_names.index(col) + 1  # Convert to 1-based index
        #print(f"'{col}' found in column {column_indices[col]}")
    except ValueError as e:
        raise ValueError(f"Required column '{col}' is missing in the sheet: {e}")

# Fetch stock names from column B, starting from row 3
stock_symbols = sheet.col_values(2)[2:]  # Column B, skipping the header rows
#print(f"Fetched stock names: {stock_symbols}")

# Function to convert data to Heiken Ashi
def convert_to_heiken_ashi(data):
    if data.empty or len(data) < 2:
        raise ValueError("Data is empty or has insufficient rows for Heiken Ashi conversion.")

    required_columns = ['Open', 'High', 'Low', 'Close']
    if not all(col in data.columns for col in required_columns):
        raise ValueError(f"Data must contain the following columns: {required_columns}")

    ha_data = data[['Open', 'High', 'Low', 'Close']].copy()
    ha_data['HA_Close'] = (ha_data['Open'] + ha_data['High'] + ha_data['Low'] + ha_data['Close']) / 4
    ha_data['HA_Open'] = (ha_data['Open'] + ha_data['Close']) / 2
    ha_data.iloc[0, ha_data.columns.get_loc('HA_Open')] = (ha_data.iloc[0]['Open'] + ha_data.iloc[0]['Close']) / 2

    for i in range(1, len(ha_data)):
        ha_data.iloc[i, ha_data.columns.get_loc('HA_Open')] = (
            ha_data.iloc[i - 1]['HA_Open'] + ha_data.iloc[i - 1]['HA_Close']
        ) / 2

    ha_data['HA_High'] = ha_data[['High', 'HA_Open', 'HA_Close']].max(axis=1)
    ha_data['HA_Low'] = ha_data[['Low', 'HA_Open', 'HA_Close']].min(axis=1)
    ha_data.columns = [col[0] if isinstance(col, tuple) else col for col in ha_data.columns]
    
    return ha_data[['HA_Open', 'HA_High', 'HA_Low', 'HA_Close']]

# Function to calculate MACD for any frequency
def calculate_resampled_macd(data, freq, short_window=12, long_window=24, signal_window=3):
    if 'HA_Close' not in data.columns:
        raise ValueError("Heiken Ashi data is missing required 'HA_Close' column.")

    if not isinstance(data.index, pd.DatetimeIndex):
        raise ValueError("Data index must be a DatetimeIndex for resampling.")

    resampled_data = data.resample(freq).agg({
        'HA_Open': 'first',
        'HA_High': 'max',
        'HA_Low': 'min',
        'HA_Close': 'last'
    }).dropna()

    resampled_data['EMA_12'] = resampled_data['HA_Close'].ewm(span=short_window, adjust=False).mean()
    resampled_data['EMA_24'] = resampled_data['HA_Close'].ewm(span=long_window, adjust=False).mean()
    resampled_data['MACD'] = resampled_data['EMA_12'] - resampled_data['EMA_24']
    resampled_data['Signal'] = resampled_data['MACD'].ewm(span=signal_window, adjust=False).mean()
    return resampled_data

# Function to calculate Rate of Change (ROC)
def calculate_roc(data, period):
    if 'HA_Close' not in data.columns:
        raise ValueError("Data must have a 'HA_Close' column to calculate ROC.")

    roc = (data['HA_Close'] - data['HA_Close'].shift(period)) / data['HA_Close'].shift(period) * 100
    return roc

# Function to convert a pandas Series to scalar (float)
def to_scalar(value):
    if isinstance(value, pd.Series):
        return float(value.iloc[0])
    return float(value) if not pd.isna(value) else None

# Function to calculate percentage return
def calculate_percentage_return(data, weeks):
    if len(data) < weeks * 5:
        raise ValueError(f"Not enough data to calculate {weeks} weeks return.")

    try:
        recent_close = data['Close'].iloc[-1]
        past_close = data['Close'].iloc[-weeks * 5]
        return ((recent_close - past_close) / past_close) * 100
    except Exception as e:
        raise ValueError(f"Error calculating percentage return: {e}")

# Function to update a batch of data in Google Sheets
def batch_update(sheet, row_updates):
    """
    Write each field to its own actual cell, in a single API call.

    :param sheet: Google Sheets worksheet instance.
    :param row_updates: List of (row_number, row_data) tuples, where
        row_data is a {column_label: value} dict. Each field is written
        to the cell resolved from column_indices[label] rather than a
        fixed "E:AA" range, so this is unaffected by columns being
        reordered, removed, or having unrelated columns inserted between
        them. Using each stock's real row number (instead of assuming a
        contiguous block) also means a skipped/failed stock never shifts
        the rows after it out of place.
    """
    if not row_updates:
        return
    updates = [
        {
            "range": gspread.utils.rowcol_to_a1(row, column_indices[label]),
            "values": [[value]],
        }
        for row, row_data in row_updates
        for label, value in row_data.items()
    ]
    try:
        sheet.batch_update(updates)
        rows = [row for row, _ in row_updates]
        print(f"Batch update successful for rows: {rows}")
    except Exception as e:
        print(f"Error during batch update: {e}")

# Configuration
batch_size = 5  # Number of stocks per batch
batch_wait_time = 10  # Wait time between batches to handle API quota
column_labels = ['CMP', 'ATH', '% from ATH', 'wMACD', 'wSignal',
                             'wMACD-Signal', 'wMACD-Signal 1', 'wMACD-Signal 2', 'wROC','mMACD',
                             'mSignal', 'mMACD-Signal', 'mMACD-Signal 1', 'mMACD-Signal 2',
                             'mROC', '52W % Return', '26W % Return', '13W % Return',
                             'Entry', 'Pre-Rankings','W Rankings', 'M Rankings']   

# Process stocks in batches
batch_start = 0
while batch_start < len(stock_symbols):
    batch_end = min(batch_start + batch_size, len(stock_symbols))
    batch_stocks = stock_symbols[batch_start:batch_end]

    batch_data = []  # Collect (row_number, row_data) for batch update
    for index, stock in enumerate(batch_stocks, start=batch_start + 3):  # Row starts from 3
        try:
            print(f"Processing {stock}...")
            stock_data = yf.download(stock + ".NS", start="2020-01-01", end=datetime.today().strftime('%Y-%m-%d'))

            if stock_data.empty:
                print(f"No data fetched for {stock}. Skipping.")
                continue

            # Perform calculations
            ath = stock_data['High'].max()
            cmp = float(stock_data['Close'].iloc[-1])
            pct_ath = ((ath - cmp) / ath) * 100
            pct_ath = float(pct_ath) if isinstance(pct_ath, pd.Series) else pct_ath

            return_52w = calculate_percentage_return(stock_data, 52)
            return_26w = calculate_percentage_return(stock_data, 26)
            return_13w = calculate_percentage_return(stock_data, 13)

            return_52w = float(return_52w) if isinstance(return_52w, pd.Series) else return_52w
            return_26w = float(return_26w) if isinstance(return_26w, pd.Series) else return_26w
            return_13w = float(return_13w) if isinstance(return_13w, pd.Series) else return_13w

            ha_data = convert_to_heiken_ashi(stock_data)
            weekly_macd = calculate_resampled_macd(ha_data, 'W')
            monthly_macd = calculate_resampled_macd(ha_data, 'ME')

            latest_weekly_macd = weekly_macd.iloc[-1]['MACD']
            latest_weekly_signal = weekly_macd.iloc[-1]['Signal']
            weekly_macd_signal_diff = latest_weekly_macd - latest_weekly_signal
            
            second_latest_weekly_macd = weekly_macd.iloc[-2]['MACD']
            second_latest_weekly_signal = weekly_macd.iloc[-2]['Signal']
            weekly_macd_signal_diff_1 = float(second_latest_weekly_macd) - float(second_latest_weekly_signal)

            third_latest_weekly_macd = weekly_macd.iloc[-3]['MACD']
            third_latest_weekly_signal = weekly_macd.iloc[-3]['Signal']
            weekly_macd_signal_diff_2 = float(third_latest_weekly_macd) - float(third_latest_weekly_signal)

            latest_monthly_macd = monthly_macd.iloc[-1]['MACD']
            latest_monthly_signal = monthly_macd.iloc[-1]['Signal']
            monthly_macd_signal_diff = latest_monthly_macd - latest_monthly_signal

            second_latest_monthly_macd = monthly_macd.iloc[-2]['MACD']
            second_latest_monthly_signal = monthly_macd.iloc[-2]['Signal']
            monthly_macd_signal_diff_1 = float(second_latest_monthly_macd) - float(second_latest_monthly_signal)

            third_latest_macd = monthly_macd.iloc[-3]['MACD']
            third_latest_signal = monthly_macd.iloc[-3]['Signal']
            monthly_macd_signal_diff_2 = float(third_latest_macd) - float(third_latest_signal)

            # Calculate weekly ROC
            weekly_roc_data = weekly_macd.copy()  # Use the resampled weekly data
            weekly_roc_data['ROC'] = calculate_roc(weekly_roc_data, 6)  # Calculate ROC(6)

            # Get the latest ROC value
            latest_weekly_roc = weekly_roc_data.iloc[-1]['ROC']

            # Calculate monthly ROC
            monthly_roc_data = monthly_macd.copy()  # Use the resampled monthly data
            monthly_roc_data['ROC'] = calculate_roc(monthly_roc_data, 6)  # Calculate ROC(6)

            # Get the latest ROC value
            latest_monthly_roc = monthly_roc_data.iloc[-1]['ROC']
            
            try:
                if float(pct_ath) < 25 and float(monthly_macd_signal_diff) > 0 and float(latest_monthly_roc) > 0 and float(weekly_macd_signal_diff)>0:

                    Entry = 'Consider'
                    curr_weekly_diff = float(latest_weekly_macd) - float(latest_weekly_signal)
                    if (curr_weekly_diff > 0.85 * float(weekly_macd_signal_diff_1)) \
                            and (float(weekly_macd_signal_diff_1) > 0):
                        pre_ranking_status = 'Ready for Ranking'
                    else:
                        pre_ranking_status = 'Not Eligible for ranking'
                else:
                    Entry = "Don't Consider"
                    pre_ranking_status = 'Not Eligible for ranking'
            except Exception as e:
                print(f"Error in conditional check for {stock}: {e}")
                continue
            w_rankings = (float(latest_weekly_macd) * 100) / float(cmp) if cmp else None

            m_rankings = ((float(monthly_macd_signal_diff)) * 100) / float(cmp) if cmp else None    
   
            # Collect row data, keyed by column header name (not position) so
            # writing is immune to columns being reordered/inserted later.
            row_values = [
                to_scalar(cmp), to_scalar(ath), to_scalar(pct_ath), to_scalar(latest_weekly_macd), to_scalar(latest_weekly_signal),
                to_scalar(weekly_macd_signal_diff),
                to_scalar(weekly_macd_signal_diff_1), to_scalar(weekly_macd_signal_diff_2), to_scalar(latest_weekly_roc),
                to_scalar(latest_monthly_macd), to_scalar(latest_monthly_signal), to_scalar(monthly_macd_signal_diff),
                to_scalar(monthly_macd_signal_diff_1), to_scalar(monthly_macd_signal_diff_2), to_scalar(latest_monthly_roc),
                to_scalar(return_52w), to_scalar(return_26w), to_scalar(return_13w),
                Entry, pre_ranking_status, to_scalar(w_rankings), to_scalar(m_rankings)
            ]
            row_data = dict(zip(column_labels, row_values))
            batch_data.append((index, row_data))

        except Exception as e:
            print(f"Error processing {stock}: {e}")

    # Perform batch update
    if batch_data:
        batch_update(sheet, batch_data)

    batch_start += batch_size
    print(f"Batch {batch_start // batch_size} completed. Waiting {batch_wait_time} seconds before next batch.")
    time.sleep(batch_wait_time)
