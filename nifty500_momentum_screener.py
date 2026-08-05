"""
Nifty 500 Momentum Screener — Live Scanner
===========================================
Replicates the Chartink screener: a stock qualifies when, on the SAME day,
ALL of the following just crossed above their threshold:
  - Daily RSI(14)                      crosses above 65
  - Daily Williams %R(140)             crosses above -20
  - Daily CCI(Daily High, 200)         crosses above 100

Run this once per trading day after market close. Matching symbols (with
CMP, indicator values, and a suggested +5% target) are appended to the
"Nifty500 Screener" tab of your Google Sheet, so you build a running log
of every signal fired and can place the trade yourself.
"""

import yfinance as yf
import pandas as pd
import numpy as np
from google.oauth2.service_account import Credentials
import gspread
import gspread.utils
from datetime import date
import time

# ── Config ─────────────────────────────────────────────────────────────────────
CREDS_PATH     = r'/Users/gagankumarchavan/Documents/API Cred/noble-aquifer-437514-k4-a50658fe7247.json'
SHEET_NAME     = "My ETF Portfolio tracker"
WORKSHEET_NAME = "Nifty500 Screener"

# Entry filter thresholds — match the Chartink screener exactly
RSI_PERIOD = 14
RSI_LEVEL  = 65.0
WR_PERIOD  = 140
WR_LEVEL   = -20.0
CCI_PERIOD = 200
CCI_LEVEL  = 100.0

TARGET_PCT = 0.05   # suggested take-profit shown in the sheet (informational only)

# Yahoo Finance download settings
BATCH_SIZE            = 20   # tickers per yfinance batch call
SLEEP_BETWEEN_BATCHES = 3.0  # seconds between batch downloads
SLEEP_INDIVIDUAL       = 2.0  # seconds before an individual retry
MAX_RETRIES            = 3

WRITE_CHUNK = 25  # rows per Google Sheets API call

# Nifty 500 constituents — pulled from NSE's official archive
# (archives.nseindia.com/content/indices/ind_nifty500list.csv) on 2026-08-04.
NIFTY500_STOCKS = {
    "360ONE": "360ONE.NS", "3MINDIA": "3MINDIA.NS", "AADHARHFC": "AADHARHFC.NS",
    "AARTIIND": "AARTIIND.NS", "AAVAS": "AAVAS.NS", "ABB": "ABB.NS",
    "ABBOTINDIA": "ABBOTINDIA.NS", "ABCAPITAL": "ABCAPITAL.NS", "ABDL": "ABDL.NS",
    "ABFRL": "ABFRL.NS", "ABLBL": "ABLBL.NS", "ABREL": "ABREL.NS", "ABSLAMC": "ABSLAMC.NS",
    "ACC": "ACC.NS", "ACE": "ACE.NS", "ACMESOLAR": "ACMESOLAR.NS", "ACUTAAS": "ACUTAAS.NS",
    "ADANIENSOL": "ADANIENSOL.NS", "ADANIENT": "ADANIENT.NS",
    "ADANIGREEN": "ADANIGREEN.NS", "ADANIPORTS": "ADANIPORTS.NS",
    "ADANIPOWER": "ADANIPOWER.NS", "AEGISLOG": "AEGISLOG.NS",
    "AEGISVOPAK": "AEGISVOPAK.NS", "AFCONS": "AFCONS.NS", "AFFLE": "AFFLE.NS",
    "AIAENG": "AIAENG.NS", "AIIL": "AIIL.NS", "AJANTPHARM": "AJANTPHARM.NS",
    "ALKEM": "ALKEM.NS", "AMBER": "AMBER.NS", "AMBUJACEM": "AMBUJACEM.NS",
    "ANANDRATHI": "ANANDRATHI.NS", "ANANTRAJ": "ANANTRAJ.NS", "ANGELONE": "ANGELONE.NS",
    "ANTHEM": "ANTHEM.NS", "ANURAS": "ANURAS.NS", "APARINDS": "APARINDS.NS",
    "APLAPOLLO": "APLAPOLLO.NS", "APOLLOHOSP": "APOLLOHOSP.NS",
    "APOLLOTYRE": "APOLLOTYRE.NS", "APTUS": "APTUS.NS", "ARE&M": "ARE&M.NS",
    "ASAHIINDIA": "ASAHIINDIA.NS", "ASHOKLEY": "ASHOKLEY.NS",
    "ASIANPAINT": "ASIANPAINT.NS", "ASTERDM": "ASTERDM.NS", "ASTRAL": "ASTRAL.NS",
    "ATGL": "ATGL.NS", "ATHERENERG": "ATHERENERG.NS", "ATUL": "ATUL.NS",
    "AUBANK": "AUBANK.NS", "AUROPHARMA": "AUROPHARMA.NS", "AWL": "AWL.NS",
    "AXISBANK": "AXISBANK.NS", "BAJAJ-AUTO": "BAJAJ-AUTO.NS",
    "BAJAJFINSV": "BAJAJFINSV.NS", "BAJAJHFL": "BAJAJHFL.NS",
    "BAJAJHLDNG": "BAJAJHLDNG.NS", "BAJFINANCE": "BAJFINANCE.NS",
    "BALKRISIND": "BALKRISIND.NS", "BALRAMCHIN": "BALRAMCHIN.NS",
    "BANDHANBNK": "BANDHANBNK.NS", "BANKBARODA": "BANKBARODA.NS",
    "BANKINDIA": "BANKINDIA.NS", "BATAINDIA": "BATAINDIA.NS", "BAYERCROP": "BAYERCROP.NS",
    "BBTC": "BBTC.NS", "BDL": "BDL.NS", "BEL": "BEL.NS", "BELRISE": "BELRISE.NS",
    "BEML": "BEML.NS", "BERGEPAINT": "BERGEPAINT.NS", "BHARATFORG": "BHARATFORG.NS",
    "BHARTIARTL": "BHARTIARTL.NS", "BHARTIHEXA": "BHARTIHEXA.NS", "BHEL": "BHEL.NS",
    "BIKAJI": "BIKAJI.NS", "BIOCON": "BIOCON.NS", "BLS": "BLS.NS",
    "BLUEDART": "BLUEDART.NS", "BLUEJET": "BLUEJET.NS", "BLUESTARCO": "BLUESTARCO.NS",
    "BOSCHLTD": "BOSCHLTD.NS", "BPCL": "BPCL.NS", "BRIGADE": "BRIGADE.NS",
    "BRITANNIA": "BRITANNIA.NS", "BSE": "BSE.NS", "BSOFT": "BSOFT.NS", "CAMS": "CAMS.NS",
    "CANBK": "CANBK.NS", "CANFINHOME": "CANFINHOME.NS", "CANHLIFE": "CANHLIFE.NS",
    "CAPLIPOINT": "CAPLIPOINT.NS", "CARBORUNIV": "CARBORUNIV.NS",
    "CARTRADE": "CARTRADE.NS", "CASTROLIND": "CASTROLIND.NS", "CCL": "CCL.NS",
    "CDSL": "CDSL.NS", "CEATLTD": "CEATLTD.NS", "CEMPRO": "CEMPRO.NS",
    "CENTRALBK": "CENTRALBK.NS", "CESC": "CESC.NS", "CGCL": "CGCL.NS",
    "CGPOWER": "CGPOWER.NS", "CHALET": "CHALET.NS", "CHAMBLFERT": "CHAMBLFERT.NS",
    "CHENNPETRO": "CHENNPETRO.NS", "CHOICEIN": "CHOICEIN.NS", "CHOLAFIN": "CHOLAFIN.NS",
    "CHOLAHLDNG": "CHOLAHLDNG.NS", "CIEINDIA": "CIEINDIA.NS", "CIPLA": "CIPLA.NS",
    "CLEAN": "CLEAN.NS", "COALINDIA": "COALINDIA.NS", "COCHINSHIP": "COCHINSHIP.NS",
    "COFORGE": "COFORGE.NS", "COHANCE": "COHANCE.NS", "COLPAL": "COLPAL.NS",
    "CONCOR": "CONCOR.NS", "CONCORDBIO": "CONCORDBIO.NS", "COROMANDEL": "COROMANDEL.NS",
    "CPPLUS": "CPPLUS.NS", "CRAFTSMAN": "CRAFTSMAN.NS", "CREDITACC": "CREDITACC.NS",
    "CRISIL": "CRISIL.NS", "CROMPTON": "CROMPTON.NS", "CUB": "CUB.NS",
    "CUMMINSIND": "CUMMINSIND.NS", "CYIENT": "CYIENT.NS", "DABUR": "DABUR.NS",
    "DALBHARAT": "DALBHARAT.NS", "DATAPATTNS": "DATAPATTNS.NS",
    "DCMSHRIRAM": "DCMSHRIRAM.NS", "DEEPAKFERT": "DEEPAKFERT.NS",
    "DEEPAKNTR": "DEEPAKNTR.NS", "DELHIVERY": "DELHIVERY.NS", "DEVYANI": "DEVYANI.NS",
    "DIVISLAB": "DIVISLAB.NS", "DIXON": "DIXON.NS", "DLF": "DLF.NS", "DMART": "DMART.NS",
    "DOMS": "DOMS.NS", "DRREDDY": "DRREDDY.NS", "ECLERX": "ECLERX.NS",
    "EICHERMOT": "EICHERMOT.NS", "EIDPARRY": "EIDPARRY.NS", "EIHOTEL": "EIHOTEL.NS",
    "ELECON": "ELECON.NS", "ELGIEQUIP": "ELGIEQUIP.NS", "EMAMILTD": "EMAMILTD.NS",
    "EMCURE": "EMCURE.NS", "EMMVEE": "EMMVEE.NS", "ENDURANCE": "ENDURANCE.NS",
    "ENGINERSIN": "ENGINERSIN.NS", "ENRIN": "ENRIN.NS", "ERIS": "ERIS.NS",
    "ESCORTS": "ESCORTS.NS", "ETERNAL": "ETERNAL.NS", "EXIDEIND": "EXIDEIND.NS",
    "FACT": "FACT.NS", "FEDERALBNK": "FEDERALBNK.NS", "FINCABLES": "FINCABLES.NS",
    "FIRSTCRY": "FIRSTCRY.NS", "FIVESTAR": "FIVESTAR.NS", "FLUOROCHEM": "FLUOROCHEM.NS",
    "FORCEMOT": "FORCEMOT.NS", "FORTIS": "FORTIS.NS", "FSL": "FSL.NS",
    "GABRIEL": "GABRIEL.NS", "GAIL": "GAIL.NS", "GALLANTT": "GALLANTT.NS",
    "GESHIP": "GESHIP.NS", "GICRE": "GICRE.NS", "GILLETTE": "GILLETTE.NS",
    "GLAND": "GLAND.NS", "GLAXO": "GLAXO.NS", "GLENMARK": "GLENMARK.NS",
    "GMDCLTD": "GMDCLTD.NS", "GMRAIRPORT": "GMRAIRPORT.NS", "GODFRYPHLP": "GODFRYPHLP.NS",
    "GODIGIT": "GODIGIT.NS", "GODREJCP": "GODREJCP.NS", "GODREJIND": "GODREJIND.NS",
    "GODREJPROP": "GODREJPROP.NS", "GPIL": "GPIL.NS", "GRANULES": "GRANULES.NS",
    "GRAPHITE": "GRAPHITE.NS", "GRASIM": "GRASIM.NS", "GRAVITA": "GRAVITA.NS",
    "GROWW": "GROWW.NS", "GRSE": "GRSE.NS", "GVT&D": "GVT&D.NS", "HAL": "HAL.NS",
    "HAVELLS": "HAVELLS.NS", "HBLENGINE": "HBLENGINE.NS", "HCLTECH": "HCLTECH.NS",
    "HDBFS": "HDBFS.NS", "HDFCAMC": "HDFCAMC.NS", "HDFCBANK": "HDFCBANK.NS",
    "HDFCLIFE": "HDFCLIFE.NS", "HEG": "HEG.NS", "HEROMOTOCO": "HEROMOTOCO.NS",
    "HEXT": "HEXT.NS", "HFCL": "HFCL.NS", "HINDALCO": "HINDALCO.NS",
    "HINDCOPPER": "HINDCOPPER.NS", "HINDPETRO": "HINDPETRO.NS",
    "HINDUNILVR": "HINDUNILVR.NS", "HINDZINC": "HINDZINC.NS", "HOMEFIRST": "HOMEFIRST.NS",
    "HONASA": "HONASA.NS", "HONAUT": "HONAUT.NS", "HSCL": "HSCL.NS", "HUDCO": "HUDCO.NS",
    "HYUNDAI": "HYUNDAI.NS", "ICICIAMC": "ICICIAMC.NS", "ICICIBANK": "ICICIBANK.NS",
    "ICICIGI": "ICICIGI.NS", "ICICIPRULI": "ICICIPRULI.NS", "IDBI": "IDBI.NS",
    "IDEA": "IDEA.NS", "IDFCFIRSTB": "IDFCFIRSTB.NS", "IEX": "IEX.NS", "IFCI": "IFCI.NS",
    "IGIL": "IGIL.NS", "IGL": "IGL.NS", "IIFL": "IIFL.NS", "IKS": "IKS.NS",
    "INDGN": "INDGN.NS", "INDHOTEL": "INDHOTEL.NS", "INDIACEM": "INDIACEM.NS",
    "INDIAMART": "INDIAMART.NS", "INDIANB": "INDIANB.NS", "INDIGO": "INDIGO.NS",
    "INDUSINDBK": "INDUSINDBK.NS", "INDUSTOWER": "INDUSTOWER.NS", "INFY": "INFY.NS",
    "INOXWIND": "INOXWIND.NS", "INTELLECT": "INTELLECT.NS", "IOB": "IOB.NS",
    "IOC": "IOC.NS", "IPCALAB": "IPCALAB.NS", "IRB": "IRB.NS", "IRCON": "IRCON.NS",
    "IRCTC": "IRCTC.NS", "IREDA": "IREDA.NS", "IRFC": "IRFC.NS", "ITC": "ITC.NS",
    "ITCHOTELS": "ITCHOTELS.NS", "ITI": "ITI.NS", "J&KBANK": "J&KBANK.NS",
    "JAINREC": "JAINREC.NS", "JBMA": "JBMA.NS", "JINDALSAW": "JINDALSAW.NS",
    "JINDALSTEL": "JINDALSTEL.NS", "JIOFIN": "JIOFIN.NS", "JKCEMENT": "JKCEMENT.NS",
    "JKTYRE": "JKTYRE.NS", "JMFINANCIL": "JMFINANCIL.NS", "JPPOWER": "JPPOWER.NS",
    "JSL": "JSL.NS", "JSWCEMENT": "JSWCEMENT.NS", "JSWDULUX": "JSWDULUX.NS",
    "JSWENERGY": "JSWENERGY.NS", "JSWINFRA": "JSWINFRA.NS", "JSWSTEEL": "JSWSTEEL.NS",
    "JUBLFOOD": "JUBLFOOD.NS", "JUBLINGREA": "JUBLINGREA.NS",
    "JUBLPHARMA": "JUBLPHARMA.NS", "JWL": "JWL.NS", "JYOTICNC": "JYOTICNC.NS",
    "KAJARIACER": "KAJARIACER.NS", "KALYANKJIL": "KALYANKJIL.NS",
    "KARURVYSYA": "KARURVYSYA.NS", "KAYNES": "KAYNES.NS", "KEC": "KEC.NS", "KEI": "KEI.NS",
    "KFINTECH": "KFINTECH.NS", "KIMS": "KIMS.NS", "KIRLOSENG": "KIRLOSENG.NS",
    "KOTAKBANK": "KOTAKBANK.NS", "KPIL": "KPIL.NS", "KPITTECH": "KPITTECH.NS",
    "KPRMILL": "KPRMILL.NS", "LALPATHLAB": "LALPATHLAB.NS", "LATENTVIEW": "LATENTVIEW.NS",
    "LAURUSLABS": "LAURUSLABS.NS", "LEMONTREE": "LEMONTREE.NS", "LENSKART": "LENSKART.NS",
    "LGEINDIA": "LGEINDIA.NS", "LICHSGFIN": "LICHSGFIN.NS", "LICI": "LICI.NS",
    "LINDEINDIA": "LINDEINDIA.NS", "LLOYDSME": "LLOYDSME.NS", "LODHA": "LODHA.NS",
    "LT": "LT.NS", "LTF": "LTF.NS", "LTFOODS": "LTFOODS.NS", "LTM": "LTM.NS",
    "LTTS": "LTTS.NS", "LUPIN": "LUPIN.NS", "M&M": "M&M.NS", "M&MFIN": "M&MFIN.NS",
    "MAHABANK": "MAHABANK.NS", "MANAPPURAM": "MANAPPURAM.NS", "MANKIND": "MANKIND.NS",
    "MAPMYINDIA": "MAPMYINDIA.NS", "MARICO": "MARICO.NS", "MARUTI": "MARUTI.NS",
    "MAXHEALTH": "MAXHEALTH.NS", "MAZDOCK": "MAZDOCK.NS", "MCX": "MCX.NS",
    "MEDANTA": "MEDANTA.NS", "MEESHO": "MEESHO.NS", "MFSL": "MFSL.NS", "MGL": "MGL.NS",
    "MINDACORP": "MINDACORP.NS", "MMTC": "MMTC.NS", "MOTHERSON": "MOTHERSON.NS",
    "MOTILALOFS": "MOTILALOFS.NS", "MPHASIS": "MPHASIS.NS", "MRF": "MRF.NS",
    "MRPL": "MRPL.NS", "MSUMI": "MSUMI.NS", "MUTHOOTFIN": "MUTHOOTFIN.NS",
    "NAM-INDIA": "NAM-INDIA.NS", "NATCOPHARM": "NATCOPHARM.NS",
    "NATIONALUM": "NATIONALUM.NS", "NAUKRI": "NAUKRI.NS", "NAVA": "NAVA.NS",
    "NAVINFLUOR": "NAVINFLUOR.NS", "NBCC": "NBCC.NS", "NCC": "NCC.NS",
    "NESTLEIND": "NESTLEIND.NS", "NETWEB": "NETWEB.NS", "NEULANDLAB": "NEULANDLAB.NS",
    "NEWGEN": "NEWGEN.NS", "NH": "NH.NS", "NHPC": "NHPC.NS", "NIACL": "NIACL.NS",
    "NIVABUPA": "NIVABUPA.NS", "NLCINDIA": "NLCINDIA.NS", "NMDC": "NMDC.NS",
    "NSLNISP": "NSLNISP.NS", "NTPC": "NTPC.NS", "NTPCGREEN": "NTPCGREEN.NS",
    "NUVAMA": "NUVAMA.NS", "NUVOCO": "NUVOCO.NS", "NYKAA": "NYKAA.NS",
    "OBEROIRLTY": "OBEROIRLTY.NS", "OFSS": "OFSS.NS", "OIL": "OIL.NS",
    "OLAELEC": "OLAELEC.NS", "OLECTRA": "OLECTRA.NS", "ONESOURCE": "ONESOURCE.NS",
    "ONGC": "ONGC.NS", "PAGEIND": "PAGEIND.NS", "PARADEEP": "PARADEEP.NS",
    "PATANJALI": "PATANJALI.NS", "PAYTM": "PAYTM.NS", "PCBL": "PCBL.NS",
    "PERSISTENT": "PERSISTENT.NS", "PETRONET": "PETRONET.NS", "PFC": "PFC.NS",
    "PFIZER": "PFIZER.NS", "PFOCUS": "PFOCUS.NS", "PGEL": "PGEL.NS",
    "PHOENIXLTD": "PHOENIXLTD.NS", "PIDILITIND": "PIDILITIND.NS", "PIIND": "PIIND.NS",
    "PINELABS": "PINELABS.NS", "PIRAMALFIN": "PIRAMALFIN.NS", "PNB": "PNB.NS",
    "PNBHOUSING": "PNBHOUSING.NS", "POLICYBZR": "POLICYBZR.NS", "POLYCAB": "POLYCAB.NS",
    "POLYMED": "POLYMED.NS", "POONAWALLA": "POONAWALLA.NS", "POWERGRID": "POWERGRID.NS",
    "POWERINDIA": "POWERINDIA.NS", "PPLPHARMA": "PPLPHARMA.NS",
    "PREMIERENE": "PREMIERENE.NS", "PRESTIGE": "PRESTIGE.NS", "PTCIL": "PTCIL.NS",
    "PVRINOX": "PVRINOX.NS", "PWL": "PWL.NS", "RADICO": "RADICO.NS",
    "RAILTEL": "RAILTEL.NS", "RAINBOW": "RAINBOW.NS", "RAMCOCEM": "RAMCOCEM.NS",
    "RBLBANK": "RBLBANK.NS", "RECLTD": "RECLTD.NS", "REDINGTON": "REDINGTON.NS",
    "RELIANCE": "RELIANCE.NS", "RHIM": "RHIM.NS", "RITES": "RITES.NS",
    "RKFORGE": "RKFORGE.NS", "RPOWER": "RPOWER.NS", "RRKABEL": "RRKABEL.NS",
    "RVNL": "RVNL.NS", "SAGILITY": "SAGILITY.NS", "SAIL": "SAIL.NS",
    "SAILIFE": "SAILIFE.NS", "SAMMAANCAP": "SAMMAANCAP.NS", "SAPPHIRE": "SAPPHIRE.NS",
    "SARDAEN": "SARDAEN.NS", "SAREGAMA": "SAREGAMA.NS", "SBFC": "SBFC.NS",
    "SBICARD": "SBICARD.NS", "SBILIFE": "SBILIFE.NS", "SBIN": "SBIN.NS",
    "SCHAEFFLER": "SCHAEFFLER.NS", "SCHNEIDER": "SCHNEIDER.NS", "SCI": "SCI.NS",
    "SHREECEM": "SHREECEM.NS", "SHRIRAMFIN": "SHRIRAMFIN.NS", "SHYAMMETL": "SHYAMMETL.NS",
    "SIEMENS": "SIEMENS.NS", "SIGNATURE": "SIGNATURE.NS", "SJVN": "SJVN.NS",
    "SOBHA": "SOBHA.NS", "SOLARINDS": "SOLARINDS.NS", "SONACOMS": "SONACOMS.NS",
    "SONATSOFTW": "SONATSOFTW.NS", "SPLPETRO": "SPLPETRO.NS", "SRF": "SRF.NS",
    "STARHEALTH": "STARHEALTH.NS", "SUMICHEM": "SUMICHEM.NS",
    "SUNDARMFIN": "SUNDARMFIN.NS", "SUNPHARMA": "SUNPHARMA.NS", "SUNTV": "SUNTV.NS",
    "SUPREMEIND": "SUPREMEIND.NS", "SUZLON": "SUZLON.NS", "SWANCORP": "SWANCORP.NS",
    "SWIGGY": "SWIGGY.NS", "SYNGENE": "SYNGENE.NS", "SYRMA": "SYRMA.NS",
    "TARIL": "TARIL.NS", "TATACAP": "TATACAP.NS", "TATACHEM": "TATACHEM.NS",
    "TATACOMM": "TATACOMM.NS", "TATACONSUM": "TATACONSUM.NS", "TATAELXSI": "TATAELXSI.NS",
    "TATAINVEST": "TATAINVEST.NS", "TATAPOWER": "TATAPOWER.NS",
    "TATASTEEL": "TATASTEEL.NS", "TATATECH": "TATATECH.NS", "TBOTEK": "TBOTEK.NS",
    "TCS": "TCS.NS", "TECHM": "TECHM.NS", "TECHNOE": "TECHNOE.NS", "TEGA": "TEGA.NS",
    "TEJASNET": "TEJASNET.NS", "TENNIND": "TENNIND.NS", "THELEELA": "THELEELA.NS",
    "THERMAX": "THERMAX.NS", "TIINDIA": "TIINDIA.NS", "TIMKEN": "TIMKEN.NS",
    "TITAGARH": "TITAGARH.NS", "TITAN": "TITAN.NS", "TMCV": "TMCV.NS", "TMPV": "TMPV.NS",
    "TORNTPHARM": "TORNTPHARM.NS", "TORNTPOWER": "TORNTPOWER.NS",
    "TRAVELFOOD": "TRAVELFOOD.NS", "TRENT": "TRENT.NS", "TRIDENT": "TRIDENT.NS",
    "TRITURBINE": "TRITURBINE.NS", "TTML": "TTML.NS", "TVSMOTOR": "TVSMOTOR.NS",
    "UBL": "UBL.NS", "UCOBANK": "UCOBANK.NS", "ULTRACEMCO": "ULTRACEMCO.NS",
    "UNIONBANK": "UNIONBANK.NS", "UNITDSPR": "UNITDSPR.NS", "UNOMINDA": "UNOMINDA.NS",
    "UPL": "UPL.NS", "URBANCO": "URBANCO.NS", "USHAMART": "USHAMART.NS",
    "UTIAMC": "UTIAMC.NS", "VBL": "VBL.NS", "VEDL": "VEDL.NS", "VIJAYA": "VIJAYA.NS",
    "VMM": "VMM.NS", "VOLTAS": "VOLTAS.NS", "VTL": "VTL.NS", "WAAREEENER": "WAAREEENER.NS",
    "WELCORP": "WELCORP.NS", "WELSPUNLIV": "WELSPUNLIV.NS", "WHIRLPOOL": "WHIRLPOOL.NS",
    "WIPRO": "WIPRO.NS", "WOCKPHARMA": "WOCKPHARMA.NS", "YESBANK": "YESBANK.NS",
    "ZEEL": "ZEEL.NS", "ZENSARTECH": "ZENSARTECH.NS", "ZENTEC": "ZENTEC.NS",
    "ZFCVINDIA": "ZFCVINDIA.NS", "ZYDUSLIFE": "ZYDUSLIFE.NS", "ZYDUSWELL": "ZYDUSWELL.NS",
}


# ── Indicators ─────────────────────────────────────────────────────────────────
def calc_rsi(close: pd.Series, period: int) -> pd.Series:
    """Wilder's RSI."""
    delta = close.diff()
    gain  = delta.clip(lower=0)
    loss  = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calc_williams_r(high: pd.Series, low: pd.Series, close: pd.Series,
                    period: int) -> pd.Series:
    hh = high.rolling(period).max()
    ll = low.rolling(period).min()
    return (hh - close) / (hh - ll) * -100


def calc_cci(source: pd.Series, period: int) -> pd.Series:
    """CCI computed on an arbitrary input series (e.g. Daily High), matching
    Chartink's generalized 'CCI Line(source, period)' function."""
    sma = source.rolling(period).mean()
    mad = source.rolling(period).apply(lambda x: np.abs(x - x.mean()).mean(), raw=True)
    return (source - sma) / (0.015 * mad)


def crossed_above(prev_val: float, curr_val: float, level: float) -> bool:
    return prev_val <= level and curr_val > level


# ── Yahoo Finance download helpers ──────────────────────────────────────────────
def extract_ohlc(df, ticker):
    """Return an OHLC DataFrame for `ticker` from a (possibly multi-ticker) batch."""
    if isinstance(df.columns, pd.MultiIndex):
        cols = {}
        for label in ["Open", "High", "Low", "Close"]:
            if (label, ticker) in df.columns:
                cols[label] = df[(label, ticker)]
        if len(cols) < 4:
            return None
        return pd.DataFrame(cols).dropna()
    if all(c in df.columns for c in ["Open", "High", "Low", "Close"]):
        return df[["Open", "High", "Low", "Close"]].dropna()
    return None


def download_individual(ticker, symbol):
    for attempt in range(MAX_RETRIES):
        try:
            df = yf.download(ticker, period="2y", interval="1d", progress=False)
            if not df.empty:
                return df
            print(f"  ⚠️  {symbol} ({ticker}): empty response")
            return pd.DataFrame()
        except Exception as e:
            wait = 2 ** (attempt + 1)
            if attempt < MAX_RETRIES - 1:
                print(f"  ↩️  {symbol} retry {attempt + 1}/{MAX_RETRIES - 1} in {wait}s — {e}")
                time.sleep(wait)
            else:
                print(f"  ❌ {symbol}: all retries exhausted — {e}")
    return pd.DataFrame()


def get_ohlc(batch_df, ticker, batch_tickers, symbol):
    if not batch_df.empty:
        ohlc = extract_ohlc(batch_df, ticker)
        if ohlc is not None and len(ohlc) > 0:
            return ohlc

    print(f"  ↩️  {symbol} ({ticker}): not in batch — downloading individually...")
    time.sleep(SLEEP_INDIVIDUAL)
    ind_df = download_individual(ticker, symbol)
    if ind_df.empty:
        return None
    return extract_ohlc(ind_df, ticker)


def check_signal(ohlc: pd.DataFrame):
    """Return a match dict if RSI/Williams %R/CCI all crossed above their
    levels on the most recent completed day, else None."""
    lookback = max(RSI_PERIOD, WR_PERIOD, CCI_PERIOD) + 2
    if len(ohlc) < lookback:
        return None

    rsi = calc_rsi(ohlc["Close"], RSI_PERIOD)
    wr  = calc_williams_r(ohlc["High"], ohlc["Low"], ohlc["Close"], WR_PERIOD)
    cci = calc_cci(ohlc["High"], CCI_PERIOD)

    r_p, r_c = rsi.iloc[-2], rsi.iloc[-1]
    w_p, w_c = wr.iloc[-2], wr.iloc[-1]
    c_p, c_c = cci.iloc[-2], cci.iloc[-1]
    if pd.isna(r_p) or pd.isna(r_c) or pd.isna(w_p) or pd.isna(w_c) \
            or pd.isna(c_p) or pd.isna(c_c):
        return None

    if not (crossed_above(r_p, r_c, RSI_LEVEL) and
            crossed_above(w_p, w_c, WR_LEVEL) and
            crossed_above(c_p, c_c, CCI_LEVEL)):
        return None

    cmp_ = float(ohlc["Close"].iloc[-1])
    return dict(
        scan_date=ohlc.index[-1].date().isoformat(),
        cmp=round(cmp_, 2),
        rsi=round(float(r_c), 2),
        williams_r=round(float(w_c), 2),
        cci=round(float(c_c), 2),
        target=round(cmp_ * (1 + TARGET_PCT), 2),
    )


# ── Google Sheets connection ───────────────────────────────────────────────────
scope  = ["https://spreadsheets.google.com/feeds",
          "https://www.googleapis.com/auth/drive"]
creds  = Credentials.from_service_account_file(CREDS_PATH, scopes=scope)
client = gspread.authorize(creds)

spreadsheet = client.open(SHEET_NAME)
existing_ws = [ws.title for ws in spreadsheet.worksheets()]
if WORKSHEET_NAME not in existing_ws:
    spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows=200, cols=8)
    print(f"  Created new worksheet: '{WORKSHEET_NAME}'")
sheet = spreadsheet.worksheet(WORKSHEET_NAME)

HEADER = ["Scan Date", "Symbol", "CMP", "RSI(14)", "Williams %R(140)",
          "CCI(High,200)", f"Target (+{TARGET_PCT*100:.0f}%)"]
if sheet.row_values(1) != HEADER:
    sheet.update(range_name="A1", values=[HEADER])
    print("  Header written.")

# ── Scan universe ───────────────────────────────────────────────────────────────
symbols = list(NIFTY500_STOCKS.items())
total_batches = (len(symbols) - 1) // BATCH_SIZE + 1
print(f"Scanning {len(symbols)} Nifty 500 stocks across {total_batches} batches "
      f"| RSI({RSI_PERIOD})>{RSI_LEVEL}  WilliamsR({WR_PERIOD})>{WR_LEVEL}  "
      f"CCI(High,{CCI_PERIOD})>{CCI_LEVEL}")
print("─" * 65)

matches = []
failed  = []

for b_start in range(0, len(symbols), BATCH_SIZE):
    batch         = symbols[b_start:b_start + BATCH_SIZE]
    b_num         = b_start // BATCH_SIZE + 1
    batch_tickers = [t for _, t in batch]

    print(f"\n⬇️  Batch {b_num}/{total_batches} — downloading {len(batch_tickers)} tickers...")
    try:
        if len(batch_tickers) == 1:
            batch_df = yf.download(batch_tickers[0], period="2y", interval="1d", progress=False)
        else:
            batch_df = yf.download(batch_tickers, period="2y", interval="1d", progress=False)
        print(f"   Batch {b_num} downloaded — {len(batch_df)} daily rows")
    except Exception as e:
        print(f"   ⚠️  Batch {b_num} failed ({e}) — will fall back to individual downloads")
        batch_df = pd.DataFrame()

    for symbol, ticker in batch:
        try:
            ohlc = get_ohlc(batch_df, ticker, batch_tickers, symbol)
            if ohlc is None or ohlc.empty:
                failed.append(symbol)
                continue
            hit = check_signal(ohlc)
            if hit:
                hit["symbol"] = symbol
                matches.append(hit)
                print(f"   🎯 {symbol:12} MATCH  | CMP={hit['cmp']}  "
                      f"RSI={hit['rsi']}  W%R={hit['williams_r']}  CCI={hit['cci']}")
        except Exception as e:
            print(f"   ❌ {symbol:12}: {e}")
            failed.append(symbol)

    if b_start + BATCH_SIZE < len(symbols):
        time.sleep(SLEEP_BETWEEN_BATCHES)

print("\n" + "─" * 65)
print(f"Scan complete — {len(matches)} match(es) found, {len(failed)} symbol(s) failed to fetch.")

# ── Write matches to Google Sheet ───────────────────────────────────────────────
if matches:
    rows = [[m["scan_date"], m["symbol"], m["cmp"], m["rsi"],
             m["williams_r"], m["cci"], m["target"]] for m in matches]
    for i in range(0, len(rows), WRITE_CHUNK):
        sheet.append_rows(rows[i:i + WRITE_CHUNK], value_input_option="USER_ENTERED")
        time.sleep(1)
    print(f"📤 Appended {len(rows)} matching stock(s) to '{WORKSHEET_NAME}'.")
else:
    print("No stocks matched today — nothing written to the sheet.")

if failed:
    print(f"\n⚠️  No data for {len(failed)} symbol(s): {', '.join(failed[:20])}"
          f"{' ...' if len(failed) > 20 else ''}")
