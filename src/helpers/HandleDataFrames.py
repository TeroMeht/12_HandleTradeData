from common.Calculate import *
from common.AdjustTimezone import adjust_timezone_IB_data
from database.DBfunctions import *


def prepare_bars_dataframe(bars_df, symbol):
    """
    Clean incoming DataFrame:
    - Drop unwanted columns
    - Capitalize column names
    - Add Symbol column
    Returns cleaned DataFrame or None if empty/error.
    """
    try:
        if bars_df is None or bars_df.empty:
            print("prepare_bars_dataframe: received empty DataFrame")
            return None

        df = bars_df.copy()

        # Drop unnecessary columns if present
        df.drop(columns=[col for col in ['average', 'barCount'] if col in df.columns],
                inplace=True, errors='ignore')

        # Capitalize column names
        df.columns = [col.capitalize() for col in df.columns]

        # Add Symbol column
        df['Symbol'] = symbol

        return df

    except Exception as e:
        print(f"prepare_bars_dataframe: error preparing DataFrame: {e}")
        return None


def handle_incoming_dataframe_daily(
    bars_df: pd.DataFrame, 
    symbol: str, 
    trade_id: int
) -> pd.DataFrame | None:
    """
    Process incoming daily bars DataFrame:
    - Clean with prepare_bars_dataframe
    - Select required columns
    - Calculate indicators (RVOL, optionally ATR)
    - Add TradeId
    Returns processed DataFrame or None if input is empty/invalid.
    """
    try:
        # Step 1: Clean and prepare
        df = prepare_bars_dataframe(bars_df, symbol)
        if df is None:
            print(f"[Daily Handler] No data for symbol {symbol}")
            return None

        # Step 2: Select required columns
        required_columns = ['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        df = df[required_columns]

        # Step 3: Calculate indicators
        df = calculate_rvol(df)
        # df = calculate_14day_atr(df)  # Uncomment if needed

        # Step 4: Add TradeId
        df = df.copy()
        df['TradeId'] = trade_id

        return df

    except Exception as e:
        print(f"[Daily Handler] Error processing symbol {symbol}: {e}")
        return None



def handle_incoming_dataframe_midterm(
    bars_df: pd.DataFrame, 
    symbol: str, 
    trade_id: int
) -> pd.DataFrame | None:
    """
    Process midterm bars:
    - Clean using prepare_bars_dataframe
    - Adjust timezone on Date column
    - Calculate EMA65
    - Add TradeId
    """
    try:
        # Step 1: Prepare DataFrame
        df = prepare_bars_dataframe(bars_df, symbol)
        if df is None:
            print(f"[Midterm Handler] No data for symbol {symbol}")
            return None

        # Step 2: Apply timezone adjustment to 'Date' (capitalized by prepare_bars_dataframe)
        if 'Date' in df.columns:
            df['Date'] = df['Date'].apply(adjust_timezone_IB_data)

        # Step 3: Calculate EMA65
        df = calculate_ema(df, 65)

        # Step 4: Select required columns
        df = df[['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'EMA65']]

        # Step 5: Add TradeId
        df = df.copy()
        df['TradeId'] = trade_id

        return df

    except Exception as e:
        print(f"[Midterm Handler] Error processing symbol {symbol}: {e}")
        return None

def handle_incoming_dataframe_intraday(
    bars_df: pd.DataFrame, 
    symbol: str, 
    trade_id: int
) -> pd.DataFrame | None:
    """
    Process intraday bars:
    - Clean using prepare_bars_dataframe
    - Adjust timezone on Date column
    - Calculate VWAP and EMA9
    - Split Date into Date and Time
    - Add TradeId
    """
    try:
        # Step 1: Prepare DataFrame
        df = prepare_bars_dataframe(bars_df, symbol)
        if df is None:
            print(f"[Intraday Handler] No data for symbol {symbol}")
            return None

        # Step 2: Apply timezone adjustment to 'Date' column
        if 'Date' in df.columns:
            df['Date'] = df['Date'].apply(adjust_timezone_IB_data)

        # Step 3: Calculate indicators
        df = calculate_vwap(df)
        df = calculate_ema(df, period=9)

        # Step 4: Split Date into Date and Time
        df['Date'] = df['Date'].astype(str)
        df[['Date', 'Time']] = df['Date'].str.split(' ', expand=True)

        # Step 5: Add TradeId
        df = df.copy()
        df['TradeId'] = trade_id

        # Step 6: Select final columns
        final_columns = [
            'Symbol', 'Date', 'Time', 'Open', 'High', 'Low', 'Close',
            'Volume', 'VWAP', 'EMA9', 'TradeId'
        ]
        df = df[final_columns]

        return df

    except Exception as e:
        print(f"[Intraday Handler] Error processing symbol {symbol}: {e}")
        return None
    
def handle_incoming_dataframe_atr(
    bars_df: pd.DataFrame,
    symbol: str, 
    trade_id: int
) -> pd.DataFrame | None:
    """
    Process ATR bars:
    - Clean using prepare_bars_dataframe
    - Select relevant columns
    - Calculate 14-day ATR
    - Add TradeId
    """
    try:
        # Step 1: Prepare DataFrame
        df = prepare_bars_dataframe(bars_df, symbol)
        if df is None:
            print(f"[ATR Handler] No data for symbol {symbol}")
            return None

        # Step 2: Select required columns
        df = df[['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

        # Step 3: Calculate ATR
        df = calculate_14day_atr(df)

        # Step 4: Add TradeId
        df = df.copy()
        df['TradeId'] = trade_id

        return df

    except Exception as e:
        print(f"[ATR Handler] Error processing symbol {symbol}: {e}")
        return None