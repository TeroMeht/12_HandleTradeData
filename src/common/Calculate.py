import pandas as pd

# in = df (Open, High, Low, Close, Volume)
# out = df (Open, High, Low, Close, Volume, VWAP)
def calculate_vwap(data):
    data = data.copy()
    data['OHLC4'] = (data['Open'] + data['High'] + data['Low'] + data['Close']) / 4
    cumulative_vol = data['Volume'].cumsum()
    cumulative_pv = (data['OHLC4'] * data['Volume']).cumsum()
    data['VWAP'] = (cumulative_pv / cumulative_vol).fillna(0).round(2)
    data.drop(columns=['OHLC4'], inplace=True)
    return data


# in = df (Open, High, Low, Close, Volume)
# out = df (Open, High, Low, Close, Volume, EMA9)
def calculate_ema(data, period):

    if 'Close' not in data.columns:
        raise ValueError("The DataFrame must contain a 'Close' column.")

    column_name = f'EMA{period}'
    data[column_name] = data['Close'].ewm(span=period, adjust=False).mean().round(2)
    return data

# in = df (High, Low, Close)
# out = df (High, Low, Close, Prev_Close, TR, ATR)
def calculate_14day_atr(data, period=14):
    """
    Calculate 14-day ATR for all rows and return a DataFrame with ATR column.
    Input: DataFrame with at least High, Low, Close columns.
    Output: DataFrame with Prev_Close, TR, and ATR columns added.
    """
    df = data.copy()

    # Previous close
    df['Prev_Close'] = df['Close'].shift(1)

    # True Range (TR)
    df['TR'] = df.apply(
        lambda row: max(
            row['High'] - row['Low'],
            abs(row['High'] - row['Prev_Close']) if pd.notnull(row['Prev_Close']) else row['High'] - row['Low'],
            abs(row['Low'] - row['Prev_Close']) if pd.notnull(row['Prev_Close']) else row['High'] - row['Low']
        ),
        axis=1
    )

    # ATR: exponential moving average of TR (rounded to 4 decimals)
    df['ATR'] = df['TR'].ewm(span=period, adjust=False).mean().round(4)

    return df

def calculate_rvol(data, period = 5):

    data = data.copy()
    
    # Calculate 5-day average volume
    data['5DayAvgVolume'] = data['Volume'].rolling(window=period).mean()
    
    # Calculate relative volume
    data['RelativeVolume'] = data['Volume'] / data['5DayAvgVolume']
    
    return data

# intraday data and daily atr data Relatr calculation will be done and column added
def calculate_relatr(intraday_df, daily_atr_df):
    """
    Adds a Relatr column to intraday_df using the last ATR value from daily_atr_df.
    
    intraday_df: DataFrame with columns ['Symbol', 'Close', 'VWAP', ...]
    daily_atr_df: DataFrame with columns ['Symbol', 'ATR', ...]
    """
    intraday_df = intraday_df.copy()
    
    # Get last ATR value per symbol
    last_atr = daily_atr_df.groupby('Symbol')['ATR'].last().to_dict()

    # Compute Relatr for each row
    intraday_df['Relatr'] = intraday_df.apply(
        lambda row: round((row['VWAP'] - row['Close']) / last_atr.get(row['Symbol'], 1), 2),
        axis=1
    )
    
    return intraday_df