

# Function to calculate VWAP
def calculate_vwap(data):
    # Initialize VWAP column with float zeros
    data['VWAP'] = 0.0  # Explicitly set to float

    # Calculate OHLC4 and add it as a new column
    data['OHLC4'] = (data['Open'] + data['High'] + data['Low'] + data['Close']) / 4


    for i, row in data.iterrows():
        # Use OHLC4 instead of Close for VWAP calculation
        cumulative_volume = data.loc[:i, 'Volume'].sum()
        cumulative_price_volume = (data.loc[:i, 'OHLC4'] * data.loc[:i, 'Volume']).sum()
        data.at[i, 'VWAP'] = cumulative_price_volume / cumulative_volume if cumulative_volume != 0 else 0.0
        data['VWAP'] = data['VWAP'].round(2)
        
    # Drop the OHLC4 column if it's not needed in the final output
    data.drop(columns=['OHLC4'], inplace=True)

    return data

def calculate_ema(data, period):

    if 'Close' not in data.columns:
        raise ValueError("The DataFrame must contain a 'Close' column.")

    column_name = f'EMA{period}'
    data[column_name] = data['Close'].ewm(span=period, adjust=False).mean().round(2)
    return data

def calculate_14day_atr(data,period=14):

    data['TR'] = data[['High', 'Low', 'Close']].apply(
        lambda row: max(row['High'] - row['Low'], 
                        abs(row['High'] - row['Close']), 
                        abs(row['Low'] - row['Close'])), axis=1)

    # Calculate ATR using Exponential Moving Average (EMA)
    data['ATR'] = data['TR'].ewm(span=period, adjust=False).mean()

    return data

def calculate_rvol(data, period = 5):

    data = data.copy()
    
    # Calculate 5-day average volume
    data['5DayAvgVolume'] = data['Volume'].rolling(window=period).mean()
    
    # Calculate relative volume
    data['RelativeVolume'] = data['Volume'] / data['5DayAvgVolume']
    
    return data

def calculate_vwap_relative_atr(data, atr_value):

    if atr_value is None or atr_value == 0:
        raise ValueError("Invalid ATR value. Cannot divide by zero.")

    # Calculate Relative ATR
    data['Relatr'] = (data['VWAP'] - data['Close']) / atr_value
        # Round the Relatr column to 2 decimal places
    data['Relatr'] = data['Relatr'].round(2)
    
    return data


# tää ei oo viel käytössä missään
def calculate_sma(data, period):
    if 'Close' not in data.columns:
        raise ValueError("The DataFrame must contain a 'Close' column.")

    column_name = f'SMA{period}'
    data[column_name] = data['Close'].ewm(span=period, adjust=False).mean().round(2)
    return data
