from common.Calculate import *
from common.AdjustTimezone import adjust_timezone_IB_data
from helpers.DBfunctions import *



def handle_incoming_dataframe_daily(bars_df, symbol, trade_id):
    try:
        if not bars_df.empty:
            # Drop unnecessary columns if present
            bars_df = bars_df.drop(columns=[col for col in ['average', 'barCount'] if col in bars_df.columns])

            # Capitalize column names and assign symbol
            bars_df.columns = [col.capitalize() for col in bars_df.columns]
            bars_df['Symbol'] = symbol

            # Select required columns
            bars_df = bars_df[['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

            # Calculate indicators
            data = calculate_rvol(bars_df)
            data = calculate_14day_atr(data)

            data = data.copy()  # make a copy first to avoid the warning
            data.loc[:, 'TradeId'] = trade_id

            return data  # Or call insert_marketdata(data, cursor, connection)

        else:
            print("Empty data")

    except Exception as e:
        print(f"An error occurred while processing the data: {e}")


def handle_incoming_dataframe_midterm(bars_df, symbol, trade_id):
    try:
        # Check if the DataFrame is not empty
        if not bars_df.empty:
            # Drop unnecessary columns (if they exist)
            if 'average' in bars_df.columns and 'barCount' in bars_df.columns:
                bars_df = bars_df.drop(columns=['average', 'barCount'])

            # Apply the timezone adjustment function to the 'date' column
            if 'date' in bars_df.columns:
                bars_df['date'] = bars_df['date'].apply(adjust_timezone_IB_data)

            # Capitalize column names
            bars_df.columns = [col.capitalize() for col in bars_df.columns]
            bars_df['Symbol'] = symbol
                                    # Calculate EMA9
            bars_df = calculate_ema(bars_df,65)
            # Format the DataFrame (select relevant columns)
            data = bars_df[['Symbol','Date', 'Open', 'High', 'Low', 'Close', 'Volume','EMA65']]


            data = data.copy()  # make a copy first to avoid the warning
            data.loc[:, 'TradeId'] = trade_id

            return data

        else:
            print("empty data")

    except Exception as e:
        # General error handler for any other exceptions
        error_msg = f"An error occurred while processing the data: {e}"
        print(error_msg)


def handle_incoming_dataframe_intraday(bars_df, symbol, trade_id, database_config):
    try:
        # Check if the DataFrame is not empty
        if bars_df is not None and not bars_df.empty:
            # Drop unnecessary columns
            for col in ['average', 'barCount']:
                if col in bars_df.columns:
                    bars_df = bars_df.drop(columns=[col])

            # Apply the timezone adjustment
            if 'date' in bars_df.columns:
                bars_df['date'] = bars_df['date'].apply(adjust_timezone_IB_data)

            # Capitalize column names
            bars_df.columns = [col.capitalize() for col in bars_df.columns]
            bars_df['Symbol'] = symbol

            # Select relevant columns
            bars_df = bars_df[['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

            # Calculate indicators
            bars_df = calculate_vwap(bars_df)
            bars_df = calculate_ema(bars_df, 9)

            # Get ATR value and calculate Relatr
            atr_value = fetch_atr_value(trade_id, database_config)
            print(f"ATR Value: {atr_value}")
            if atr_value is not None:
                atr_value = float(atr_value)
                bars_df = calculate_vwap_relative_atr(bars_df, atr_value)
            else:
                print("ATR value is None; skipping relative VWAP calculation.")
                bars_df['Relatr'] = None  # Fill with None to keep schema

            # Split the Date column into Date and Time
            if bars_df['Date'].dtype == object:
                bars_df[['Date', 'Time']] = bars_df['Date'].str.split(' ', expand=True)
            else:
                bars_df['Date'] = bars_df['Date'].astype(str)
                bars_df[['Date', 'Time']] = bars_df['Date'].str.split(' ', expand=True)

            # Final formatting
            data = bars_df[['Symbol', 'Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'VWAP', 'EMA9', 'Relatr']].copy()
            data.loc[:, 'TradeId'] = trade_id

            return data


        else:
            print("empty data")

    except Exception as e:
        # General error handler for any other exceptions
        error_msg = f"An error occurred while processing the data: {e}"
        print(error_msg)