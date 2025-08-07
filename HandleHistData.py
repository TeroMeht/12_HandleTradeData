from ib_insync import *
import pandas as pd
import os
from Calculate import calculate_vwap, calculate_ema, calculate_rvol,calculate_14day_atr,calculate_vwap_relative_atr,calculate_sma  # Use VWAP function from another code
from AdjustTimezone import adjust_timezone_IB_data
from time import sleep
from db_connection import connect_db
from config import read_project_config


# Keskustelu oman kannan kanssa
def get_all_transactions(cursor):
    try:
        query = """
            SELECT 
                "Symbol", "Date","Time", "PermId", "AvgPrice", "Shares", 
                "Side", "Commission", "AdjustedAvgPrice"
            FROM executions
            ORDER BY "Time" ASC;
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        return df

    except Exception as e:
        print(f" Error fetching transactions: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error

def get_uniquetickers_and_dates(df):
    try:
        # Ensure Date is a string in YYYYMMDD format
        df["Date"] = df["Date"].astype(str).str.replace("-", "")  # Remove dashes if present

        # Drop duplicates and return only Symbol-Date pairs
        unique_pairs = df[["Symbol", "Date"]].drop_duplicates().reset_index(drop=True)

        return unique_pairs

    except Exception as e:
        print(f" Error parsing unique tickers and dates: {e}")
        return pd.DataFrame(columns=["Symbol", "Date"])

def insert_trades_to_db(df, cursor, connection):
    try: # Eli yhden traden ajatellaan pelkistetysti olevan Ticker, Date yhdistelmä. Vaikka samana päivänä olisi useampi niin
        # datankeruu riittää yhdelle.
        # Ensure the 'Date' column is datetime and format to string 'YYYY-MM-DD'
        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')

        # Extract unique (Symbol, Date) pairs
        values = df[["Symbol", "Date"]].drop_duplicates().values.tolist()

        if not values:
            print("No trades to insert.")
            return

        insert_query = """
            INSERT INTO trades ("Symbol", "Date")
            VALUES (%s, %s)
            ON CONFLICT ("Symbol", "Date") DO NOTHING;
        """

        cursor.executemany(insert_query, values)
        connection.commit()
        print(f"Inserted {len(values)} trades into 'trades' table (ignoring duplicates).")

    except Exception as e:
        print(f"Error inserting into trades table: {e}")
        connection.rollback()

def get_all_trades(cursor):
    try:
        query = '''
            SELECT * 
            FROM trades 
            WHERE "Symbol" <> 'CNDX' 
            ORDER BY "TradeId";
        '''
        cursor.execute(query)
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=colnames)
    except Exception as e:
        print(f"Error fetching trades: {e}")
        return pd.DataFrame()  # Return empty DataFrame on failure

def get_atr_value(trade_id, cursor):
    query = """
    SELECT m."ATR"
    FROM trades t
    JOIN marketdatad m
      ON m."Date" = t."Date"::date
     AND m."Symbol" = t."Symbol"
    WHERE t."TradeId" = %s
    LIMIT 1;
    """
    cursor.execute(query, (trade_id,))
    result = cursor.fetchone()
    if result:
        return result[0]  # ATR value
    else:
        return None

def check_trade_exists(cursor, table_name: str, trade_id: int) -> bool:
    try:
        query = f'''
            SELECT "TradeId"
            FROM "{table_name}"
            WHERE "TradeId" = %s
            LIMIT 1;
        '''
        cursor.execute(query, (trade_id,))
        row = cursor.fetchone()
        return bool(row)
    except Exception as e:
        print(f" Error checking if TradeId {trade_id} exists in table {table_name}: {e}")
        return False




def insert_marketdata_to_db(data, cursor, connection):
    try:
        if data.empty:
            print("No market data to insert.")
            return

        # Convert DataFrame to list of pure Python tuples
        values = [
            (
                str(row["Symbol"]),
                row["Date"].date() if hasattr(row["Date"], "date") else row["Date"],
                float(row["Open"]),
                float(row["High"]),
                float(row["Low"]),
                float(row["Close"]),
                int(row["Volume"]),
                float(row["5DayAvgVolume"]),
                float(row["RelativeVolume"]),
                float(row["TR"]),
                float(row["ATR"]),
                int(row["TradeId"])
            )
            for _, row in data.iterrows()
        ]

        insert_query = """
            INSERT INTO marketdatad (
                "Symbol", "Date", "Open", "High", "Low", "Close", "Volume",
                "5DayAvgVolume", "RelativeVolume", "TR", "ATR", "TradeId"
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ("Symbol", "Date", "TradeId") DO NOTHING;
        """

        cursor.executemany(insert_query, values)
        connection.commit()
        print(f"Inserting daily market data:  Symbol-Date: {data[['Symbol','Date']].drop_duplicates().iloc[0].to_dict()}")

    except Exception as e:
        print(f"Error inserting market data: {e}")
        connection.rollback()

def insert_marketdataintrad_to_db(data, cursor, connection):
    try:
        if data.empty:
            print("No intraday market data to insert.")
            return
        # Ensure types are compatible with SQL (especially numpy types)
        data = data.astype({
            'Open': 'float',
            'High': 'float',
            'Low': 'float',
            'Close': 'float',
            'Volume': 'int',
            'VWAP': 'float',
            'EMA9': 'float',
            'Relatr': 'float',
            'TradeId': 'int'
        })

        insert_query = """
            INSERT INTO marketdataintrad (
                "Symbol", "Date", "Time", "Open", "High", "Low", "Close",
                "Volume", "VWAP", "EMA9", "Relatr","TradeId"
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
            ON CONFLICT ON CONSTRAINT unique_marketdataintrad DO NOTHING;
        """

        # Prepare data rows
        values = data[[
            "Symbol", "Date", "Time", "Open", "High", "Low", "Close",
            "Volume", "VWAP", "EMA9", "Relatr" ,"TradeId"
        ]].values.tolist()

        cursor.executemany(insert_query, values)
        connection.commit()
        print(f"Inserting intraday market data: Symbol-Date: {data[['Symbol','Date']].drop_duplicates().iloc[0].to_dict()}")

    except Exception as e:
        print(f"Error inserting intraday market data: {e}")
        connection.rollback()

def insert_marketdata30mins(data, cursor, connection): 
    try:
        if data.empty:
            print("No intraday market data to insert.")
            return

        # Ensure correct data types
        data = data.astype({
            'Open': 'float',
            'High': 'float',
            'Low': 'float',
            'Close': 'float',
            'Volume': 'int',
            'EMA65': 'float',
            'TradeId': 'int'
        })

        insert_query = """
            INSERT INTO marketdata30mins (
                "Symbol", "Date", "Open", "High", "Low", "Close",
                "Volume", "EMA65","TradeId"
            )
            VALUES (%s, %s, %s, %s, %s,  %s, %s, %s,%s)
            ON CONFLICT ON CONSTRAINT unique_market30 DO NOTHING;
        """

        values = data[[
            "Symbol", "Date", "Open", "High", "Low", "Close",
            "Volume", "EMA65","TradeId"
        ]].values.tolist()

        cursor.executemany(insert_query, values)
        connection.commit()
        print(f"Inserting 30mins market data: Symbol-Date: {data[['Symbol','Date']].drop_duplicates().iloc[0].to_dict()}")

    except Exception as e:
        print(f"Error inserting intraday market data: {e}")
        connection.rollback()



# IB data handling

# Daily
def daily_data(df_data, ib, bar_size, durationStr, cursor, connection):
    for _, row in df_data.iterrows():
        symbol = row['Symbol']
        date = row['Date']
        # Remove hyphens from the date string to get e.g. '20240514'
        date_str = str(date).replace('-', '')
        end_date = f"{date_str} 16:59:59 US/Eastern"
        trade_id = row['TradeId']

        # Check if trade_id already exists
        if check_trade_exists(cursor, 'marketdatad', trade_id):
            continue

        try:
            contract = Stock(symbol, 'SMART', 'USD')
            contract.primaryExchange = 'ARCA'

            bars = ib.reqHistoricalData(
                contract,
                endDateTime=end_date,
                durationStr=durationStr,
                barSizeSetting=bar_size,
                whatToShow="TRADES",
                useRTH=0,
                formatDate=1
            )
        except Exception as e:
            print(f"Failed to fetch data for {symbol} on {end_date}. Error: {e}")
            continue

        bars_df = pd.DataFrame(bars)

        # Call handler with specific TradeId row
        data = handle_incoming_dataframe_daily(bars_df, symbol, trade_id)


        insert_marketdata_to_db(data, cursor, connection)

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

# 30mins
def midterm_data(df_data,ib, bar_size, durationStr, cursor, connection):

    for _, row in df_data.iterrows():
        symbol = row['Symbol']
        date = row['Date']
        # Remove hyphens from the date string to get e.g. '20240514'
        date_str = str(date).replace('-', '')
        end_date = f"{date_str} 16:59:59 US/Eastern"
        trade_id = row['TradeId']

        # Check if trade_id already exists
        if check_trade_exists(cursor, 'marketdata30mins', trade_id):
            continue
        try:
            contract = Stock(symbol, 'SMART', 'USD')
            contract.primaryExchange = 'ARCA'

            bars = ib.reqHistoricalData(
                contract,
                endDateTime=end_date,
                durationStr=durationStr,
                barSizeSetting=bar_size,
                whatToShow="TRADES",
                useRTH=0,
                formatDate=1
            )

            bars_df = pd.DataFrame(bars)

            data =  handle_incoming_dataframe_midterm(bars_df, symbol,adjust_timezone_IB_data,trade_id)
            insert_marketdata30mins(data,  cursor, connection)

        except Exception as e:
            print(f" Failed to fetch data for {symbol} on {end_date}. Error: {e}")
            continue

def handle_incoming_dataframe_midterm(bars_df, symbol,adjust_timezone_IB_data,trade_id):
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

# Intraday
def intraday_data(df_data,ib, bar_size, durationStr, cursor, connection):

    for _, row in df_data.iterrows():
        symbol = row['Symbol']  # Assuming 'Ticker' column contains the stock symbol
        date = row['Date']
        # Remove hyphens from the date string to get e.g. '20240514'
        date_str = str(date).replace('-', '')
        end_date = f"{date_str} 16:59:59 US/Eastern"
        trade_id = row['TradeId']

        # Check if trade_id already exists
        if check_trade_exists(cursor, 'marketdataintrad', trade_id):
            continue

        # Yritä hakea tietyn osakkeen dataa
        try:
            contract = Stock(symbol, 'SMART', 'USD')
            contract.primaryExchange = 'ARCA' 
            bars = ib.reqHistoricalData(contract, 
                                        endDateTime=end_date, 
                                        durationStr=durationStr,
                                        barSizeSetting=bar_size,  
                                        whatToShow="TRADES", 
                                        useRTH=0,
                                        formatDate=1)
        

        except Exception as e:
            print(f"Failed to fetch data for {symbol} on {end_date}. Error: {e}")
            continue  # Skip to the next row if an error occurs
            # Convert bars to DataFrame
        bars_df = pd.DataFrame(bars)

        data = handle_incoming_dataframe_intraday(bars_df,symbol,adjust_timezone_IB_data, calculate_vwap, calculate_ema,calculate_vwap_relative_atr, trade_id)

        insert_marketdataintrad_to_db(data, cursor, connection)

def handle_incoming_dataframe_intraday(bars_df, symbol,adjust_timezone_IB_data, calculate_vwap, calculate_ema, calculate_vwap_relative_atr,trade_id):
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
            atr_value = get_atr_value(trade_id, cursor)
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


            
# Main process
if __name__ == '__main__':


    # Read configuration from the JSON file
    config = read_project_config('config.json')
    # Connect to the database - Tää hakee kaikki kannassa olevat executionit jotka 10_Tradeperformance tai PSC alarms logic on sinne tunkenu
    connection = connect_db()
    cursor = connection.cursor()

    df_transactions = get_all_transactions(cursor) # tää hakee kaikki transactionit

    df_uniquepairs_data = get_uniquetickers_and_dates(df_transactions) # uniikit parit sieltä eli käytännössä ticker, date


# Kirjoittaa kantaan tradet execution taulun perusteella
    insert_trades_to_db(df_uniquepairs_data, cursor, connection)
    trades = get_all_trades(cursor)
    # Get the IB connection settings from the config file
    ib_host = config['ib_connection']['host']
    ib_port = config['ib_connection']['port']
    ib_client_id = config['ib_connection']['clientId']

    # Initialize IB connection
    ib = IB()
    ib.connect(ib_host, ib_port,ib_client_id)

    # trades mukana menee myös niiden id:t
    daily_data(df_data=trades, ib=ib, bar_size="1 day", durationStr='200 D', cursor = cursor,connection = connection)
    midterm_data(df_data=trades, ib=ib, bar_size="30 mins", durationStr='30 D', cursor = cursor,connection = connection)
    intraday_data(df_data=trades, ib=ib, bar_size="2 mins", durationStr='1 D', cursor = cursor,connection = connection)
