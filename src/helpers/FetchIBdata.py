
from ib_insync import IB, Stock
from helpers.HandleDataFrames import *
from helpers.DBfunctions import *
from common.ReadConfigsIn import *
from common.Calculate import *

# Daily
def daily_data(df_data, ib, bar_size, durationStr, database_config):


    for _, row in df_data.iterrows():
        symbol = row['Symbol']
        date = row['Date']
        # Remove hyphens from the date string to get e.g. '20240514'
        date_str = str(date).replace('-', '')
        end_date = f"{date_str} 16:59:59 US/Eastern"
        trade_id = row['TradeId']

        # Check if trade_id already exists
        if fetch_individual_trade(database_config, 'marketdatad', trade_id):
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
                useRTH=False,
                formatDate=1
            )
        except Exception as e:
            print(f"Failed to fetch data for {symbol} on {end_date}. Error: {e}")
            continue

        bars_df = pd.DataFrame(bars)

        # Call handler with specific TradeId row
        data = handle_incoming_dataframe_daily(bars_df, symbol, trade_id)

        insert_marketdata_to_db(data, database_config)

# 30mins
def midterm_data(df_data,ib, bar_size, durationStr, database_config):

    for _, row in df_data.iterrows():
        symbol = row['Symbol']
        date = row['Date']
        # Remove hyphens from the date string to get e.g. '20240514'
        date_str = str(date).replace('-', '')
        end_date = f"{date_str} 16:59:59 US/Eastern"
        trade_id = row['TradeId']

        # Check if trade_id already exists
        if fetch_individual_trade(database_config, 'marketdata30mins', trade_id):
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
                useRTH=False,
                formatDate=1
            )

            bars_df = pd.DataFrame(bars)

            data =  handle_incoming_dataframe_midterm(bars_df, symbol,trade_id)
            insert_marketdata30mins_to_db(data,  database_config)

        except Exception as e:
            print(f" Failed to fetch data for {symbol} on {end_date}. Error: {e}")
            continue

# Intraday
def intraday_data(df_data, ib, bar_size, durationStr, database_config):
    """Fetch intraday data and ATR separately for each trade."""
    all_data = []  # collect each trade's data if you want to return them

    for _, row in df_data.iterrows():
        symbol = row['Symbol']  # adjust to your actual column name
        date = row['Date']
        trade_id = row['TradeId']

        # skip if already in DB
        if fetch_individual_trade(database_config, 'marketdataintrad', trade_id):
            continue

        # fetch ATR for this trade only (single-row DataFrame)
        atr_df = atrdata(
            df_data=pd.DataFrame([row]),  # one trade
            ib=ib,
            bar_size="1 day",
            durationStr="14 D",
            database_config=database_config
        )

        # Build IB contract and end_date
        date_str = str(date).replace('-', '')
        end_date = f"{date_str} 16:59:59 US/Eastern"

        try:
            contract = Stock(symbol, 'SMART', 'USD')
            contract.primaryExchange = 'ARCA'

            bars = ib.reqHistoricalData(
                contract,
                endDateTime=end_date,
                durationStr=durationStr,
                barSizeSetting=bar_size,
                whatToShow="TRADES",
                useRTH=False,
                formatDate=1
            )

        except Exception as e:
            print(f"Failed to fetch data for {symbol} on {end_date}. Error: {e}")
            continue

        # Convert bars to DataFrame
        bars_df = pd.DataFrame(bars)
  
        # handle and calculate relATR
        data = handle_incoming_dataframe_intraday(bars_df, symbol, trade_id)
        print(data)
        print(atr_df)
        intraday_with_relatr = calculate_relatr(data, atr_df)
        print(intraday_with_relatr)

        # insert into DB
        insert_marketdataintrad_to_db(intraday_with_relatr, database_config)



# Fetching ATR data until previous day on trade
def atrdata(df_data, ib, bar_size, durationStr, database_config):
    """
    Fetch last 14 days of daily historical data from IB for each trade symbol.
    End date is set to the trade date (exclusive), so last bar is the day before.
    Returns a list of tuples: (symbol, trade_id, DataFrame with ATR).
    """


    for _, row in df_data.iterrows():
        symbol = row['Symbol']
        trade_id = row.get("TradeId", None)
        ref_date = row['Date']

        # End time = trade date at 00:00:00 (exclusive)
        end_dt_str = ref_date.strftime('%Y%m%d %H:%M:%S')

        contract = Stock(symbol, "SMART", "USD")

        try:
            bars = ib.reqHistoricalData(
                contract,
                endDateTime=end_dt_str,
                durationStr=durationStr,
                barSizeSetting=bar_size,
                whatToShow="TRADES",
                useRTH=True
            )

            if not bars:
                print(f"No data returned for {symbol}")
                continue

            # Convert bars to pandas DataFrame before handling
            bars_df = pd.DataFrame(bars)

            # Now call your handle_incoming_dataframe_daily
            df_processed = handle_incoming_dataframe_atr(bars_df, symbol, trade_id)
  

        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            continue

    return df_processed





def fetch_trade_data(my_trades, project_config,database_config):
    """
    Connects to IB and fetches daily, midterm, and intraday trade data.
    Handles connection errors gracefully.
    """
    ib = IB()
    try:
        ib.connect(
            project_config['ib_connection']['host'],
            project_config['ib_connection']['port'],
            project_config['ib_connection']['clientId']
        )
        if not ib.isConnected():
            print("Could not connect to IB. Skipping fetch.")
            return

        print("Connected to IB, starting data fetch...")

        # # # # Fetch data at different intervals
        daily_data(
            df_data=my_trades,
            ib=ib,
            bar_size="1 day",
            durationStr="200 D",
            database_config=database_config
        )
        midterm_data(
            df_data=my_trades,
            ib=ib,
            bar_size="30 mins",
            durationStr="30 D",
            database_config=database_config
        )

        intraday_data(
            df_data=my_trades,
            ib=ib,
            bar_size="2 mins",
            durationStr="1 D",
            database_config=database_config
        )
 




    except ConnectionRefusedError as e:
        print(f"Connection refused: {e}. Is TWS/Gateway running?")
    except Exception as e:
        print(f"Error while fetching trade data: {e}")
    finally:
        if ib.isConnected():
            ib.disconnect()
            print("Disconnected from IB")


    # Optional: disconnect IB after fetching
    ib.disconnect()



