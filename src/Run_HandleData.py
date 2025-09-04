from ib_insync import *
import pandas as pd
import shutil
import os

from common.ReadConfigsIn import *
from common.AdjustTimezone import *
from helpers.DBfunctions import *
from helpers.HandleDataFrames import *
from helpers.ReadTlgFile import read_tlg_file  # from helpers folder



def handle_executions(transactions_df,  file_path, project_config,database_config):
    """
    Adjusts transaction times, bulk inserts into the database.
    Moves the .tlg file to 'out' folder on success, 'error' folder on failure.
    """
    transactions_df['Time'] = transactions_df['Time'].apply(adjust_timezone_transactions)

    out_folder = project_config['folders']['out']
    error_folder = project_config['folders']['error']
    filename = os.path.basename(file_path)

    try:
        insert_executions_to_db(transactions_df, database_config)
 
        # Move file to out folder
        shutil.move(file_path, os.path.join(out_folder, filename))
        print(f"Moved {filename} to {out_folder}")
    except Exception as e:

        # Move file to error folder
        shutil.move(file_path, os.path.join(error_folder, filename))
        print(f"Bulk insert failed: {e}. Moved {filename} to {error_folder}")



def get_uniquetickers_and_dates(df):
    try:
        # Ensure Date is a string in YYYYMMDD format
        df["Date"] = df["Date"].astype(str).str.replace("-", "")  # Remove dashes if present

        # Rename Ticker -> Symbol if the column exists
        if "Ticker" in df.columns:
            df = df.rename(columns={"Ticker": "Symbol"})

        # Drop duplicates and return only Symbol-Date pairs
        unique_pairs = df[["Symbol", "Date"]].drop_duplicates().reset_index(drop=True)

        return unique_pairs

    except Exception as e:
        print(f"Error parsing unique symbols and dates: {e}")
        return pd.DataFrame(columns=["Symbol", "Date"])








# IB data handling

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
                useRTH=0,
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
                useRTH=0,
                formatDate=1
            )

            bars_df = pd.DataFrame(bars)

            data =  handle_incoming_dataframe_midterm(bars_df, symbol,trade_id)
            insert_marketdata30mins_to_db(data,  database_config)

        except Exception as e:
            print(f" Failed to fetch data for {symbol} on {end_date}. Error: {e}")
            continue

# Intraday
def intraday_data(df_data,ib, bar_size, durationStr, database_config):

    for _, row in df_data.iterrows():
        symbol = row['Symbol']  # Assuming 'Ticker' column contains the stock symbol
        date = row['Date']
        # Remove hyphens from the date string to get e.g. '20240514'
        date_str = str(date).replace('-', '')
        end_date = f"{date_str} 16:59:59 US/Eastern"
        trade_id = row['TradeId']

        # Check if trade_id already exists
        if fetch_individual_trade(database_config, 'marketdataintrad', trade_id):
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

        data = handle_incoming_dataframe_intraday(bars_df,symbol,trade_id,database_config)

        insert_marketdataintrad_to_db(data, database_config)





def fetch_trades_by_pairs_loop(df_pairs, database_config):

    all_trades = []

    for _, row in df_pairs.iterrows():
        symbol = row["Symbol"]
        date = row["Date"]
        trades_df = fetch_trades_by_symbol_and_date(symbol, date, database_config)
        if not trades_df.empty:
            all_trades.append(trades_df)

    if all_trades:
        return pd.concat(all_trades, ignore_index=True)
    else:
        return pd.DataFrame()  # Return empty DataFrame if no trades found

def fetch_trade_data(my_trades, project_config, database_config):
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

        # Fetch data at different intervals
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








            
# Main process
if __name__ == '__main__':


    # Read configuration from the JSON file
    project_config = read_project_config(config_file='config.json')
    database_config = read_database_config(filename="database.ini", section="postgresql")


    # Read execution data
    account_info, executions_df,file_path = read_tlg_file(project_config['folders']['in'])


# If executions_df is empty, read from manual CSV
if not executions_df.empty:

    # Display the parsed data
    print("Account Information:")
    print(account_info)
    print("\nTransactions DataFrame:")
    print(executions_df)

    # Handle executions: write to DB
    handle_executions(executions_df, file_path, project_config,database_config)


    df_uniquepairs_data = get_uniquetickers_and_dates(executions_df) # uniikit parit sieltä eli käytännössä ticker, date
 

    # # Kirjoittaa kantaan tradet 
    trade_status = insert_trades_to_db(df_uniquepairs_data, database_config)
    print("\nTrade insert statuses:")
    for status in trade_status:
        print(f"Symbol={status['Symbol']}, Date={status['Date']}, Status={status['Status']}")

    # Hakee Tradeidt jotka luotiin kun kirjoitettiin kantaan
    my_trades = fetch_trades_by_pairs_loop(df_uniquepairs_data, database_config)

    # Step 2: filter them by checking DB
    new_trades = check_if_tradeid_has_marketdata(my_trades, database_config)

    # Step 3: print what will be fetched
    if not new_trades.empty:
        print("Starting fetch for the following trades:")
        for _, row in new_trades.iterrows():
            print(f"TradeId={row['TradeId']}, Symbol={row.get('Symbol', 'N/A')}, Date={row.get('Date', 'N/A')}")

        # Step 4: fetch trade data
        fetch_trade_data(new_trades, project_config, database_config)
    else:
        print("No new trades to fetch market data for.")



# Manul entry if no transactions found

else:
    print("No transactions found in the .tlg file. Please check the file or provide a manual CSV.")
    manual_file = project_config['folders']['manual']

     # Read manual CSV file
    trades_df = pd.read_csv(manual_file)

    print("\nManual data entries")
    print(trades_df)

    # Return trade status of trade inserts
    trade_status = insert_trades_to_db(trades_df, database_config)
      # Print status for each trade individually


    print("\nTrade insert statuses:")
    for status in trade_status:
        print(f"Symbol={status['Symbol']}, Date={status['Date']}, Status={status['Status']}")

    # Step 1: fetch trades (from your loop function)
    my_trades = fetch_trades_by_pairs_loop(trades_df, database_config)

    # Step 2: filter them by checking DB
    new_trades = check_if_tradeid_has_marketdata(my_trades, database_config)

    # Step 3: print what will be fetched
    if not new_trades.empty:
        print("\nStarting IB market data fetch for the following trades:")
        for _, row in new_trades.iterrows():
            print(f"TradeId={row['TradeId']}, Symbol={row.get('Symbol', 'N/A')}, Date={row.get('Date', 'N/A')}")
        print("\n")

        # Step 4: fetch trade data
        fetch_trade_data(new_trades, project_config, database_config)
    else:
        print("No new trades to fetch market data for.")

