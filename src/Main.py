from ib_insync import *
import pandas as pd

from common.ReadConfigsIn import *
from common.AdjustTimezone import *
from database.DBfunctions import *
from helpers.HandleDataFrames import *
from helpers.ReadTlgFile import read_tlg_file  # from helpers folder
from helpers.FetchIBdata import fetch_trade_data
from helpers.HandleExecutions import handle_executions





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








            
def process_trades(executions_df: pd.DataFrame, project_config: dict, database_config: dict):
    """
    Process trades from a DataFrame:
    - Insert trades into DB
    - Fetch new trades that require market data
    - Fetch market data for them
    """
    if executions_df.empty:
        print("No transactions found in the file. Please check the file or provide a manual CSV.")
        manual_file = project_config['folders']['manual']
        executions_df = pd.read_csv(manual_file)
        print("\nManual data entries:")
        print(executions_df)

    else:
        print("Account Information:")
        print(account_info)
        print("\nTransactions DataFrame:")
        print(executions_df)
        handle_executions(executions_df, file_path, project_config, database_config)

    # Step 1: Get unique tickers and dates
    df_uniquepairs_data = get_uniquetickers_and_dates(executions_df)

    # Step 2: Insert trades to DB
    trade_status = insert_trades_to_db(df_uniquepairs_data, database_config)
    print("\nTrade insert statuses:")
    for status in trade_status:
        print(f"Symbol={status['Symbol']}, Date={status['Date']}, Status={status['Status']}")

    # Step 3: Fetch trades from DB
    my_trades = fetch_trades_by_pairs_loop(df_uniquepairs_data, database_config)

    # Step 4: Filter trades that need market data
    new_trades = check_if_tradeid_has_marketdata(my_trades, database_config)

    # Step 5: Fetch market data if needed
    if not new_trades.empty:
        print("Starting fetch for the following trades:")
        for _, row in new_trades.iterrows():
            print(f"TradeId={row['TradeId']}, Symbol={row.get('Symbol', 'N/A')}, Date={row.get('Date', 'N/A')}")
        fetch_trade_data(new_trades, project_config, database_config)
    else:
        print("No new trades to fetch market data for.")




if __name__ == "__main__":
    # Load configs
    project_config = read_project_config(config_file='config.json')
    database_config = read_database_config(filename="database.ini", section="postgresql")

    # Read execution data
    account_info, executions_df, file_path = read_tlg_file(project_config['folders']['in'])

    # Process trades
    process_trades(executions_df, project_config, database_config)

