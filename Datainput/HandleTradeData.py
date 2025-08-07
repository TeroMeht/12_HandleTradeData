import glob
import pandas as pd
from AdjustTimezone import adjust_timezone_transactions
from db_connection import connect_db
import os
import shutil


def read_tlg_file(data_in_folder):
    """
    Reads the single .tlg file in the current folder and parses account information and transactions.
    Returns:
        dict: Account information
        pd.DataFrame: Transactions DataFrame
        str: TLG file path
    """
    # Find the single .tlg file in the current folder
    file_paths = glob.glob(f"{data_in_folder}/*.tlg")
    if not file_paths:
        raise FileNotFoundError("No .tlg file found in the current directory.")
    file_path = file_paths[0]  # Assumes exactly one .tlg file exists

    # Initialize containers
    account_info = {}
    transactions = []

    # State variables to track the section
    current_section = None

    # Read the file line by line
    with open(file_path, 'r', encoding='latin1') as file:
        for line in file:
            line = line.strip()  # Remove leading/trailing whitespace

            if line.startswith("ACCOUNT_INFORMATION"):
                current_section = "ACCOUNT_INFORMATION"
                continue
            elif line.startswith("STOCK_TRANSACTIONS"):
                current_section = "STOCK_TRANSACTIONS"
                continue

            if current_section == "ACCOUNT_INFORMATION" and line.startswith("ACT_INF"):
                # Parse account information
                parts = line.split("|")
                account_info = {
                    "Account ID": parts[1],
                    "Name": parts[2],
                    "Type": parts[3],
                    "Address": parts[4]
                }
            elif current_section == "STOCK_TRANSACTIONS" and line.startswith("STK_TRD"):
                # Parse stock transaction
                parts = line.split("|")
                transaction = {
                    "TransactionID": parts[1],
                    "Ticker": parts[2],
                    "CompanyName": parts[3],
                    "Venue": parts[4],
                    "Action": parts[5],
                    "OrderType": parts[6],
                    "Date": parts[7],
                    "Time": parts[8],
                    "Currency": parts[9],
                    "Quantity": float(parts[10]),
                    "Multiplier": float(parts[11]),
                    "Price": float(parts[12]),
                    "Amount": float(parts[13]),
                    "Fee": float(parts[14]),
                    "Extra": parts[15]
                }
                transactions.append(transaction)

    # Convert transactions to a DataFrame
    transactions_df = pd.DataFrame(transactions)

    return account_info, transactions_df, file_path

def handle_transactions(transactions_df, connection, cursor, file_path, config):
    """
    Adjusts transaction times, bulk inserts into the database.
    Moves the .tlg file to 'out' folder on success, 'error' folder on failure.
    """
    transactions_df['Time'] = transactions_df['Time'].apply(adjust_timezone_transactions)

    out_folder = config['folders']['out']
    error_folder = config['folders']['error']
    filename = os.path.basename(file_path)

    try:
        insert_all_transactions(cursor, transactions_df)
        connection.commit()
        # Move file to out folder
        shutil.move(file_path, os.path.join(out_folder, filename))
        print(f"Moved {filename} to {out_folder}")
    except Exception as e:
        connection.rollback()
        # Move file to error folder
        shutil.move(file_path, os.path.join(error_folder, filename))
        print(f"Bulk insert failed: {e}. Moved {filename} to {error_folder}")

def insert_all_transactions(cursor, transactions_df):
    insert_query = """
        INSERT INTO executions (
            "Symbol", "Date", "Time", "PermId", "AvgPrice", "Shares", 
            "Side", "Commission", "AdjustedAvgPrice"
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    inserted_info = []

    for _, row in transactions_df.iterrows():
        perm_id = str(row["TransactionID"])
        try:
            # Check if PermId already exists
            cursor.execute('SELECT 1 FROM executions WHERE "PermId" = %s;', (perm_id,))
            exists = cursor.fetchone()
            if exists:
                print(f"PermId {perm_id} already exists. Skipping insert.")
                continue

            date_str = str(row["Date"])  # Should be in YYYY-MM-DD format
            time_str = str(row["Time"])  # Should be in HH:MM:SS or full timestamp format

            values = (
                row["Ticker"],
                date_str,
                time_str,
                perm_id,
                float(row["Price"]),
                int(row["Quantity"]),
                row["Action"],
                float(row["Fee"]),
                float(row["Price"])  # Adjusted price fallback
            )
            cursor.execute(insert_query, values)
            inserted_info.append({
                "PermId": perm_id,
                "Ticker": row["Ticker"],
                "Date": date_str,
                "Time": time_str,
                "Quantity": row["Quantity"],
                "Price": row["Price"],
                "Action": row["Action"]
            })
        except Exception as e:
            # Raise the error to be handled by the calling function
            raise e

    print("Inserted Executions:")
    for info in inserted_info:
        print(info)


# Main function
def handle_tradedata(config):

    data_in_folder = config['folders']['in']

    try:
        # Read execution data
        account_info, transactions_df,file_path = read_tlg_file(data_in_folder)

        # Display the parsed data
        print("Account Information:")
        print(account_info)
        print("\nTransactions DataFrame:")
        print(transactions_df)

        # Connect to the database
        connection = connect_db()
        cursor = connection.cursor()

        # Handle transactions: write to DB
        handle_transactions(transactions_df, connection, cursor, file_path, config)

    except Exception as e:
        print(f" Error during data reading or database operation: {e}")
        if 'connection' in locals():
            connection.rollback()

    finally:
        try:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
            print(" Database connection closed.")
        except Exception as close_error:
            print(f" Error closing database connection: {close_error}")


