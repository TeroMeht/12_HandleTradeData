import pandas as pd
import glob


def read_tlg_file(data_in_folder):

    # Find the single .tlg file in the folder
    file_paths = glob.glob(f"{data_in_folder}/*.tlg")
    if not file_paths:
        print(f"\nNo .tlg file found in {data_in_folder}. Returning empty DataFrame.")
        return {}, pd.DataFrame(), None

    file_path = file_paths[0]  # Assumes exactly one .tlg file exists

    # Initialize containers
    account_info = {}
    transactions = []

    # State variable to track the section
    current_section = None

    # Read the file line by line
    with open(file_path, 'r', encoding='latin1') as file:
        for line in file:
            line = line.strip()

            if line.startswith("ACCOUNT_INFORMATION"):
                current_section = "ACCOUNT_INFORMATION"
                continue
            elif line.startswith("STOCK_TRANSACTIONS"):
                current_section = "STOCK_TRANSACTIONS"
                continue

            if current_section == "ACCOUNT_INFORMATION" and line.startswith("ACT_INF"):
                parts = line.split("|")
                account_info = {
                    "Account ID": parts[1],
                    "Name": parts[2],
                    "Type": parts[3],
                    "Address": parts[4]
                }
            elif current_section == "STOCK_TRANSACTIONS" and line.startswith("STK_TRD"):
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
