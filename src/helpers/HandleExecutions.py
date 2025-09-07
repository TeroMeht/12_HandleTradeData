from common.AdjustTimezone import adjust_timezone_transactions
from helpers.DBfunctions import *  
import os
import shutil





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