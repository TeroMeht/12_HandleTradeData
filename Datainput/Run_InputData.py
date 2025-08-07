import sys
import os
# Add the parent directory (main folder) to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from HandleTradeData import handle_tradedata
from db_connection import read_project_config


# Main process
if __name__ == '__main__':

    # Read configuration from the JSON file
    config = read_project_config(r'C:\Projects\12_HandleTradeData\config.json')

    # Pass data config
    df_data = handle_tradedata(config)
