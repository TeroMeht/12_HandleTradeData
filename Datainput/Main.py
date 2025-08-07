

from HandleTradeData import handle_tradedata
from db_config import read_project_config


# Main process
if __name__ == '__main__':

    # Read configuration from the JSON file
    config = read_project_config(r'C:\Projects\12_HandleTradeData\Datainput\config_dataread.json')

    # Pass data config
    df_data = handle_tradedata(config)
