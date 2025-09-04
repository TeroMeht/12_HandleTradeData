import psycopg2
import pandas as pd

# Return connection and cursor
def get_connection_and_cursor(database_config):
    """Create and return a database connection and cursor."""
    conn = psycopg2.connect(**database_config)
    if not conn:
        raise Exception("Failed to connect to database.")
    cur = conn.cursor()
    return conn, cur




def insert_trades_to_db(df, database_config):
    results = []  # Store outcome per trad
    conn, cur = get_connection_and_cursor(database_config)

    try:
        # Ensure the 'Date' column is datetime and format to string 'YYYY-MM-DD'
        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')

        # Extract unique (Symbol, Date) pairs
        values = df[["Symbol", "Date"]].drop_duplicates().values.tolist()

        if not values:
            print("No trades to insert.")
            return []

        insert_query = """
            INSERT INTO trades ("Symbol", "Date")
            VALUES (%s, %s)
            ON CONFLICT ("Symbol", "Date") DO NOTHING
            RETURNING "Symbol", "Date";
        """

        
        for symbol, date in values:
            try:
                cur.execute(insert_query, (symbol, date))
                returned = cur.fetchone()
                if returned:
                    results.append({"Symbol": symbol, "Date": date, "Status": "Inserted"})
                else:
                    results.append({"Symbol": symbol, "Date": date, "Status": "Duplicate - Skipped"})
            except Exception as e:
                results.append({"Symbol": symbol, "Date": date, "Status": f"Error: {e}"})
                conn.rollback()
                continue

        conn.commit()
        return results

    except Exception as e:
        print(f"Error inserting into trades table: {e}")
        if conn:
            conn.rollback()
        return [{"Error": str(e)}]

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def insert_executions_to_db(transactions_df, database_config):
    conn, cur = get_connection_and_cursor(database_config)

    insert_query = """
        INSERT INTO executions (
            "Symbol", "Date", "Time", "PermId", "AvgPrice", "Shares", 
            "Side", "Commission", "AdjustedAvgPrice"
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    inserted_info = []
    try:
      

        for _, row in transactions_df.iterrows():
            perm_id = str(row["TransactionID"])
            try:
                # Check if PermId already exists
                cur.execute('SELECT 1 FROM executions WHERE "PermId" = %s;', (perm_id,))
                exists = cur.fetchone()
                if exists:
                    print(f"PermId {perm_id} already exists in the database. Skipping insert.")
                    continue

                date_str = str(row["Date"])  # YYYY-MM-DD
                time_str = str(row["Time"])  # HH:MM:SS or full timestamp

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

                cur.execute(insert_query, values)

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
                print(f"Error inserting PermId {perm_id}: {e}")
                conn.rollback()
                continue

        conn.commit()

        print("\nInserted Executions:")
        for info in inserted_info:
            print(info)

    except Exception as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()
        raise e
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def insert_marketdata_to_db(data,database_config):

    conn, cur = get_connection_and_cursor(database_config)

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

        cur.executemany(insert_query, values)
        conn.commit()
        print(f"Inserting daily market data:  Symbol-Date: {data[['Symbol','Date']].drop_duplicates().iloc[0].to_dict()}")

    except Exception as e:
        print(f"Error inserting market data: {e}")
        conn.rollback()

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
    
def insert_marketdataintrad_to_db(data, database_config):

    conn, cur = get_connection_and_cursor(database_config)

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

        cur.executemany(insert_query, values)
        conn.commit()
        print(f"Inserting intraday market data: Symbol-Date: {data[['Symbol','Date']].drop_duplicates().iloc[0].to_dict()}")

    except Exception as e:
        print(f"Error inserting intraday market data: {e}")
        conn.rollback()

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def insert_marketdata30mins_to_db(data, database_config): 

    conn, cur = get_connection_and_cursor(database_config)

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

        cur.executemany(insert_query, values)
        conn.commit()
        print(f"Inserting 30mins market data: Symbol-Date: {data[['Symbol','Date']].drop_duplicates().iloc[0].to_dict()}")

    except Exception as e:
        print(f"Error inserting intraday market data: {e}")
        conn.rollback()

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()









def fetch_all_executions(database_config):
    
    conn, cur = get_connection_and_cursor(database_config)

    try:
        query = """
            SELECT 
                "Symbol", "Date","Time", "PermId", "AvgPrice", "Shares", 
                "Side", "Commission", "AdjustedAvgPrice"
            FROM executions
            ORDER BY "Time" ASC;
        """
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=columns)
        return df

    except Exception as e:
        print(f" Error fetching transactions: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error
    
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
    
def fetch_all_trades(database_config):

    conn, cur = get_connection_and_cursor(database_config)
    try:
        query = '''
            SELECT * 
            FROM trades 
            WHERE "Symbol" <> 'CNDX' 
            ORDER BY "TradeId";
        '''
        cur.execute(query)
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        return pd.DataFrame(rows, columns=colnames)
    except Exception as e:
        print(f"Error fetching trades: {e}")
        return pd.DataFrame()  # Return empty DataFrame on failure
    
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def fetch_individual_trade(database_config, table_name: str, trade_id: int) -> bool:
    conn, cur = get_connection_and_cursor(database_config)
    try:
        query = f'''
            SELECT "TradeId"
            FROM "{table_name}"
            WHERE "TradeId" = %s
            LIMIT 1;
        '''
        cur.execute(query, (trade_id,))
        row = cur.fetchone()
        return bool(row)
    except Exception as e:
        print(f" Error checking if TradeId {trade_id} exists in table {table_name}: {e}")
        return False
    
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def fetch_atr_value(trade_id, database_config):

    try:
        conn, cur = get_connection_and_cursor(database_config)

        query = """
        SELECT m."ATR"
        FROM trades t
        JOIN marketdatad m
          ON m."Date" = t."Date"::date
         AND m."Symbol" = t."Symbol"
        WHERE t."TradeId" = %s
        LIMIT 1;
        """
        cur.execute(query, (trade_id,))
        result = cur.fetchone()

        if result:
            return result[0]  # ATR value
        else:
            return None

    except Exception as e:
        print(f"Error fetching ATR for trade_id {trade_id}: {e}")
        return None

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def fetch_trades_by_symbol_and_date(symbol, date, database_config):

    conn, cur = get_connection_and_cursor(database_config)

    try:
        
        query = '''
            SELECT * 
            FROM trades 
            WHERE "Symbol" = %s 
              AND "Date" = %s
            ORDER BY "TradeId";
        '''
        cur.execute(query, (symbol, date))
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]

        return pd.DataFrame(rows, columns=colnames)

    except Exception as e:
        print(f"Error fetching trades for Symbol={symbol}, Date={date}: {e}")
        return pd.DataFrame()  # Return empty DataFrame on failure

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()



def check_if_tradeid_has_marketdata(my_trades, database_config):
    """
    Remove trades that already exist in any of the 3 tables (by TradeId).
    Returns a DataFrame with only new trades.
    """
    if my_trades.empty:
        return my_trades

    conn, cur = get_connection_and_cursor(database_config)

    try:
        trade_ids = tuple(my_trades["TradeId"].tolist())
        if len(trade_ids) == 1:  # psycopg2 requires (id,) not (id)
            trade_ids = (trade_ids[0],)

        query = '''
            SELECT "TradeId" 
            FROM marketdatad
            WHERE "TradeId" IN %s
            UNION
            SELECT "TradeId" 
            FROM marketdataintrad
            WHERE "TradeId" IN %s
            UNION
            SELECT "TradeId" 
            FROM marketdata30mins
            WHERE "TradeId" IN %s;
        '''

        cur.execute(query, (trade_ids, trade_ids, trade_ids))
        existing_ids = [row[0] for row in cur.fetchall()]

        # Keep only trades that are NOT already in DB
        new_trades = my_trades[~my_trades["TradeId"].isin(existing_ids)]

        return new_trades

    except Exception as e:
        print(f"Error filtering new trades: {e}")
        return my_trades  # fallback: return everything

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
