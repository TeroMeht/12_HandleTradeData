import dash
from dash import dcc, html, Input, Output, State
import plotly.graph_objects as go
import pandas as pd
from db_connection import connect_db
from plotly.subplots import make_subplots
import numpy as np
from dash import dash_table
import BuildHTML as BHT
from dash import callback_context

# Create Dash app
app = dash.Dash(__name__)
app.title = "Candlestick Dashboard"

# --- Layout ---
app.layout = html.Div([
    BHT.build_header(),
    BHT.build_status_display(),
    BHT.build_chart_row(),
    BHT.build_intraday_and_table_row(),
    BHT.build_error_display()
])


# Data fetch codes

def fetch_marketdata(trade_id: int,cursor,table_name: str) -> pd.DataFrame:
    try:

        query = f'SELECT * FROM "{table_name}" WHERE "TradeId" = %s ORDER BY "Date";'
        cursor.execute(query, (trade_id,))
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=colnames)

        if df.empty:
            return df

        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df = df.dropna(subset=['Date'])

        for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        df = df.dropna(subset=['Open', 'High', 'Low', 'Close', 'Volume'])

        return df

    except Exception as e:
        print(f"Error fetching data for TradeId {trade_id}: {e}")
        return pd.DataFrame()

def fetch_trade_info(trade_id: int, cursor, table_name: str) -> dict:
    try:
        query = f'''
            SELECT "Symbol", "Date", "Setup", "Rating"
            FROM "{table_name}"
            WHERE "TradeId" = %s
            LIMIT 1;
        '''
        cursor.execute(query, (trade_id,))
        result = cursor.fetchone()

        if result:
            symbol, date, setup, rating = result
            return {
                "Symbol": symbol,
                "Date": pd.to_datetime(date),
                "Setup": setup,
                "Rating": rating
            }
        else:
            return {}

    except Exception as e:
        print(f"Error fetching trade info for TradeId {trade_id}: {e}")
        return {}

def fetch_intraday_data(trade_id: int, cursor, table_name: str) -> pd.DataFrame:
    try:
        query = f'SELECT * FROM "{table_name}" WHERE "TradeId" = %s ORDER BY "Time";'
        cursor.execute(query, (trade_id,))
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=colnames)

        if df.empty:
            return df

        df['Time'] = pd.to_datetime(df['Time'], errors='coerce')
        df = df.dropna(subset=['Time'])

        # List of columns to convert to numeric, including VWAP and EMA9
        numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume', 'VWAP', 'EMA9','Relatr']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')


        return df

    except Exception as e:
        print(f"Error fetching intraday data for TradeId {trade_id}: {e}")
        return pd.DataFrame()

def fetch_relative_volume(trade_id: int, cursor) -> pd.DataFrame:
    """
    Given a trade_id, fetch the Symbol and Date from trades table,
    then query marketdatad for RelativeVolume where Symbol and Date match.
    Returns a DataFrame with the relevant data.
    """
    try:
        # Step 1: Get Symbol and Date from trades
        query_trade = 'SELECT "Symbol", "Date" FROM "trades" WHERE "TradeId" = %s LIMIT 1;'
        cursor.execute(query_trade, (trade_id,))
        result = cursor.fetchone()
        
        if not result:
            return pd.DataFrame()  # No trade found
        
        symbol, date = result
        
        # Step 2: Query marketdatad using Symbol and Date for RelativeVolume
        query_marketdatad = '''
            SELECT "Date", "RelativeVolume" 
            FROM "marketdatad" 
            WHERE "Symbol" = %s AND "Date" = %s
            ORDER BY "Date"
            LIMIT 1;
        '''
        cursor.execute(query_marketdatad, (symbol, date))
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        
        df = pd.DataFrame(rows, columns=colnames)
        
        # Convert columns to proper types
        if not df.empty:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            df['RelativeVolume'] = pd.to_numeric(df['RelativeVolume'], errors='coerce')
            df = df.dropna(subset=['Date', 'RelativeVolume'])
        
        return df
    
    except Exception as e:
        print(f"Error fetching RelativeVolume for TradeId {trade_id}: {e}")
        return pd.DataFrame()

def fetch_marketdata30mins(trade_id: int, cursor,table_name:str) -> pd.DataFrame:
    try:
        query = f'''
            SELECT "Symbol", "Date", "Open", "High", "Low", "Close", "Volume", "EMA65", "TradeId"
            FROM "{table_name}"
            WHERE "TradeId" = %s
            ORDER BY "Date";
        '''
        cursor.execute(query, (trade_id,))
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=colnames)

        if df.empty:
            return df

        # Parse timestamp
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df = df.dropna(subset=['Date'])

        # Convert price and volume columns
        for col in ['Open', 'High', 'Low', 'Close', 'Volume','EMA65']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        return df

    except Exception as e:
        print(f"Error fetching 30-min data for TradeId {trade_id}: {e}")
        return pd.DataFrame()

def fetch_trade_executions(trade_info: dict, cursor, table_name: str) -> pd.DataFrame:

    try:
        symbol = trade_info.get("Symbol")
        trade_date = trade_info.get("Date")

        if not symbol or not trade_date:
            print(" Missing Symbol or Date in trade_info.")
            return pd.DataFrame()

        # Convert date to string in YYYYMMDD format (if it's a datetime object)
        if isinstance(trade_date, pd.Timestamp):
            trade_date_str = trade_date.strftime("%Y%m%d")
        else:
            trade_date_str = str(trade_date)

        query = f"""
            SELECT * FROM {table_name}
            WHERE "Symbol" = %s AND "Date" = %s
            ORDER BY "Time" ASC
        """

        cursor.execute(query, (symbol, trade_date_str))
        rows = cursor.fetchall()

        # Get column names
        colnames = [desc[0] for desc in cursor.description]

        return pd.DataFrame(rows, columns=colnames)

    except Exception as e:
        print(f" Error fetching trade executions: {e}")
        return pd.DataFrame()

def fetch_trades_with_rvol(cursor, trades_table: str, marketdatad_table: str) -> pd.DataFrame:
    query = f"""
        SELECT
            t.*,
            m."RelativeVolume",
            m."Date"
        FROM {trades_table} t
        LEFT JOIN LATERAL (
            SELECT "RelativeVolume", "Date"
            FROM {marketdatad_table} m
            WHERE m."TradeId" = t."TradeId"
            ORDER BY m."Date" DESC
            LIMIT 1
        ) m ON TRUE
        WHERE m."RelativeVolume" > 0
        ORDER BY m."Date" DESC;
    """
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    return pd.DataFrame(data, columns=columns)


# Write database

def update_trade_setup(trade_id, setup, connection):
    try:
        cursor = connection.cursor()
        query = """
            UPDATE trades
            SET "Setup" = %s
            WHERE "TradeId" = %s;
        """
        cursor.execute(query, (setup, trade_id))
        connection.commit()
        cursor.close()  # Close the cursor after execution
    except Exception as e:
        print(f"Error updating Setup for TradeId {trade_id}: {e}")
        connection.rollback()

# Match execution data to 2 min chart
def align_execution_times_to_intraday(df_executions: pd.DataFrame, df_intraday: pd.DataFrame) -> pd.DataFrame:

    if df_executions.empty or df_intraday.empty:
        return df_executions.copy()

    # Helper function to convert time or datetime to seconds from midnight
    def to_seconds(t):
        if pd.isnull(t):
            return np.nan
        if isinstance(t, pd.Timestamp) or isinstance(t, pd._libs.tslibs.timestamps.Timestamp):
            t = t.time()
        return t.hour * 3600 + t.minute * 60 + t.second

    # Convert intraday and exec times to seconds from midnight as numpy arrays
    intraday_secs = np.array([to_seconds(t) for t in df_intraday['Time']])
    exec_secs = np.array([to_seconds(t) for t in df_executions['Time']])

    def find_nearest_time(exec_sec):
        # Find index of closest intraday time
        idx = np.abs(intraday_secs - exec_sec).argmin()
        return df_intraday['Time'].iloc[idx]

    aligned_times = [find_nearest_time(t) for t in exec_secs]

    df_aligned = df_executions.copy()
    df_aligned['Time'] = aligned_times

    return df_aligned

def align_execution_times_to_30mins(df_executions: pd.DataFrame, df_30min: pd.DataFrame) -> pd.DataFrame:
    if df_executions.empty or df_30min.empty:
        return df_executions.copy()

    # Convert to seconds since midnight
    def to_seconds(t):
        if pd.isnull(t):
            return np.nan
        if isinstance(t, pd.Timestamp):
            t = t.time()
        return t.hour * 3600 + t.minute * 60 + t.second

    intraday_secs = np.array([to_seconds(t) for t in df_30min['Date']])
    exec_secs = np.array([to_seconds(t) for t in df_executions['Time']])

    aligned_times = []
    for exec_sec in exec_secs:
        idx = np.abs(intraday_secs - exec_sec).argmin()
        aligned_time = df_30min['Date'].iloc[idx].time()  # Only use the time
        aligned_times.append(aligned_time)

    df_aligned = df_executions.copy()
    df_aligned['Time'] = aligned_times  # Replace only Time
    # Date remains untouched

    return df_aligned


# Graph plottinf codes
def plot_daily_chart(df_daily, symbol, trade_id, date_str, df_executions):
    fig_daily = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.02,
        row_heights=[0.7, 0.2],
        subplot_titles=[f'Daily']
    )

    if not df_daily.empty:
        # Create a date-only string column for x-axis
        df_daily = df_daily.copy()
        df_daily['DateStr'] = df_daily['Date'].dt.strftime('%Y-%m-%d')

        fig_daily.add_trace(go.Candlestick(
            x=df_daily['DateStr'],
            open=df_daily['Open'],
            high=df_daily['High'],
            low=df_daily['Low'],
            close=df_daily['Close'],
            name='OHLC Daily'
        ), row=1, col=1)

        # ➕ Add execution markers (Daily)
        if df_executions is not None and not df_executions.empty:
            df_executions = df_executions.copy()
            # Convert execution dates to string dates too
            df_executions['DateStr'] = pd.to_datetime(df_executions['Date']).dt.strftime('%Y-%m-%d')

            color_map = {'BUYTOOPEN': 'blue', 'BUYTOCLOSE': 'blue', 'SELLTOCLOSE': 'red', 'SELLTOOPEN': 'red'}
            symbol_map = {'BUYTOOPEN': 'triangle-up', 'BUYTOCLOSE': 'triangle-up', 'SELLTOCLOSE': 'triangle-down', 'SELLTOOPEN': 'triangle-down'}
            colors = df_executions['Side'].map(color_map).fillna('black')
            symbols = df_executions['Side'].map(symbol_map).fillna('circle')
            price_col = 'AvgPrice' if 'AvgPrice' in df_executions.columns else 'Price'

            fig_daily.add_trace(go.Scatter(
                x=df_executions['DateStr'],
                y=df_executions[price_col],
                mode='markers',
                marker=dict(color=colors, size=12, symbol=symbols),
                name='Executions'
            ), row=1, col=1)

        fig_daily.add_trace(go.Bar(
            x=df_daily['DateStr'],
            y=df_daily['Volume'],
            marker_color='blue',
            name='Volume Daily'
        ), row=2, col=1)

        fig_daily.update_layout(
            height=600,
            showlegend=False,
            xaxis=dict(
                type='category',
                tickangle=45,
                tickfont=dict(size=10),
            ),
            xaxis2=dict(
                type='category',
                tickangle=45,
                tickfont=dict(size=10),
            ),
            yaxis=dict(title='Price'),
            yaxis2=dict(title='Volume'),
            xaxis_rangeslider_visible=False,
        )
    else:
        fig_daily = go.Figure()
        fig_daily.update_layout(title="No daily data available")

    return fig_daily

def plot_30min_chart(df_30min, symbol, trade_id, df_executions):
    if not df_30min.empty:
        df_30min = df_30min.copy()
        # Format datetime as date+time string without seconds (for cleaner labels)
        df_30min['DateStr'] = df_30min['Date'].dt.strftime('%Y-%m-%d %H:%M')

        fig_30min = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.02,
            row_heights=[0.7, 0.2],
            subplot_titles=[f'30-Minutes']
        )

        fig_30min.add_trace(go.Candlestick(
            x=df_30min['DateStr'],
            open=df_30min['Open'],
            high=df_30min['High'],
            low=df_30min['Low'],
            close=df_30min['Close'],
            name='OHLC 30min'
        ), row=1, col=1)

        if 'EMA65' in df_30min.columns:
            fig_30min.add_trace(go.Scatter(
                x=df_30min['DateStr'],
                y=df_30min['EMA65'],
                mode='lines',
                line=dict(color='blue', width=1.5),
                name='EMA65'
            ), row=1, col=1)

        # ➕ Execution markers (30min)
        if df_executions is not None and not df_executions.empty:
            df_executions = df_executions.copy()
            # Combine Date and Time as string (same format as df_30min DateStr)
            df_executions['DateTimeStr'] = df_executions.apply(
                lambda row: f"{row['Date'].strftime('%Y-%m-%d')} {row['Time'].strftime('%H:%M')}", axis=1
            )

            color_map = {'BUYTOOPEN': 'blue', 'BUYTOCLOSE': 'blue', 'SELLTOCLOSE': 'red', 'SELLTOOPEN': 'red'}
            symbol_map = {'BUYTOOPEN': 'triangle-up', 'BUYTOCLOSE': 'triangle-up', 'SELLTOCLOSE': 'triangle-down', 'SELLTOOPEN': 'triangle-down'}
            colors = df_executions['Side'].map(color_map).fillna('black')
            symbols = df_executions['Side'].map(symbol_map).fillna('circle')
            price_col = 'AvgPrice' if 'AvgPrice' in df_executions.columns else 'Price'

            fig_30min.add_trace(go.Scatter(
                x=df_executions['DateTimeStr'],
                y=df_executions[price_col],
                mode='markers',
                marker=dict(color=colors, size=12, symbol=symbols),
                name='Executions'
            ), row=1, col=1)

        fig_30min.add_trace(go.Bar(
            x=df_30min['DateStr'],
            y=df_30min['Volume'],
            marker_color='blue',
            name='Volume 30min'
        ), row=2, col=1)

        fig_30min.update_layout(
            height=600,
            showlegend=False,
            xaxis=dict(
                type='category',
                tickangle=45,
                tickfont=dict(size=15),
                showticklabels=False
            ),
            xaxis2=dict(
                type='category',
                tickangle=45,
                tickfont=dict(size=15),
                showticklabels=False
            ),
            yaxis=dict(title='Price'),
            yaxis2=dict(title='Volume'),
            xaxis_rangeslider_visible=False,
        )
    else:
        fig_30min = go.Figure()
        fig_30min.update_layout(title="No 30-minute data available")

    return fig_30min

def plot_intraday_chart(df_intraday, df_executions, symbol, trade_id, date_str):
    if not df_intraday.empty:

        fig_intraday = make_subplots(
            rows=3, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.02,
            row_heights=[0.6, 0.2, 0.2],
            subplot_titles=[f'Intraday Price & Indicators']
        )

        # Candlestick
        fig_intraday.add_trace(go.Candlestick(
            x=df_intraday['Time'],
            open=df_intraday['Open'],
            high=df_intraday['High'],
            low=df_intraday['Low'],
            close=df_intraday['Close'],
            name='OHLC'
        ), row=1, col=1)

        # VWAP
        if 'VWAP' in df_intraday.columns:
            fig_intraday.add_trace(go.Scatter(
                x=df_intraday['Time'],
                y=df_intraday['VWAP'],
                mode='lines',
                line=dict(color='red', width=2),
                name='VWAP'
            ), row=1, col=1)

        # EMA9
        if 'EMA9' in df_intraday.columns:
            fig_intraday.add_trace(go.Scatter(
                x=df_intraday['Time'],
                y=df_intraday['EMA9'],
                mode='lines',
                line=dict(color='purple', width=1),
                name='EMA9'
            ), row=1, col=1)

        # Execution markers
        if df_executions is not None and not df_executions.empty:
            color_map = {
                'BUYTOOPEN': 'blue',
                'BUYTOCLOSE': 'blue',
                'SELLTOCLOSE': 'red',
                'SELLTOOPEN': 'red',
            }
            symbol_map = {
                'BUYTOOPEN': 'triangle-up',
                'BUYTOCLOSE': 'triangle-up',
                'SELLTOCLOSE': 'triangle-down',
                'SELLTOOPEN': 'triangle-down',
            }
            colors = df_executions['Side'].map(color_map).fillna('black')
            symbols = df_executions['Side'].map(symbol_map).fillna('circle')
            price_col = 'AvgPrice' if 'AvgPrice' in df_executions.columns else 'Price'

            fig_intraday.add_trace(go.Scatter(
                x=df_executions['Time'],
                y=df_executions[price_col],
                mode='markers',
                marker=dict(color=colors, size=15, symbol=symbols),
                name='Executions'
            ), row=1, col=1)

        # Volume
        fig_intraday.add_trace(go.Bar(
            x=df_intraday['Time'],
            y=df_intraday['Volume'],
            marker_color='blue',
            name='Volume'
        ), row=2, col=1)

        # Relatr
        if 'Relatr' in df_intraday.columns:
            fig_intraday.add_trace(go.Scatter(
                x=df_intraday['Time'],
                y=df_intraday['Relatr'],
                mode='lines',
                line=dict(color='green', width=2),
                name='Relatr'
            ), row=3, col=1)

            # Add horizontal lines
            for y_val in [0, 0.4, -0.4]:
                fig_intraday.add_shape(
                    type='line',
                    x0=df_intraday['Time'].min(),
                    x1=df_intraday['Time'].max(),
                    y0=y_val,
                    y1=y_val,
                    line=dict(color='black', width=1, dash='dash'),
                    xref='x3', yref='y3',
                )

        # Layout
        fig_intraday.update_layout(
            height=800,
            showlegend=False,
            xaxis3=dict(title='Time'),
            yaxis=dict(title='Price'),
            yaxis2=dict(title='Volume'),
            yaxis3=dict(title='Relatr'),
            xaxis_rangeslider_visible=False,
        )
    else:
        fig_intraday = go.Figure()
        fig_intraday.update_layout(title="No intraday data available")

    return fig_intraday



# Trades table
def generate_table(dataframe, max_rows=15):
    return dash_table.DataTable(
        id='trade-table',
        columns=[{"name": i, "id": i} for i in dataframe.columns],
        data=dataframe.to_dict('records'),
        sort_action='native',
        page_size=max_rows,
        page_action='native',
        fixed_rows={'headers': True},
        
        # Retain pagination, sort, and filter states across sessions
        persistence=True,  
        persistence_type='session',  # or 'local' if you want it persistent across browser restarts
        persisted_props=['page_current', 'sort_by', 'filter_query'],  
        
        style_table={
            'border': '1px solid #e1e1e1',
            'backgroundColor': 'white',
        },
        style_cell={
            'textAlign': 'center',
            'padding': '2px',
            'backgroundColor': 'white',
            'color': '#333',
            'border': '1px solid #f0f0f0',
            'fontFamily': 'Arial, sans-serif',
            'fontSize': '12px',
        },
        style_header={
            'backgroundColor': '#f8f8f8',
            'color': '#333',
            'fontWeight': 'bold',
            'border': '1px solid #ddd',
            'fontSize': '12px',
        },
        style_data_conditional=[
            {
                'if': {'row_index': 'odd'},
                'backgroundColor': '#f9f9f9',
            },
            {
                'if': {'state': 'selected'},
                'backgroundColor': '#d0e6f7',
                'border': '1px solid #a6c8ff',
            },
            {
                'if': {'state': 'active'},
                'backgroundColor': '#cce4ff',
                'border': '1px solid #8bbfff',
            }
        ],
        style_as_list_view=True,
    )


def update_trade_setup(trade_id: int, setup: str, conn):
    with conn.cursor() as cursor:
        cursor.execute(
            'UPDATE trades SET "Setup" = %s WHERE "TradeId" = %s;',
            (setup, trade_id)
        )
        conn.commit()

def update_trade_rating(trade_id: int, rating: int, conn):
    with conn.cursor() as cursor:
        cursor.execute(
            'UPDATE trades SET "Rating" = %s WHERE "TradeId" = %s;',
            (rating, trade_id)
        )
        conn.commit()


@app.callback(
    Output('trade-id-input', 'value'),
    Input('trade-table', 'active_cell'),
    State('trade-table', 'derived_viewport_data'),
    prevent_initial_call=True
)
def update_trade_id_input(active_cell, visible_data):
    if active_cell and visible_data:
        row_index = active_cell['row']
        column_id = active_cell['column_id']

        if column_id == 'TradeId' and 0 <= row_index < len(visible_data):
            trade_id = visible_data[row_index]['TradeId']
           # print(f"Selected TradeId: {trade_id}")
            return trade_id

    return dash.no_update


# Callback function (correct signature)
@app.callback(
    Output('daily-chart', 'figure'),
    Output('chart30mins', 'figure'),           # Added output for 30min chart
    Output('intraday-chart', 'figure'),
    Output('relative-volume-display', 'children'),
    Output('error-message', 'children'),
    Output('trade-data-table', 'children'),
    Input('fetch-button', 'n_clicks'),
    State('trade-id-input', 'value'),
    State('setup-dropdown', 'value'),
    prevent_initial_call=True
)
def update_chart(n_clicks, trade_id,setup):
    if n_clicks == 0:
        raise dash.exceptions.PreventUpdate
    if not trade_id:
        return go.Figure(), go.Figure(), go.Figure(), "", "Please enter a valid Trade ID."

    connection = connect_db()
    cursor = connection.cursor()
    try:
        # Fetch price data
        df_daily = fetch_marketdata(trade_id, cursor, table_name="marketdatad")
        df_intraday = fetch_marketdata(trade_id, cursor, table_name="marketdataintrad")
        df_30min = fetch_marketdata30mins(trade_id, cursor, table_name="marketdata30mins")

        # Fetch trade details
        trade_info = fetch_trade_info(trade_id, cursor, table_name="trades")
        df_relative_volume = fetch_relative_volume(trade_id, cursor)
       

        trade_executions = fetch_trade_executions(trade_info, cursor, table_name="executions")
        

        df_rvol_trades = fetch_trades_with_rvol(cursor, "trades", "marketdatad")
        trade_table = generate_table(df_rvol_trades)

    finally:
        cursor.close()
        connection.close()

    if df_daily.empty and df_intraday.empty:
        return go.Figure(), go.Figure(), f"No data found for TradeId {trade_id}"

    symbol = trade_info.get("Symbol", "Unknown Symbol")
    trade_date = trade_info.get("Date")
    date_str = trade_date.strftime("%Y-%m-%d") if trade_date else "Unknown Date"
    setup = trade_info.get("Setup", "No Setup")

    relative_volume = df_relative_volume['RelativeVolume'].iloc[0]
    trade_details_text = (
        f"TradeId: {trade_id} | Symbol: {symbol} | Date: {date_str} | Setup: {setup} | Relative Volume: {relative_volume:.2f}"
    )
  #  print(trade_details_text)


    # Align timestamps
    df_executions_aligned = align_execution_times_to_intraday(trade_executions, df_intraday)
    df_executions_aligned30min = align_execution_times_to_30mins(trade_executions, df_30min)



    fig_intraday = plot_intraday_chart(df_intraday,df_executions_aligned, symbol, trade_id, date_str)
    fig_daily = plot_daily_chart(df_daily, symbol, trade_id, date_str,trade_executions)
    fig_30min = plot_30min_chart(df_30min, symbol, trade_id, df_executions_aligned30min)




    return fig_daily, fig_30min, fig_intraday, trade_details_text, "",trade_table



@app.callback(
    Output('save-status-message', 'children'),
    Input('save-setup-button', 'n_clicks'),
    Input('save-rating-button', 'n_clicks'),
    State('trade-id-input', 'value'), 
    State('setup-dropdown', 'value'),
    State('rating-input', 'value'),
    prevent_initial_call=True
)
def save_trade_info(setup_clicks, rating_clicks, trade_id, setup, rating):
    if not trade_id:
        raise dash.exceptions.PreventUpdate

    ctx = callback_context
    triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]
    messages = []

    try:
        conn = connect_db()

        if triggered_id == 'save-setup-button':
            if setup:
                update_trade_setup(trade_id, setup, conn)
                messages.append(f"Setup: '{setup}'")
            else:
                messages.append("⚠️ No setup selected.")

        elif triggered_id == 'save-rating-button':
            if rating is not None:
                update_trade_rating(trade_id, rating, conn)
                messages.append(f"Rating: '{rating}'")
            else:
                messages.append("⚠️ No rating provided.")

        conn.close()

        return f"✅ Saved for Trade ID {trade_id}: " + " ".join(messages)

    except Exception as e:
        return f"❌ Error: {str(e)}"






if __name__ == '__main__':
    app.run(debug=True)
