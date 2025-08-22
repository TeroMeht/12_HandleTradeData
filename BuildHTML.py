from dash import dcc, html, Input, Output, State
import pandas as pd
from dash import dash_table

def build_header():
    return html.H2("Trade viewer", style={"textAlign": "left"})


def build_status_display():
    return html.Div([
        html.Div(id='relative-volume-display', style={"textAlign": "left", "fontSize": 20, "marginTop": "1px"}),
        html.Div(id='save-status-message', style={"color": "green", "marginTop": "1px"}),
    ])


def build_chart_row():
    return html.Div([
        html.Div([
            dcc.Graph(id='daily-chart', style={"height": "600px", "width": "100%"})
        ], style={"width": "50%", "display": "inline-block", "padding": "1px"}),

        html.Div([
            dcc.Graph(id='chart30mins', style={"height": "600px", "width": "100%"})
        ], style={"width": "50%", "display": "inline-block", "padding": "1px"}),
    ], style={"display": "flex", "justifyContent": "center"})


def build_trade_controls():
    return html.Div([
        html.Label("TradeId:", style={"marginRight": "1px"}),
        dcc.Input(id='trade-id-input', type='number', value=6175, debounce=True, style={"width": "100px"}),

        html.Button("Fetch Data", id='fetch-button', n_clicks=0, style={"marginLeft": "1px", "marginRight": "30px"}),

        html.Label("Select Setup:", style={"marginRight": "1px"}),
        dcc.Dropdown(
            id='setup-dropdown',
            options=[
                {'label': 'Extreme Reversal', 'value': 'Extreme Reversal'},
                {'label': 'No setup', 'value': 'No setup'},
                {'label': 'ORB', 'value': 'ORB'},
                {'label': 'Swing trade exit', 'value': 'Swing trade exit'},
                {'label': 'Reversal', 'value': 'Reversal'},
                {'label': 'Reversal short', 'value': 'Reversal short'},
                {'label': 'Parabolic short', 'value': 'Parabolic short'},
                {'label': 'Swing trade', 'value': 'Swing trade'},
                {'label': 'VWAP continuation', 'value': 'VWAP continuation'},
                {'label': 'Other', 'value': 'Other'}
            ],
            placeholder="Choose a setup",
            style={"width": "180px", "display": "inline-block", "verticalAlign": "middle"}
        ),

        html.Button("Save Setup", id='save-setup-button', n_clicks=0, style={"marginLeft": "1px"}),

        html.Label("Rating:", style={"marginLeft": "30px", "marginRight": "1px"}),
        dcc.Input(id='rating-input', type='number', min=1, max=5, step=1, style={"width": "60px"}),

        html.Button("Save Rating", id='save-rating-button', n_clicks=0, style={"marginLeft": "1px"}),
        
    ], style={"display": "flex", "alignItems": "center", "marginBottom": "1px", "gap": "1px"})



def build_trade_panel():
    return html.Div([
        build_trade_controls(),
        html.Div(id='trade-data-table', children=[
            # Placeholder DataTable to register its ID in initial layout
            dash_table.DataTable(
                id='trade-table',
                columns=[],
                data=[],
                style_table={"overflowX": "auto"}
            )
        ]),
    ], style={
        "width": "38%",
        "display": "inline-block",
        "padding": "5px",
        "height": "auto",
        "maxHeight": "none"
    })




def build_intraday_and_table_row():
    return html.Div([
        html.Div([
            dcc.Graph(id='intraday-chart', style={"height": "600px", "width": "100%"})
        ], style={"width": "60%", "display": "inline-block", "padding": "1px"}),

        build_trade_panel()
    ], style={"display": "flex", "justifyContent": "center"})

def build_error_display():
    return html.Div(id='error-message', style={
        "color": "red", "textAlign": "center", "marginTop": "5px"
    })