import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import numpy as np
import json

# Updated Color Variables
BG_COL = '#1e1e2f'  # Dark Purple/Black
ACCENT_RED = '#e74c3c'
ACCENT_BLUE = '#3498db'
ACCENT_GREEN = '#2ecc71'  # Added ACCENT_GREEN for consistency
TEXT_COLOR = '#ecf0f1'
BORDER_COLOR = '#7f8c8d'  # Updated border color to a lighter gray
GRID_COLOR = '#3a3a4e'    # Defined gridline color variable
BORDER_WIDTH = 3           # Increased width for more prominent borders

# Initialize the Dash app
app = dash.Dash(__name__)

# Layout of the app using CSS Grid with updated styles for dark theme
app.layout = html.Div(style={
    'display': 'grid',
    'gridTemplateColumns': '50% 50%',  # Left column is smaller than right column
    'gridTemplateRows': '1fr 1fr',  # Two equal rows
    'gap': '100px',
    'backgroundColor': BG_COL,
    'color': TEXT_COLOR,
    'font-family': 'Verdana, sans-serif',  # Updated font for consistency
    'font-size': '16px',  # Increased font size
    'font-weight': 'bold',  # Made text bold
}, children=[
    dcc.Graph(
        id='live-update-graph-1',
        config={'displayModeBar': False},
        style={'backgroundColor': BG_COL}
    ),  # Plot 1 in (1, 1)
    dcc.Graph(
        id='live-update-graph-2',
        config={'displayModeBar': False},
        style={'backgroundColor': BG_COL}
    ),  # Plot 2 in (2, 1)
    dcc.Graph(
        id='live-update-graph-3',
        config={'displayModeBar': False},
        style={
            'gridColumn': '2',
            'gridRow': '1 / span 4',
            'height': '100%',
            'backgroundColor': BG_COL
        }
    )  # Plot 3 spans both rows in column 2 and full height
])

# Callback to update all graphs
@app.callback(
    Output('live-update-graph-1', 'figure'),
    Output('live-update-graph-2', 'figure'),
    Output('live-update-graph-3', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_graphs(n):

    # LOAD IN ALL DATA AND GET IN PLOTTABLE FORM
    base_dir = "../examples/ex_multifeed_out"
    symbol = "DOGE/USD"
    try:
        # Read data from CSV files
        l1df = pd.read_csv(f'{base_dir}/L1.csv')
        l1df = l1df[l1df['symbol'] == symbol]
        df_ohlc = pd.read_csv(f'{base_dir}/OHLC.csv')
        df_ohlc = df_ohlc[df_ohlc['symbol'] == symbol]
        df_ohlc = df_ohlc.groupby('timestamp').last().reset_index()


        df_ohlc['tend'] = pd.to_datetime(df_ohlc['timestamp'])

        with open(f'{base_dir}/L2_live_orderbooks.json', 'r') as f:
            data_unp = json.load(f)
            data_unp = data_unp[symbol]


        data = {}
        for dk in data_unp['bids'].keys():
            data[dk] = data_unp['bids'][dk]
        for dk in data_unp['asks'].keys():
            data[dk] = data_unp['asks'][dk]
        myKeys = list(data.keys())
        myKeys.sort()
        # Sorted Dictionary
        data = {i: data[i] for i in myKeys}

        # Ensure the 'timestamp', 'bid', and 'ask' columns are present
        required_columns = ['timestamp', 'bid', 'ask']
        if not all(col in l1df.columns for col in required_columns):
            raise ValueError(f"CSV must contain {', '.join(required_columns)} columns.")

        # Convert the 'timestamp' column to datetime
        l1df['timestamp'] = pd.to_datetime(l1df['timestamp'])

        # Create first figure (Plot 1)
        figure1 = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.05,
                                subplot_titles=("BBO Data", "Spread"))

        # Add traces for BBO
        figure1.add_trace(go.Scatter(
            x=l1df['timestamp'],
            y=l1df['bid'],
            mode='lines+markers',
            marker_color=ACCENT_BLUE,
            name='BBO'
        ), row=1, col=1)

        # Add traces for BAO
        figure1.add_trace(go.Scatter(
            x=l1df['timestamp'],
            y=l1df['ask'] - l1df['bid'],
            mode='lines+markers',
            marker_color=ACCENT_RED,
            name='Spread'
        ), row=2, col=1)

        # Update layout for figure1
        figure1.update_layout(
            height=500,  # Adjust the height to accommodate two subplots
            paper_bgcolor=BG_COL,
            plot_bgcolor=BG_COL,
            font=dict(color=TEXT_COLOR, family='Verdana, sans-serif'),  # Updated font
            showlegend=False,
            margin=dict(t=50, b=50, l=50, r=50),
            shapes=[dict(
                type="rect",
                xref="paper",
                yref="paper",
                x0=0, y0=0,
                x1=1, y1=1,
                line=dict(color=BORDER_COLOR, width=BORDER_WIDTH)
            )]
        )

        # Update x-axis properties
        figure1.update_xaxes(
            title_text='Time',
            row=2,
            col=1,
            linecolor=ACCENT_BLUE,
            color=TEXT_COLOR,
            gridcolor=GRID_COLOR  # Replaced with GRID_COLOR
        )
        figure1.update_xaxes(
            showgrid=False,  # Gridlines are hidden for the top plot
            row=1,
            col=1
        )

        # Update y-axis properties
        figure1.update_yaxes(
            title_text='BBO',
            row=1,
            col=1,
            linecolor=ACCENT_BLUE,
            color=TEXT_COLOR,
            gridcolor=GRID_COLOR  # Replaced with GRID_COLOR
        )
        figure1.update_yaxes(
            title_text='Spread',
            row=2,
            col=1,
            linecolor=ACCENT_RED,
            color=TEXT_COLOR,
            gridcolor=GRID_COLOR  # Replaced with GRID_COLOR
        )

        # Create second figure (Plot 2)
        figure2 = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.02, row_heights=[0.7, 0.3],
                                subplot_titles=("OHLC Candlestick", "Volume"))

        # Create OHLC candlestick trace with fixed colors
        candlestick = go.Candlestick(
            x=df_ohlc['tend'],
            open=df_ohlc['open'],
            high=df_ohlc['high'],
            low=df_ohlc['low'],
            close=df_ohlc['close'],
            name='OHLC',
            increasing=dict(line=dict(color=ACCENT_BLUE), fillcolor=ACCENT_BLUE),
            decreasing=dict(line=dict(color=ACCENT_RED), fillcolor=ACCENT_RED)
        )
        figure2.add_trace(candlestick, row=1, col=1)

        # Add volume bars
        colors = [ACCENT_BLUE if close >= open else ACCENT_RED for open, close in zip(df_ohlc['open'], df_ohlc['close'])]
        volume = go.Bar(
            x=df_ohlc['tend'],
            y=df_ohlc['volume'],
            name='Volume',
            marker_color=colors
        )
        figure2.add_trace(volume, row=2, col=1)

        # Update layout for figure2
        figure2.update_layout(
            height=500,
            paper_bgcolor=BG_COL,
            plot_bgcolor=BG_COL,
            font=dict(color=TEXT_COLOR, family='Verdana, sans-serif'),  # Updated font
            showlegend=False,
            xaxis_rangeslider_visible=False,
            margin=dict(t=50, b=50, l=50, r=50),
        )

        # Update shapes for figure2
        figure2.update_layout(
            shapes=[dict(
                type="rect",
                xref="paper",
                yref="paper",
                x0=0,
                y0=0,
                x1=1,
                y1=1,
                line=dict(color=BORDER_COLOR, width=BORDER_WIDTH)
            )]
        )

        # Update y-axes for figure2
        figure2.update_yaxes(
            title_text="Price",
            row=1,
            col=1,
            linecolor=ACCENT_BLUE,
            color=TEXT_COLOR,
            gridcolor=GRID_COLOR  # Replaced with GRID_COLOR
        )
        figure2.update_yaxes(
            title_text="Volume",
            row=2,
            col=1,
            linecolor=ACCENT_RED,
            color=TEXT_COLOR,
            gridcolor=GRID_COLOR  # Replaced with GRID_COLOR
        )

        # Update x-axis for figure2
        figure2.update_xaxes(
            title_text="Time",
            row=2,
            col=1,
            linecolor=ACCENT_BLUE,
            color=TEXT_COLOR,
            gridcolor=GRID_COLOR  # Replaced with GRID_COLOR
        )

        # Enable live updates
        figure2.update_layout(uirevision='constant')

        # Create third figure (Plot 3)
        figure3 = go.Figure()
        stock_prices = list(data.keys())
        stock_volumes = list(data.values())

        # Fixed length for bars
        fixed_length = 3.0

        # Calculate the lengths of each bar based on the stock volumes
        filled_lengths = np.array([(vol / max(stock_volumes)) * fixed_length for vol in stock_volumes])

        # Create indices for equally spacing the bars
        top_indices = list(range(10))
        bottom_indices = list(range(12, 22))  # Shifted to create space

        # Add horizontal bars for the top 10 (blue)
        figure3.add_trace(go.Bar(
            y=top_indices,
            x=np.log10(filled_lengths[:10] / np.min(filled_lengths[:10])),
            orientation='h',
            marker=dict(color=ACCENT_BLUE, line=dict(color='black', width=1.5)),
            hoverinfo='text',
            text=[f'V: {volume}' for volume in stock_volumes[:10]],
            name='Top 10'
        ))

        # Add horizontal bars for the bottom 10 (red)
        figure3.add_trace(go.Bar(
            y=bottom_indices,
            x=np.log10(filled_lengths[10:] / np.min(filled_lengths[10:])),
            orientation='h',
            marker=dict(color=ACCENT_RED, line=dict(color='black', width=1.5)),
            hoverinfo='text',
            text=[f'V: {volume}' for volume in stock_volumes[10:]],
            name='Bottom 10'
        ))

        # Update layout for figure3
        figure3.update_layout(
            height=1000,  # Increase the height to ensure it takes full vertical space
            title='L2 Order Book',
            paper_bgcolor=BG_COL,
            plot_bgcolor=BG_COL,
            font=dict(color=TEXT_COLOR, family='Verdana, sans-serif'),  # Updated font
            showlegend=False,
            margin=dict(t=100, b=50, l=100, r=50),
            shapes=[dict(
                type="rect",
                xref="paper",
                yref="paper",
                x0=0,
                y0=0,
                x1=1,
                y1=1,
                line=dict(color=BORDER_COLOR, width=BORDER_WIDTH)
            )],
            xaxis=dict(
                title='Volume (scaled)',
                linecolor=ACCENT_BLUE,
                color=TEXT_COLOR,
                gridcolor=GRID_COLOR  # Replaced with GRID_COLOR
            ),
            yaxis=dict(
                title='Stock Prices',
                tickvals=top_indices + bottom_indices,
                ticktext=stock_prices[:10] + stock_prices[10:],
                linecolor=ACCENT_BLUE,
                color=TEXT_COLOR,
                gridcolor=GRID_COLOR  # Replaced with GRID_COLOR
            )
        )

        return figure1, figure2, figure3

    except Exception as e:
        print(f"Error: {e}")
        return go.Figure(), go.Figure(), go.Figure()

# Interval component for live updates
app.layout.children.append(dcc.Interval(
    id='interval-component',
    interval=1000,  # in milliseconds
    n_intervals=0
))

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)

