import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import numpy as np
import json

# Initialize the Dash app
app = dash.Dash(__name__)

# Layout of the app using CSS Grid
app.layout = html.Div(style={
    'display': 'grid',
    'gridTemplateColumns': '70% 30%',  # Left column is smaller than right column
    'gridTemplateRows': '1fr 1fr',  # Two equal rows
    'gap': '10px'
}, children=[
    dcc.Graph(id='live-update-graph-1'),  # Plot 1 in (1, 1)
    dcc.Graph(id='live-update-graph-2'),  # Plot 2 in (2, 1)
    dcc.Graph(id='live-update-graph-3', style={'gridColumn': '2', 'gridRow': '1 / span 2'})  # Plot 3 spans both rows in column 2
])

# Callback to update all graphs
@app.callback(
    Output('live-update-graph-1', 'figure'),
    Output('live-update-graph-2', 'figure'),
    Output('live-update-graph-3', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_graphs(n):
    try:
        # Read data from CSV file
        df = pd.read_csv('../kracked/L1_BBO.csv')


        with open("../kracked/L2_orderbook.json", 'r') as f:
            data_unp = json.load(f)
        data = {}
        for dk in data_unp['b'].keys():
            data[dk] = data_unp['b'][dk]
        for dk in data_unp['a'].keys():
            data[dk] = data_unp['a'][dk]
        myKeys = list(data.keys())
        myKeys.sort()
        # Sorted Dictionary
        data = {i: data[i] for i in myKeys}


        # Ensure the 'timestamp' column is present
        if 'timestamp' not in df.columns or 'bbo' not in df.columns or 'bao' not in df.columns:
            raise ValueError("CSV must contain 'timestamp' and 'value' columns.")

        # Convert the 'timestamp' column to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Create first figure (Plot 1)
        figure1 = go.Figure()
        figure1 = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.0,
                            subplot_titles=("BBO Data", ""))

        # Add traces for BBO
        figure1.add_trace(go.Scatter(x=df['timestamp'], y=df['bbo'], mode='lines+markers', name='BBO'),
                           row=1, col=1
        )

        # Add traces for BAO
        figure1.add_trace(go.Scatter(x=df['timestamp'], y=df['bao']-df['bbo'], mode='lines+markers', name='Spread'),
                           row=2, col=1
        )

        # Update layout
        figure1.update_layout(
            # title_text='Plot 1: Data Over Time',
            height=500,  # Adjust the height to accommodate two subplots
            xaxis=dict(showgrid=False),
            xaxis2=dict(showgrid=False),
            yaxis=dict(showgrid=False),
            yaxis2=dict(showgrid=False)
        )

        # Update x-axis properties
        figure1.update_xaxes(title_text='Time', row=2, col=1, linecolor='black', linewidth=1)
        figure1.update_xaxes(title_text='', row=1, col=1, linecolor='black', linewidth=1,)

        # Update y-axis properties
        figure1.update_yaxes(title_text='BBO', row=1, col=1, linecolor='black', linewidth=1)
        figure1.update_yaxes(title_text='Spread', row=2, col=1, linecolor='black', linewidth=1)


        # Create second figure (Plot 2)
        figure2 = go.Figure()
        figure2.add_trace(go.Bar(x=df['timestamp'],
                                 y=df['bao'],
                                 marker=dict(line=dict(color='black', width=3))))

        figure2.update_layout(title='Plot 2: Bar Graph',
                              xaxis_title='Time',
                              yaxis_title='Value')

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

        # Create the figure
        # Add horizontal bars for the top 10 (green)

        figure3.add_trace(go.Bar(
            y=top_indices,
            # x=filled_lengths[:10],
            orientation='h',
            marker=dict(color='green'),
            hoverinfo='text',
            text=[f'Price: {price}, Volume: {volume}' for price, volume in zip(stock_prices[:10], stock_volumes[:10])],
        ))

        # Add horizontal bars for the bottom 10 (red)
        figure3.add_trace(go.Bar(
            y=bottom_indices,
            x=filled_lengths[10:],
            orientation='h',
            marker=dict(color='red'),
            hoverinfo='text',
            text=[f'Price: {price}, Volume: {volume}' for price, volume in zip(stock_prices[10:], stock_volumes[10:])],
        ))

        # Update layout to label y-axis with stock prices
        figure3.update_layout(
            title='Stock Prices vs Volume',
            xaxis=dict(title='Volume (scaled)'),
            yaxis=dict(
                title='Stock Prices',
                tickvals=top_indices + bottom_indices,
                ticktext=stock_prices,
            ),
            showlegend=False,
            height=500,
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

