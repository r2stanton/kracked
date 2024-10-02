import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.graph_objs as go

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
        df = pd.read_csv('data.csv')

        # Ensure the 'timestamp' column is present
        if 'timestamp' not in df.columns or 'value' not in df.columns:
            raise ValueError("CSV must contain 'timestamp' and 'value' columns.")

        # Convert the 'timestamp' column to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Create first figure (Plot 1)
        figure1 = go.Figure()
        figure1.add_trace(go.Scatter(x=df['timestamp'],
                                     y=df['value'],
                                     mode='lines+markers'), 
                          )

        figure1.update_layout(title='Plot 1: Data Over Time',
                              xaxis_title='Time',
                              yaxis_title='Value')

        # Create second figure (Plot 2)
        figure2 = go.Figure()
        figure2.add_trace(go.Bar(x=df['timestamp'],
                                 y=df['value'],
                                 marker=dict(line=dict(color='black', width=3))))

        figure2.update_layout(title='Plot 2: Bar Graph',
                              xaxis_title='Time',
                              yaxis_title='Value')

        # Create third figure (Plot 3)
        figure3 = go.Figure()
        figure3.add_trace(go.Scatter(x=df['value'],
                                     y=df['timestamp'],
                                     mode='markers'))

        figure3.update_layout(title='Plot 3: Value vs Time',
                              xaxis_title='Value',
                              yaxis_title='Time',
                              yaxis_autorange='reversed')

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

