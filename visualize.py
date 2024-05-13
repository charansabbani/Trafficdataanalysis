import dash
from dash import dcc, html
import plotly.graph_objs as go
from dash.dependencies import Input, Output
import pandas as pd
import requests
from datetime import datetime, timedelta

# Initialize Dash app
app = dash.Dash(__name__)

# Define a function to fetch real-time data
def fetch_real_time_data():
    try:
        # Make a request to the real-time data source
        response = requests.get("https://api.example.com/traffic_data")
        response.raise_for_status()  # Raise an error if request fails
        real_time_data = pd.DataFrame(response.json())  # Convert JSON response to DataFrame
        return real_time_data
    except Exception as e:
        print(f"Error fetching real-time data: {e}")
        return None

# Define layout for the dashboard
app.layout = html.Div([
    html.H1("Real-Time Traffic Data Dashboard", style={'textAlign': 'center', 'color': '#3366cc'}),
    dcc.Graph(id='live-update-graph'),
    dcc.Interval(
        id='interval-component',
        interval=1*1000,  # in milliseconds
        n_intervals=0
    ),
    html.Div(id='live-update-text')
])

# Define callback to update graph with real-time data
@app.callback([Output('live-update-graph', 'figure'), Output('live-update-text', 'children')],
              [Input('interval-component', 'n_intervals')])
def update_graph(n):
    try:
        # Fetch real-time data
        real_time_data = fetch_real_time_data()
        if real_time_data is not None:
            # Plot real-time data
            trace = go.Scatter(
                x=real_time_data['timestamp'],
                y=real_time_data['traffic_status'],
                mode='lines+markers'
            )
            layout = go.Layout(title='Real-Time Traffic Data', xaxis=dict(title='Timestamp'), yaxis=dict(title='Traffic Status'))
            
            # Add dynamic annotations
            latest_timestamp = real_time_data['timestamp'].iloc[-1]
            latest_traffic_status = real_time_data['traffic_status'].iloc[-1]
            annotations = [
                {
                    'x': latest_timestamp,
                    'y': latest_traffic_status,
                    'xref': 'x',
                    'yref': 'y',
                    'text': f'Latest Traffic Status: {latest_traffic_status}',
                    'showarrow': True,
                    'arrowhead': 5,
                    'arrowwidth': 2,
                    'arrowcolor': '#ff7f0e',
                    'font': {'color': '#ff7f0e', 'size': 12},
                    'bordercolor': '#ff7f0e',
                    'borderwidth': 2,
                    'borderpad': 4,
                    'bgcolor': '#ffffff'
                }
            ]
            layout['annotations'] = annotations
            
            return {'data': [trace], 'layout': layout}, f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        else:
            return {'data': [], 'layout': go.Layout(title='Real-Time Traffic Data - Error',
                                                    annotations=[{'text': 'Failed to fetch data', 'showarrow': False}],
                                                    )}, "Last updated: Unknown"
    except Exception as e:
        return {'data': [], 'layout': go.Layout(title='Real-Time Traffic Data - Error',
                                                annotations=[{'text': f'Error: {str(e)}', 'showarrow': False}],
                                                )}, "Last updated: Unknown"

if __name__ == '__main__':
    app.run_server(debug=True)
