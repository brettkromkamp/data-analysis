# Generate dataset for the project
# from sklearn.datasets import fetch_california_housing

# df = fetch_california_housing(as_frame=True).frame
# df.to_csv("california_housing.csv", index=False)

import pandas as pd
import plotly.express as px
from dash import Dash, Input, Output, callback, dash_table, dcc, html

# Load the dataset
df = pd.read_csv("california_housing.csv")

# Create the Dash app
app = Dash()
app.layout = [
    html.Div(children="Dashboard"),
    dash_table.DataTable(data=df.to_dict("records"), page_size=20),
    html.Div(
        [
            html.Label("Select a feature"),
            dcc.Dropdown(
                id="feature",
                options=[{"label": col, "value": col} for col in df.columns],
                value=df.columns[0],
            ),
        ]
    ),
    dcc.Graph(id="histogram"),
]


@app.callback(
    Output("histogram", "figure"),
    [Input("feature", "value")],
)
def update_histogram(feature):
    figure = px.histogram(df, x=feature)
    figure.update_layout(title=f"Histogram of {feature}", xaxis_title=feature, yaxis_title="Frequency")
    return figure


if __name__ == "__main__":
    app.run(debug=True)
