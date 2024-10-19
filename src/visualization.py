import plotly.express as px
from pyspark.sql import DataFrame
import kaleido
def save_visualization(spark_df: DataFrame, x_col: str, y_col: str, title: str, filename: str):
    """Save a visualization to a file using Spark DataFrame with Plotly."""
    # Collect data from Spark DataFrame
    data = spark_df.select(x_col, y_col).collect()
    x_values = [row[x_col] for row in data]
    y_values = [row[y_col] for row in data]

    # Create a bar plot using Plotly
    fig = px.bar(x=x_values, y=y_values, title=title, labels={x_col: x_col, y_col: y_col})
    fig.write_image(filename)

def save_visualization_multi_dim(spark_df: DataFrame, title: str, filename: str):
    """Save a multi-dimensional visualization to a file using Spark DataFrame with Plotly."""
    # Collect data from Spark DataFrame
    data = spark_df.collect()
    x_values = [row['VEH_BODY_STYL_ID'] for row in data]
    counts = [row['count'] for row in data]
    colors = [row['PRSN_ETHNICITY_ID'] for row in data]

    # Create a bar plot using Plotly
    fig = px.bar(x=x_values, y=counts, color=colors, title=title,
                 labels={'x': 'Vehicle Body Style', 'y': 'Counts'})
    fig.write_image(filename)
