# src/data_cleaning.py

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def clean_data(dataframes: dict) -> dict:
    for key, df in dataframes.items():
        # Drop columns with all null values
        df = df.dropna(how='all')

        # Dynamically fill NaN values based on column data types
        for column in df.columns:
            if df.schema[column].dataType.typeName() == 'string':  # For string columns
                df = df.fillna({column: ''})
            else:  # For numeric columns
                df = df.fillna({column: 0})

        # Update the dataframe in the dictionary
        dataframes[key] = df

    return dataframes
