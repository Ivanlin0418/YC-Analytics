import pandas as pd
from confluent_kafka import Producer

class Dataset:
    def __init__(self, name: str, df: pd.DataFrame):
        self._name = name
        self._df = df

    def get_df(self) -> pd.DataFrame:
        """Returns the DataFrame associated with the dataset."""
        return self._df

    def get_name(self) -> str:
        """Returns the name of the dataset."""
        return self._name

    def set_df(self, df: pd.DataFrame) -> None:
        """Sets a new DataFrame for the dataset."""
        self._df = df

    def set_name(self, name: str) -> None:
        """Sets a new name for the dataset."""
        self._name = name

    def get_summary(self) -> str:
        """Returns a summary of the dataset including its name and basic statistics."""
        summary = f"Dataset Name: {self._name}\n"
        summary += f"Number of Rows: {len(self._df)}\n"
        summary += f"Number of Columns: {len(self._df.columns)}\n"
        summary += f"Columns: {', '.join(self._df.columns)}\n"
        return summary

    def filter_rows(self, condition: str) -> pd.DataFrame:
        """Returns a DataFrame filtered by a given condition."""
        return self._df.query(condition)

    def add_column(self, column_name: str, data) -> None:
        """Adds a new column to the DataFrame."""
        self._df[column_name] = data
