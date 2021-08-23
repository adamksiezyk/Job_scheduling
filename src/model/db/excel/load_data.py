import pandas as pd


def load_data(path: str, sheet: str) -> pd.DataFrame:
    return pd.read_excel(path, sheet_name=sheet)
