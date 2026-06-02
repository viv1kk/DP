import pandas as pd

def load_excel_to_df(file_path, sheet_name=0):
    """
    Loads an Excel file and returns a pandas DataFrame.

    :param file_path: Path to the Excel file
    :param sheet_name: Sheet name or index (default = 0 -> first sheet)
    :return: pandas DataFrame
    """
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, engine="openpyxl")
        return df
    except FileNotFoundError:
        raise Exception(f"File not found: {file_path}")
    except ValueError as ve:
        raise Exception(f"Invalid sheet name or format issue: {ve}")
    except Exception as e:
        raise Exception(f"Error loading Excel file: {e}")