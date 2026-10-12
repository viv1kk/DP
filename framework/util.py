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

# Filler values the URL sheet uses for "no URL" (other columns use "NA" the same way).
URL_PLACEHOLDERS = {"", "NA", "N/A", "NAN", "NONE", "NULL", "-"}

def clean_url(value):
    """
    Returns the stripped URL, or None when there is no usable URL mapping.

    A missing URL must stay NULL rather than be polled: polling it fails and
    reports the system as down, which drives a false P1 in SAP Solman.
    """
    if value is None or pd.isna(value):
        return None
    url = str(value).strip()
    if url.upper() in URL_PLACEHOLDERS:
        return None
    if not url.lower().startswith(("http://", "https://")):
        return None
    return url
