def validate_columns(df, expected):

    missing = set(expected) - set(df.columns)

    if missing:
        raise ValueError(f"Missing columns: {missing}")

    return df[expected]