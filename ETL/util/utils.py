import pandas as pd
import re


def parse_date(x):
    x = str(x).strip()
    try:
        # Try ISO format first (YYYY-MM-DD)
        return pd.to_datetime(x, format="%Y-%m-%d", errors="raise")
    except Exception:
        try:
            # Try US format (MM/DD/YYYY)
            return pd.to_datetime(x, format="%m/%d/%Y", errors="raise")
        except Exception:
            return pd.NaT