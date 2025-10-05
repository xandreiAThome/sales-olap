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


def clean_phone_number(phone: str):
    phone = phone.strip()
    num = re.sub(r"x.*$", "", phone)  # Remove extension
    digits = re.sub(r"\D", "", num)  # Keep only digits
    if len(digits) == 11:
        digits = digits[1:]

    if len(digits) == 10:
        return f"{digits[0:3]}-{digits[3:6]}-{digits[6:10]}"
    return digits  # Return as is if not 10 digits/malformed
