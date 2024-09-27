import ast
import json
import pandas as pd


def ensure_list(value):
    if pd.isna(value):
        return []
    if isinstance(value, str):
        try:
            return ast.literal_eval(value)
        except (ValueError, SyntaxError):
            return []
    return value

def convert_and_extract_platform_names(platforms):
    try:
        platforms_list = json.loads(platforms)
        return [p['platform']['name'] for p in platforms_list if 'platform' in p]
    except (json.JSONDecodeError, TypeError, KeyError):
        return []
