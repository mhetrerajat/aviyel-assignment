from typing import Optional, List, Dict

import pandas as pd

from core import analyze
from core.io import export, load_preprocessed_data


def get_metric(metric_name: str) -> pd.DataFrame:

    func_name = f"compute_{metric_name}"
    func = getattr(analyze, func_name)

    base_df = load_preprocessed_data(columns=["id", "snippet.tags"])
    return func(base_df)


def export_metric(
    metrics: List[str], file_name: Optional[str] = None
) -> Dict[str, str]:
    """Export metric data in xlsx format"""
    sheets = {}
    for metric_name in metrics:
        df = get_metric(metric_name)
        sheets[metric_name] = df

    export_file_name = file_name or metric_name
    return export(file_name=export_file_name, sheets=sheets)
