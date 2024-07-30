import logging

import polars as pl
from uteis import my_log

logger = logging.getLogger("polars_to_html")


class TableHtml:
    cols_name: list[str]
    table: list[list]

    def __init__(self, cols_name: list[str], table: list[list]) -> None:
        self.cols_name = cols_name
        self.table = table


def transform_df_in_tablehtml(df: pl.DataFrame) -> TableHtml:
    df_dict = df.to_dict(as_series=False)

    cols_name = list(df_dict.keys())

    cols_values = list(df_dict.values())

    len_row = len(cols_values[0])

    table = []
    for i in range(len_row):
        table.append([])
        for j in range(len(cols_values)):
            table[i].append(cols_values[j][i])

    return TableHtml(cols_name, table)
