from logging import getLogger

import polars as pl

from logs import my_log

logger = getLogger("my_transform")


@my_log.debug_log(logger)
def expr_cast_column(column: str, dtype: str, alias: str) -> pl.Expr:
    return pl.col(column).cast(dtype).alias(alias)


@my_log.debug_log(logger)
def expr_cast_columns(
    df: pl.DataFrame, renamed_columns: dict[str, list[str, pl.DataType]]
) -> list[pl.Expr]:
    for key in renamed_columns.keys():
        if key not in df.columns:
            raise Exception(f"Column {key} not found in dataframe")

    list_expressions = []

    for key in df.schema.keys():
        if key in renamed_columns.keys():
            dtype = renamed_columns[key][1]

            list_expressions.append(
                expr_cast_column(key, dtype, renamed_columns[key][0])
            )

    return list_expressions


@my_log.debug_log(logger)
def transform(df: pl.DataFrame, transformations: list[pl.Expr]) -> pl.DataFrame:
    return df.lazy().select(transformations).collect()
