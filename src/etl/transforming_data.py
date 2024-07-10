from logging import getLogger

import polars as pl

from src.etl import dataframe_utils
from src.utils import my_log

logger = getLogger("transforming_data")
pl.Config.load_from_file("./config/polars.json")


def verify_datatype(df: pl.DataFrame, table: dataframe_utils.Table) -> None:
    columns = table.columns.values()

    schema = df.schema

    wrong_columns = []

    for column in columns:
        if not (schema[column.initial_name].is_(column.initial_dtype)):
            wrong_columns.append(
                [
                    column.initial_name,
                    schema[column.initial_name],
                    column.initial_dtype,
                ]
            )

    if len(wrong_columns) >= 1:
        raise dataframe_utils.DataTypeDifferents(f"{wrong_columns}")


def change_column_names(table: dataframe_utils.Table) -> dataframe_utils.Table:
    columns = table.columns

    for column in columns.values():
        table.set_expr(
            column.final_name,
            table.get_expr(column.final_name).alias(column.final_name),
        )

    return table


def transform_column_conceito_enade_faixa(
    table: dataframe_utils.Table,
) -> dataframe_utils.Table:
    table.set_expr(
        "conc_enade_faixa",
        table.get_expr("conc_enade_faixa").cast(pl.Int8, strict=False),
    )

    return table


def transform_columns(table: dataframe_utils.Table) -> dataframe_utils.Table:
    mod1_table = transform_column_conceito_enade_faixa(table)

    return mod1_table


def casting_columns(table: dataframe_utils.Table) -> dataframe_utils.Table:
    columns = table.columns

    for column in columns.values():
        table.set_expr(
            column.final_name, column.expression.cast(column.final_dtype)
        )

    return table


def standardizing_strings(
    table: dataframe_utils.Table,
) -> dataframe_utils.Table:
    columns = table.columns

    for column in columns.values():
        if column.final_dtype.is_(pl.String):
            table.set_expr(
                column.final_name,
                column.expression.str.to_lowercase()
                .str.strip_chars()
                .str.replace_all("á", "a")
                .str.replace_all("â", "a")
                .str.replace_all("ã", "a")
                .str.replace_all("ê", "e")
                .str.replace_all("é", "a")
                .str.replace_all("í", "i")
                .str.replace_all("õ", "o")
                .str.replace_all("ó", "o")
                .str.replace_all("ú", "u")
                .str.replace_all("ç", "c")
                .str.replace_all("-", " ")
                .str.replace_all("  ", " "),
            )

    return table


def standardizing_data(table: dataframe_utils.Table) -> dataframe_utils.Table:
    mod_strings_table = standardizing_strings(table)

    return mod_strings_table


def shrinking_numerical_columns(
    table: dataframe_utils.Table,
) -> dataframe_utils.Table:
    columns = table.columns

    for column in columns.values():
        table.set_expr(column.final_name, column.expression.shrink_dtype())

    return table


def transform(df: pl.DataFrame, table: dataframe_utils.Table) -> pl.DataFrame:
    transformations = []

    columns = table.columns
    for column in columns.values():
        transformations.append(column.expression)

    logger.info("Starting transformation...")
    try:
        final_df = (
            df.lazy().select(transformations).drop_nulls().unique().collect()
        )

        logger.info("Transformation done!")

        return final_df

    except pl.ComputeError as e:
        logger.error(e)
        raise

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise
