from logging import getLogger

import pandera.polars as pa
import polars as pl


from src.contrato_de_dados import contrato_base, contrato_final
from src.etl import dataframe_utils
from src.utils import my_log

logger = getLogger("transforming_data")
pl.Config.load_from_file("./config/polars.json")


def change_column_names(table: dataframe_utils.Table) -> dataframe_utils.Table:
    """
    Altera o nome das colunas de um DataFrame.

    Args:
        table: A tabela a ser alterada.

    Returns:
        A tabela com o nome das colunas alteradas.
    """
    columns = table.columns

    for column in columns.values():
        table.set_expr(
            column.final_name,
            table.get_expr(column.final_name).alias(column.final_name),
        )

    return table


def verifica_enade_base_lf(lf_enade: pl.LazyFrame) -> None:
    try:
        contrato_base.EnadeBase.validate(lf_enade, lazy=True)
        logger.info("Sem erros de schema na base Enade!")

    except pa.errors.SchemaError as exc:
        logger.error(exc)


def verifica_enade_base_df(df_enade: pl.DataFrame) -> None:
    try:
        contrato_base.EnadeBase.validate(df_enade, lazy=True)

        logger.info("Base Enade valida! Nenhum erro detectado\n")

    except pa.errors.SchemaError as exc:
        logger.error(exc)


def transform_column_conceito_enade_faixa(
    table: dataframe_utils.Table,
) -> dataframe_utils.Table:
    """
    Transforma a coluna "conc_enade_faixa" de um DataFrame em um tipo de dados Int8.

    Args:
        table: A tabela a ser alterada.

    Returns:
        A tabela com a coluna "conc_enade_faixa" transformada.
    """
    table.set_expr(
        "conc_enade_faixa",
        table.get_expr("conc_enade_faixa").cast(pl.Int8, strict=False),
    )

    return table


def transform_columns(table: dataframe_utils.Table) -> dataframe_utils.Table:
    """
    Aplica transformações específicas em colunas específicas.

    Args:
        table: A tabela a ser transformada.

    Returns:
        A tabela com as transformações aplicadas.
    """
    mod1_table = transform_column_conceito_enade_faixa(table)

    return mod1_table


def casting_columns(table: dataframe_utils.Table) -> dataframe_utils.Table:
    """
    Realiza o casting de todas as colunas da tabela para o tipo de dados final definido.

    Args:
        table: A tabela a ser alterada.

    Returns:
        A tabela com as colunas transformadas.
    """
    columns = table.columns

    for column in columns.values():
        table.set_expr(
            column.final_name, column.expression.cast(column.final_dtype)
        )

    return table


def standardizing_strings(
    table: dataframe_utils.Table,
) -> dataframe_utils.Table:
    """
    Aplica transformações para padronizar strings nas colunas de uma tabela.

    Args:
        table: A tabela a ser transformada.

    Returns:
        A tabela com as strings padronizadas.
    """
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
    """
    Aplica transformações para padronizar os dados de uma tabela.

    Args:
        table: A tabela a ser transformada.

    Returns:
        A tabela com os dados padronizados.
    """
    mod_strings_table = standardizing_strings(table)

    return mod_strings_table


def shrinking_numerical_columns(
    table: dataframe_utils.Table,
) -> dataframe_utils.Table:
    """
    Reduz o tamanho dos dados de todas as colunas numéricas de uma tabela.

    Args:
        table: A tabela a ser transformada.

    Returns:
        A tabela com os tipos de dados das colunas numéricas otimizados em relação à armazenamento.
    """
    columns = table.columns

    for column in columns.values():
        table.set_expr(column.final_name, column.expression.shrink_dtype())

    return table


def enade_filters() -> list[pl.Expr]:
    filters = [
        pl.col("ano").is_between(2017, 2021),
        pl.col("num_conc_insc") >= 1,
        pl.col("num_conc_part") >= 1,
        pl.col("nota_bruta_fg").is_between(0, 100),
        pl.col("nota_padronizada_fg").is_between(0, 5),
        pl.col("nota_bruta_ce").is_between(0, 100),
        pl.col("nota_padronizada_ce").is_between(0, 5),
        pl.col("conc_enade_cont").is_between(0, 5),
        pl.col("conc_enade_faixa").is_between(1, 5),
    ]

    return filters


def verifica_enade_final_lf(lf_enade: pl.LazyFrame) -> None:
    try:
        contrato_final.EnadeFinal.validate(lf_enade, lazy=True)
        logger.info("Sem erros de schema no Enade final!")

    except pa.errors.SchemaError as exc:
        logger.error(exc)


def verifica_enade_final_df(df_enade: pl.DataFrame) -> None:
    try:
        contrato_final.EnadeFinal.validate(df_enade, lazy=True)

        logger.info("Enade Final valido! Nenhum erro detectado!\n")

    except pa.errors.SchemaError as exc:
        logger.error(exc)


def apply_transformations(
    df: pl.DataFrame,
    table: dataframe_utils.Table,
) -> pl.LazyFrame:
    transformations = []

    columns = table.columns
    for column in columns.values():
        transformations.append(column.expression)

    return df.lazy().select(transformations)


def transform(
    df: pl.DataFrame, table: dataframe_utils.Table, filters: list[pl.Expr]
) -> pl.DataFrame:
    """
    Aplica transformações a uma DataFrame com base em uma tabela especificada.

    Args:
        df: O DataFrame a ser transformado.
        table: A tabela que define as transformações a serem aplicadas.

    Returns:
        O DataFrame transformado.

    Raises:
        polars.ComputeError: Se ocorrer um erro de computação durante a transformação.
        Exception: Se ocorrer um erro inesperado durante a transformação.
    """
    logger.info("Starting transformation...")
    try:
        final_df = (
            apply_transformations(df, table)
            .filter(*filters)
            .drop_nulls()
            .unique()
            .collect()
        )

        logger.info("Transformation done!")

        return final_df

    except pl.ComputeError as e:
        logger.error(e)
        raise

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise
