from logging import getLogger

import pandera.polars as pa
import polars as pl

from src.contrato_de_dados import contrato_entrada, contrato_saida
from src.etl import dataframe_utils, filtros
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
        filtros.filtro_enade_ano(),
        filtros.filtro_enade_num_conc_insc(),
        filtros.filtro_enade_num_conc_part(),
        filtros.filtro_enade_nota_bruta_fg(),
        filtros.filtro_enade_nota_padronizada_fg(),
        filtros.filtro_enade_nota_bruta_ce(),
        filtros.filtro_enade_nota_padronizada_ce(),
        filtros.filtro_enade_conc_enade_cont(),
        filtros.filtro_enade_conc_enade_faixa(),
        filtros.filtro_enade_num_conc_insc_maior_ou_igual_num_conc_part(),
    ]

    return filters


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
