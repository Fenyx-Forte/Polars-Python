from logging import getLogger

import polars as pl

from src.etl import dataframe_utils, filtros
from src.utils import my_log

logger = getLogger("transforming_data")
pl.Config.load_from_file("./config/polars.json")


def change_column_names(
    tabela_aux: dataframe_utils.TabelaAuxiliarTransformacao,
) -> dataframe_utils.TabelaAuxiliarTransformacao:
    """
    Altera o nome das colunas de um DataFrame.

    Args:
        tabela_aux: A tabela a ser alterada.

    Returns:
        A tabela com o nome das colunas alteradas.
    """
    for nome_modificado in tabela_aux.renomear_colunas.values():
        tabela_aux.set_expr(
            nome_modificado,
            tabela_aux.get_expr(nome_modificado).alias(nome_modificado),
        )

    return tabela_aux


def casting_columns(
    tabela_aux: dataframe_utils.TabelaAuxiliarTransformacao,
) -> dataframe_utils.TabelaAuxiliarTransformacao:
    """
    Realiza o casting de colunas especificas.

    Args:
        tabela_aux: A tabela a ser alterada.

    Returns:
        A tabela com as colunas transformadas.
    """
    for nome_coluna, novo_dtype in tabela_aux.casting_colunas.items():
        tabela_aux.set_expr(
            nome_coluna,
            tabela_aux.get_expr(nome_coluna).cast(novo_dtype, strict=False),
        )

    return tabela_aux


def standardizing_strings(
    tabela_aux: dataframe_utils.TabelaAuxiliarTransformacao,
) -> dataframe_utils.TabelaAuxiliarTransformacao:
    """
    Aplica transformações para padronizar strings nas colunas de uma tabela.

    Args:
        tabela_aux: A tabela a ser transformada.

    Returns:
        A tabela com as strings padronizadas.
    """
    for coluna, data_type in tabela_aux.dtypes_finais_colunas.items():
        if data_type.is_(pl.String):
            tabela_aux.set_expr(
                coluna,
                tabela_aux.get_expr(coluna)
                .str.to_lowercase()
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

    return tabela_aux


def standardizing_data(
    tabela_aux: dataframe_utils.TabelaAuxiliarTransformacao,
) -> dataframe_utils.TabelaAuxiliarTransformacao:
    """
    Aplica transformações para padronizar os dados de uma tabela.

    Args:
        tabela_aux: A tabela a ser transformada.

    Returns:
        A tabela com os dados padronizados.
    """
    mod_strings_table = standardizing_strings(tabela_aux)

    return mod_strings_table


def shrinking_numerical_columns(
    tabela_aux: dataframe_utils.TabelaAuxiliarTransformacao,
) -> dataframe_utils.TabelaAuxiliarTransformacao:
    """
    Reduz o tamanho dos dados de todas as colunas numéricas de uma tabela.

    Args:
        tabela_aux: A tabela a ser transformada.

    Returns:
        A tabela com os tipos de dados das colunas numéricas otimizados em relação à armazenamento.
    """
    for coluna in tabela_aux.transformacoes_colunas.keys():
        tabela_aux.set_expr(coluna, tabela_aux.get_expr(coluna).shrink_dtype())

    return tabela_aux


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
    tabela_aux: dataframe_utils.TabelaAuxiliarTransformacao,
) -> pl.LazyFrame:
    transformations = list(tabela_aux.transformacoes_colunas.values())

    return df.lazy().select(*transformations)


def transform(
    df: pl.DataFrame,
    tabela_aux: dataframe_utils.TabelaAuxiliarTransformacao,
    filters: list[pl.Expr],
) -> pl.DataFrame:
    """
    Aplica transformações a uma DataFrame com base em uma tabela especificada.

    Args:
        df: O DataFrame a ser transformado.
        tabela_aux: A tabela que define as transformações a serem aplicadas.

    Returns:
        O DataFrame transformado.

    Raises:
        polars.ComputeError: Se ocorrer um erro de computação durante a transformação.
        Exception: Se ocorrer um erro inesperado durante a transformação.
    """
    logger.info("Starting transformation...")
    try:
        final_df = (
            apply_transformations(df, tabela_aux)
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
