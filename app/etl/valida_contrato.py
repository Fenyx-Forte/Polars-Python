from logging import getLogger

import pandera.polars as pa
import polars as pl
from contrato_de_dados import contrato_entrada, contrato_saida
from uteis import my_log

logger = getLogger("transforming_data")
pl.Config.load_from_file("./config/polars.json")


def valida_enade_entrada_lf(lf_enade: pl.LazyFrame) -> None:
    try:
        contrato_entrada.EnadeEntrada.validate(lf_enade, lazy=True)
        logger.info("Sem erros de schema no Enade Entrada!")

    except pa.errors.SchemaError as exc:
        logger.error(exc)


def valida_enade_entrada_df(df_enade: pl.DataFrame) -> None:
    try:
        contrato_entrada.EnadeEntrada.validate(df_enade, lazy=True)

        logger.info("Enade Entrada valido! Nenhum erro detectado\n")

    except pa.errors.SchemaError as exc:
        logger.error(exc)


def valida_enade_saida_lf(lf_enade: pl.LazyFrame) -> None:
    try:
        contrato_saida.EnadeSaida.validate(lf_enade, lazy=True)
        logger.info("Sem erros de schema no Enade Saida!")

    except pa.errors.SchemaError as exc:
        logger.error(exc)


def valida_enade_saida_df(df_enade: pl.DataFrame) -> None:
    try:
        contrato_saida.EnadeSaida.validate(df_enade, lazy=True)

        logger.info("Enade Saida valido! Nenhum erro detectado!\n")

    except pa.errors.SchemaError as exc:
        logger.error(exc)
