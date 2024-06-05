from logging import getLogger

import polars as pl
from polars.expr import Expr

from logs import my_log

logger = getLogger("my_transform")


class Column:
    name_in_file: str
    name_standardized: str
    dtype: pl.DataType

    def __init__(self, name_in_file: str, name_standardized: str, dtype):
        self.name_in_file = name_in_file
        self.name_standardized = name_standardized
        self.dtype = dtype


def enade_columns() -> list[Column]:
    column_1 = Column("Ano", "ano", pl.Int16)

    column_2 = Column("Área de Avaliação", "area_avaliacao", pl.String)

    column_3 = Column("Nome da IES", "ies", pl.String)

    column_4 = Column("Organização Acadêmica", "org_acad", pl.String)

    column_5 = Column("Categoria Administrativa", "cat_acad", pl.String)

    column_6 = Column("Modalidade de Ensino", "mod_ens", pl.String)

    column_7 = Column("Município do Curso", "municipio_curso", pl.String)

    column_8 = Column("Sigla da UF", "sigla_uf", pl.String)

    column_9 = Column("Nº de Concluintes Inscritos", "num_conc_insc", pl.Int16)

    column_10 = Column(
        "Nº  de Concluintes Participantes", "num_conc_part", pl.Int16
    )

    column_11 = Column("Nota Bruta - FG", "nota_bruta_fg", pl.Float32)

    column_12 = Column(
        "Nota Padronizada - FG", "nota_padronizada_fg", pl.Float32
    )

    column_13 = Column("Nota Bruta - CE", "nota_bruta_ce", pl.Float32)

    column_14 = Column(
        "Nota Padronizada - CE", "nota_padronizada_ce", pl.Float32
    )

    column_15 = Column(
        "Conceito Enade (Contínuo)", "conc_enade_cont", pl.Float32
    )

    column_16 = Column("Conceito Enade (Faixa)", "conc_enade_faixa", pl.Int8)

    list_columns = [
        column_1,
        column_2,
        column_3,
        column_4,
        column_5,
        column_6,
        column_7,
        column_8,
        column_9,
        column_10,
        column_11,
        column_12,
        column_13,
        column_14,
        column_15,
        column_16,
    ]

    return list_columns


@my_log.debug_log(logger)
def verify_columns(df: pl.DataFrame) -> None:
    columns = enade_columns()

    for column in columns:
        if column.name_in_file not in df.columns:
            raise Exception(
                f"Column {column.name_in_file} not found in dataframe"
            )


@my_log.debug_log(logger)
def change_column_names() -> list[pl.Expr]:
    columns = enade_columns()

    list_expressions = [
        pl.col(column.name_in_file).alias(column.name_standardized)
        for column in columns
    ]

    return list_expressions


@my_log.debug_log(logger)
def transform_column_conceito_enade_faixa() -> pl.Expr:
    return pl.col("conc_enade_faixa").cast(pl.Int8, strict=False)


@my_log.debug_log(logger)
def transform_columns() -> list[pl.Expr]:
    conceito_enade_faixa = transform_column_conceito_enade_faixa()

    list_expressions = [conceito_enade_faixa]

    return list_expressions


@my_log.debug_log(logger)
def casting_columns() -> list[pl.Expr]:
    list_expressions = [
        pl.col(column.name_standardized).cast(column.dtype)
        for column in enade_columns()
    ]

    return list_expressions


@my_log.debug_log(logger)
def standardizing_strings(columns_strings: list[str]) -> pl.Expr:
    return (
        pl.col(*columns_strings)
        .str.to_lowercase()
        .str.strip()
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
        .str.replace_all("  ", " ")
    )


@my_log.debug_log(logger)
def standardizing_data() -> list[Expr]:
    all_columns = enade_columns()

    columns_strings = [
        column.name_standardized
        for column in all_columns
        if column.dtype == pl.String
    ]

    strings = standardizing_strings(columns_strings)

    list_expressions = [strings]

    return list_expressions


@my_log.debug_log(logger)
def shrinking_numerical_columns() -> list[pl.Expr]:
    return [pl.col("*").shrink_dtype()]


@my_log.debug_log(logger)
def transform(
    df: pl.DataFrame, transformations: list[list[pl.Expr]]
) -> pl.DataFrame:
    t0 = transformations[0]
    t1 = transformations[1]
    t2 = transformations[2]
    t3 = transformations[3]
    t4 = transformations[4]

    return (
        df.lazy()
        .select(t0)
        .with_columns(t1)
        .select(t2)
        .with_columns(t3)
        .select(t4)
        .drop_nulls()
        .unique()
        .collect()
    )
