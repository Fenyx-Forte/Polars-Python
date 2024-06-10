from logging import getLogger

import polars as pl

from logs import my_log

logger = getLogger("my_transform")
pl.Config.load_from_file("./config/polars.json")


class Column:
    initial_name: str
    final_name: str
    initial_dtype: pl.DataType
    final_dtype: pl.DataType
    expression: pl.Expr

    def __init__(
        self, initial_name: str, final_name: str, initial_dtype, final_dtype
    ):
        self.initial_name = initial_name
        self.final_name = final_name
        self.initial_dtype = initial_dtype
        self.final_dtype = final_dtype
        self.expression = pl.col(initial_name)


class Table:
    columns: dict[str, Column]

    def __init__(self, columns: dict[str, Column]):
        self.columns = columns

    def set_expr(self, column_name: str, expression: pl.Expr) -> None:
        self.columns[column_name].expression = expression

    def get_expr(self, column_name: str) -> pl.Expr:
        return self.columns[column_name].expression


def enade_table() -> Table:
    columns = {
        "ano": Column("Ano", "ano", pl.Int64, pl.Int16),
        "area_avaliacao": Column(
            "Área de Avaliação", "area_avaliacao", pl.String, pl.String
        ),
        "ies": Column("Nome da IES", "ies", pl.String, pl.String),
        "org_acad": Column(
            "Organização Acadêmica", "org_acad", pl.String, pl.String
        ),
        "cat_acad": Column(
            "Categoria Administrativa", "cat_acad", pl.String, pl.String
        ),
        "mod_ens": Column(
            "Modalidade de Ensino", "mod_ens", pl.String, pl.String
        ),
        "municipio_curso": Column(
            "Município do Curso", "municipio_curso", pl.String, pl.String
        ),
        "sigla_uf": Column("Sigla da UF", "sigla_uf", pl.String, pl.String),
        "num_conc_insc": Column(
            "Nº de Concluintes Inscritos", "num_conc_insc", pl.Int64, pl.Int16
        ),
        "num_conc_part": Column(
            "Nº  de Concluintes Participantes",
            "num_conc_part",
            pl.Int64,
            pl.Int16,
        ),
        "nota_bruta_fg": Column(
            "Nota Bruta - FG", "nota_bruta_fg", pl.Float64, pl.Float32
        ),
        "nota_padronizada_fg": Column(
            "Nota Padronizada - FG",
            "nota_padronizada_fg",
            pl.Float32,
            pl.Float32,
        ),
        "nota_bruta_ce": Column(
            "Nota Bruta - CE", "nota_bruta_ce", pl.Float64, pl.Float32
        ),
        "conc_enade_cont": Column(
            "Conceito Enade (Contínuo)",
            "conc_enade_cont",
            pl.Float32,
            pl.Float32,
        ),
        "conc_enade_faixa": Column(
            "Conceito Enade (Faixa)", "conc_enade_faixa", pl.String, pl.Int8
        ),
    }

    return Table(columns)


@my_log.debug_log(logger)
def verify_columns(df: pl.DataFrame, table: Table) -> None:
    columns = table.columns.values()

    for column in columns:
        if column.initial_name not in df.columns:
            raise pl.ColumnNotFoundError(
                f"Column {column.initial_name} not found in dataframe"
            )


@my_log.debug_log(logger)
def change_column_names(table: Table) -> Table:
    columns = table.columns

    for column in columns.values():
        table.set_expr(
            column.final_name,
            table.get_expr(column.final_name).alias(column.final_name),
        )

    return table


@my_log.debug_log(logger)
def transform_column_conceito_enade_faixa(table: Table) -> Table:
    table.set_expr(
        "conc_enade_faixa",
        table.get_expr("conc_enade_faixa").cast(pl.Int8, strict=False),
    )

    return table


@my_log.debug_log(logger)
def transform_columns(table: Table) -> Table:
    mod1_table = transform_column_conceito_enade_faixa(table)

    return mod1_table


@my_log.debug_log(logger)
def casting_columns(table: Table) -> Table:
    columns = table.columns

    for column in columns.values():
        table.set_expr(
            column.final_name, column.expression.cast(column.final_dtype)
        )

    return table


@my_log.debug_log(logger)
def standardizing_strings(table: Table) -> Table:
    columns = table.columns

    for column in columns.values():
        if column.final_dtype == pl.String:
            table.set_expr(
                column.final_name,
                column.expression.str.to_lowercase()
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
                .str.replace_all("  ", " "),
            )

    return table


@my_log.debug_log(logger)
def standardizing_data(table: Table) -> Table:
    mod_strings_table = standardizing_strings(table)

    return mod_strings_table


@my_log.debug_log(logger)
def shrinking_numerical_columns(table: Table) -> Table:
    columns = table.columns

    for column in columns.values():
        table.set_expr(column.final_name, column.expression.shrink_dtype())

    return table


@my_log.debug_log(logger)
def transform(df: pl.DataFrame, table: Table) -> pl.DataFrame:
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
