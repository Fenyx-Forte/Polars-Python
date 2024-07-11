from logging import getLogger

import polars as pl

from src.utils import my_log

logger = getLogger("dataframe_utils")
pl.Config.load_from_file("./config/polars.json")


class DataTypeDifferents(Exception):
    """
    Exceção levantada quando os tipos de dados de uma ou mais colunas são diferentes do esperado.
    """

    ...


class Column:
    """
    Representa uma coluna de um DataFrame.

    Args:
        initial_name (str): O nome inicial da coluna.
        final_name (str): O nome final da coluna.
        initial_dtype (pl.DataType): O tipo de dados inicial da coluna.
        final_dtype (pl.DataType): O tipo de dados final da coluna.

    Attributes:
        initial_name (str): O nome inicial da coluna.
        final_name (str): O nome final da coluna.
        initial_dtype (pl.DataType): O tipo de dados inicial da coluna.
        final_dtype (pl.DataType): O tipo de dados final da coluna.
        expression (pl.Expr): A expressão da coluna.
    """

    initial_name: str
    final_name: str
    initial_dtype: pl.DataType
    final_dtype: pl.DataType
    expression: pl.Expr

    def __init__(
        self,
        initial_name: str,
        final_name: str,
        initial_dtype: pl.DataType,
        final_dtype: pl.DataType,
    ):
        self.initial_name = initial_name
        self.final_name = final_name
        self.initial_dtype = initial_dtype
        self.final_dtype = final_dtype
        self.expression = pl.col(initial_name)


class Table:
    """
    Representa uma tabela.

    Args:
        columns (dict[str, Column]): Dicionário que mapeia nomes de colunas para objetos
            da classe Column.

    Attributes:
        columns (dict[str, Column]): Dicionário que mapeia nomes de colunas para objetos
            da classe Column.
    """

    columns: dict[str, Column]

    def __init__(self, columns: dict[str, Column]) -> None:
        """
        Inicializa um objeto da classe Table.

        Args:
            columns: Dicionário que mapeia nomes de colunas para
                objetos da classe Column.

        Returns:
            None.
        """
        self.columns = columns

    def set_expr(self, column_name: str, expression: pl.Expr) -> None:
        """
        Define a expressão de uma coluna.

        Args:
            column_name: Nome da coluna.
            expression: Expressão da coluna.

        Returns:
            None.
        """
        self.columns[column_name].expression = expression

    def get_expr(self, column_name: str) -> pl.Expr:
        """
        Obtém a expressão de uma coluna.

        Args:
            column_name (str): Nome da coluna.

        Returns:
            Expressão da coluna.
        """
        return self.columns[column_name].expression


def enade_table() -> Table:
    """
    Cria um objeto Table com as colunas necessárias para a tabela ENADE.

    Returns:
        Um objeto Table com as colunas para a tabela ENADE.
    """

    columns = {
        "ano": Column("Ano", "ano", pl.Int64(), pl.Int16()),
        "area_avaliacao": Column(
            "Área de Avaliação", "area_avaliacao", pl.String(), pl.String()
        ),
        "ies": Column("Nome da IES", "ies", pl.String(), pl.String()),
        "org_acad": Column(
            "Organização Acadêmica", "org_acad", pl.String(), pl.String()
        ),
        "cat_acad": Column(
            "Categoria Administrativa", "cat_acad", pl.String(), pl.String()
        ),
        "mod_ens": Column(
            "Modalidade de Ensino", "mod_ens", pl.String(), pl.String()
        ),
        "municipio_curso": Column(
            "Município do Curso", "municipio_curso", pl.String(), pl.String()
        ),
        "sigla_uf": Column("Sigla da UF", "sigla_uf", pl.String(), pl.String()),
        "num_conc_insc": Column(
            "Nº de Concluintes Inscritos",
            "num_conc_insc",
            pl.Int64(),
            pl.Int16(),
        ),
        "num_conc_part": Column(
            "Nº  de Concluintes Participantes",
            "num_conc_part",
            pl.Int64(),
            pl.Int16(),
        ),
        "nota_bruta_fg": Column(
            "Nota Bruta - FG", "nota_bruta_fg", pl.Float64(), pl.Float32()
        ),
        "nota_padronizada_fg": Column(
            "Nota Padronizada - FG",
            "nota_padronizada_fg",
            pl.Float64(),
            pl.Float32(),
        ),
        "nota_bruta_ce": Column(
            "Nota Bruta - CE", "nota_bruta_ce", pl.Float64(), pl.Float32()
        ),
        "conc_enade_cont": Column(
            "Conceito Enade (Contínuo)",
            "conc_enade_cont",
            pl.Float64(),
            pl.Float32(),
        ),
        "conc_enade_faixa": Column(
            "Conceito Enade (Faixa)", "conc_enade_faixa", pl.String(), pl.Int8()
        ),
    }

    return Table(columns)
