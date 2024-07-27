from logging import getLogger

import pandera.polars as pa
import polars as pl

from src.contrato_de_dados import contrato_saida
from src.utils import my_log

logger = getLogger("dataframe_utils")
pl.Config.load_from_file("./config/polars.json")


class TabelaAuxiliarTransformacao:
    renomear_colunas: dict[str, str]
    casting_colunas: dict[str, pl.DataType]
    transformacoes_colunas: dict[str, pl.Expr]
    dtypes_finais_colunas: dict[str, pl.DataType]

    def __init__(
        self,
        contrato_de_dados_saida: pa.DataFrameModel,
    ) -> None:
        schema_saida = contrato_de_dados_saida.to_schema()

        self.dtypes_finais_colunas = {}

        for field in schema_saida.columns.values():
            self.dtypes_finais_colunas[field.name] = field.dtype.type

    def set_expr(self, column_name: str, expression: pl.Expr) -> None:
        self.transformacoes_colunas[column_name] = expression

    def get_expr(self, column_name: str) -> pl.Expr:
        return self.transformacoes_colunas[column_name]

    def lista_nomes_colunas_originais(self) -> list[str]:
        return list(self.renomear_colunas.keys())

    def lista_nomes_colunas_modificados(self) -> list[str]:
        return list(self.renomear_colunas.values())


class TabelaAuxiliarEnade(TabelaAuxiliarTransformacao):
    def __init__(self) -> None:
        super().__init__(contrato_saida.EnadeSaida)

        self.renomear_colunas = {
            "Ano": "ano",
            "Área de Avaliação": "area_avaliacao",
            "Nome da IES": "ies",
            "Organização Acadêmica": "org_acad",
            "Categoria Administrativa": "cat_acad",
            "Modalidade de Ensino": "mod_ens",
            "Município do Curso": "municipio_curso",
            "Sigla da UF": "sigla_uf",
            "Nº de Concluintes Inscritos": "num_conc_insc",
            "Nº  de Concluintes Participantes": "num_conc_part",
            "Nota Bruta - FG": "nota_bruta_fg",
            "Nota Padronizada - FG": "nota_padronizada_fg",
            "Nota Bruta - CE": "nota_bruta_ce",
            "Nota Padronizada - CE": "nota_padronizada_ce",
            "Conceito Enade (Contínuo)": "conc_enade_cont",
            "Conceito Enade (Faixa)": "conc_enade_faixa",
        }

        self.casting_colunas = {"conc_enade_faixa": pl.Int8}

        self.transformacoes_colunas = {}

        for nome_original, nome_modificado in self.renomear_colunas.items():
            self.transformacoes_colunas[nome_modificado] = pl.col(nome_original)
