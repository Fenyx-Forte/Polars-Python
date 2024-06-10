import pytest
from polars.testing import assert_frame_equal
from src.etl import transforming_data
import polars as pl


@pytest.fixture
def new_column_names() -> list[str]:
    return [
        "ano",
        "area_avaliacao",
        "ies",
        "org_acad",
        "cat_acad",
        "mod_ens",
        "municipio_curso",
        "sigla_uf",
        "num_conc_insc",
        "num_conc_part",
        "nota_bruta_fg",
        "nota_padronizada_fg",
        "nota_bruta_ce",
        "conc_enade_cont",
        "conc_enade_faixa",
    ]


@pytest.fixture
def enade_dataframe() -> pl.DataFrame:
    df = pl.DataFrame(
        {
            "Ano": [2019, 2019, 2019],
            "Área de Avaliação": ["a", "b", "c"],
            "Nome da IES": ["a", "b", "c"],
            "Organização Acadêmica": ["a", "b", "c"],
            "Categoria Administrativa": ["a", "b", "c"],
            "Modalidade de Ensino": ["a", "b", "c"],
            "Município do Curso": ["a", "b", "c"],
            "Sigla da UF": ["a", "b", "c"],
            "Nº de Concluintes Inscritos": [1, 2, 3],
            "Nº  de Concluintes Participantes": [1, 2, 3],
            "Nota Bruta - FG": [1, 2, 3],
            "Nota Padronizada - FG": [1, 2, 3],
            "Nota Bruta - CE": [1, 2, 3],
            "Conceito Enade (Contínuo)": [1, 2, 3],
            "Conceito Enade (Faixa)": [1, 2, 3],
        }
    )

    return df


@pytest.fixture
def enade_table() -> transforming_data.Table:
    return transforming_data.enade_table()


def test_verify_columns(
    enade_dataframe: pl.DataFrame, enade_table: transforming_data.Table
) -> None:
    assert (
        transforming_data.verify_columns(enade_dataframe, enade_table) is None
    )


def test_change_column_names(
    enade_dataframe: pl.DataFrame,
    enade_table: transforming_data.Table,
    new_column_names: list[str],
) -> None:
    modified_table = transforming_data.change_column_names(enade_table)

    modified_df = transforming_data.transform(enade_dataframe, modified_table)

    assert modified_df.columns == new_column_names
