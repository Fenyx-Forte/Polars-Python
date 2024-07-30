import pandera.polars as pa
import polars as pl
import pytest
from contrato_de_dados import contrato_saida


def test_contrato_correto():
    df_teste = pl.DataFrame(
        {
            "ano": [2018, 2019, 2021],
            "area_avaliacao": ["A", "A", "A"],
            "ies": ["A", "A", "A"],
            "org_acad": ["A", "A", "A"],
            "cat_acad": ["A", "A", "A"],
            "mod_ens": ["A", "A", "A"],
            "municipio_curso": ["A", "A", "A"],
            "sigla_uf": ["A", "A", "A"],
            "num_conc_insc": [10, 6, 4],
            "num_conc_part": [8, 4, 3],
            "nota_bruta_fg": [51.23, 60.44, 48.15],
            "nota_padronizada_fg": [3.48, 4.50, 4.02],
            "nota_bruta_ce": [60.10, 55.22, 34.90],
            "nota_padronizada_ce": [3.70, 4.21, 3.92],
            "conc_enade_cont": [3.40, 4.00, 4.12],
            "conc_enade_faixa": [3, 4, 4],
        },
        schema_overrides={
            "ano": pl.Int16,
            "num_conc_insc": pl.Int16,
            "num_conc_part": pl.Int16,
            "nota_bruta_fg": pl.Float32,
            "nota_padronizada_fg": pl.Float32,
            "nota_bruta_ce": pl.Float32,
            "nota_padronizada_ce": pl.Float32,
            "conc_enade_cont": pl.Float32,
            "conc_enade_faixa": pl.Int8,
        },
    )

    contrato_saida.EnadeSaida.validate(df_teste)


def test_ano_fora_do_intervalo():
    df_teste = pl.DataFrame(
        {
            "ano": [2016, 2017, 2021],
            "area_avaliacao": ["A", "A", "A"],
            "ies": ["A", "A", "A"],
            "org_acad": ["A", "A", "A"],
            "cat_acad": ["A", "A", "A"],
            "mod_ens": ["A", "A", "A"],
            "municipio_curso": ["A", "A", "A"],
            "sigla_uf": ["A", "A", "A"],
            "num_conc_insc": [10, 6, 4],
            "num_conc_part": [8, 4, 3],
            "nota_bruta_fg": [51.23, 60.44, 48.15],
            "nota_padronizada_fg": [3.48, 4.50, 4.02],
            "nota_bruta_ce": [60.10, 55.22, 34.90],
            "nota_padronizada_ce": [3.70, 4.21, 3.92],
            "conc_enade_cont": [3.40, 4.00, 4.12],
            "conc_enade_faixa": [3, 4, 4],
        },
        schema_overrides={
            "ano": pl.Int16,
            "num_conc_insc": pl.Int16,
            "num_conc_part": pl.Int16,
            "nota_bruta_fg": pl.Float32,
            "nota_padronizada_fg": pl.Float32,
            "nota_bruta_ce": pl.Float32,
            "nota_padronizada_ce": pl.Float32,
            "conc_enade_cont": pl.Float32,
            "conc_enade_faixa": pl.Int8,
        },
    )

    with pytest.raises(pa.errors.SchemaError):
        contrato_saida.EnadeSaida.validate(df_teste)


def test_num_conc_insc_abaixo_de_um():
    df_teste = pl.DataFrame(
        {
            "ano": [2018, 2019, 2021],
            "area_avaliacao": ["A", "A", "A"],
            "ies": ["A", "A", "A"],
            "org_acad": ["A", "A", "A"],
            "cat_acad": ["A", "A", "A"],
            "mod_ens": ["A", "A", "A"],
            "municipio_curso": ["A", "A", "A"],
            "sigla_uf": ["A", "A", "A"],
            "num_conc_insc": [10, 6, 0],
            "num_conc_part": [8, 4, 0],
            "nota_bruta_fg": [51.23, 60.44, 48.15],
            "nota_padronizada_fg": [3.48, 4.50, 4.02],
            "nota_bruta_ce": [60.10, 55.22, 34.90],
            "nota_padronizada_ce": [3.70, 4.21, 3.92],
            "conc_enade_cont": [3.40, 4.00, 4.12],
            "conc_enade_faixa": [3, 4, 4],
        },
        schema_overrides={
            "ano": pl.Int16,
            "num_conc_insc": pl.Int16,
            "num_conc_part": pl.Int16,
            "nota_bruta_fg": pl.Float32,
            "nota_padronizada_fg": pl.Float32,
            "nota_bruta_ce": pl.Float32,
            "nota_padronizada_ce": pl.Float32,
            "conc_enade_cont": pl.Float32,
            "conc_enade_faixa": pl.Int8,
        },
    )

    with pytest.raises(pa.errors.SchemaError):
        contrato_saida.EnadeSaida.validate(df_teste)


def test_num_conc_part_negativo():
    df_teste = pl.DataFrame(
        {
            "ano": [2017, 2018, 2021],
            "area_avaliacao": ["A", "A", "A"],
            "ies": ["A", "A", "A"],
            "org_acad": ["A", "A", "A"],
            "cat_acad": ["A", "A", "A"],
            "mod_ens": ["A", "A", "A"],
            "municipio_curso": ["A", "A", "A"],
            "sigla_uf": ["A", "A", "A"],
            "num_conc_insc": [10, 6, 4],
            "num_conc_part": [8, 4, -1],
            "nota_bruta_fg": [51.23, 60.44, 48.15],
            "nota_padronizada_fg": [3.48, 4.50, 4.02],
            "nota_bruta_ce": [60.10, 55.22, 34.90],
            "nota_padronizada_ce": [3.70, 4.21, 3.92],
            "conc_enade_cont": [3.40, 4.00, 4.12],
            "conc_enade_faixa": [3, 4, 4],
        },
        schema_overrides={
            "ano": pl.Int16,
            "num_conc_insc": pl.Int16,
            "num_conc_part": pl.Int16,
            "nota_bruta_fg": pl.Float32,
            "nota_padronizada_fg": pl.Float32,
            "nota_bruta_ce": pl.Float32,
            "nota_padronizada_ce": pl.Float32,
            "conc_enade_cont": pl.Float32,
            "conc_enade_faixa": pl.Int8,
        },
    )

    with pytest.raises(pa.errors.SchemaError):
        contrato_saida.EnadeSaida.validate(df_teste)


def test_nota_bruta_fg_fora_do_intervalo():
    df_teste = pl.DataFrame(
        {
            "ano": [2017, 2018, 2021],
            "area_avaliacao": ["A", "A", "A"],
            "ies": ["A", "A", "A"],
            "org_acad": ["A", "A", "A"],
            "cat_acad": ["A", "A", "A"],
            "mod_ens": ["A", "A", "A"],
            "municipio_curso": ["A", "A", "A"],
            "sigla_uf": ["A", "A", "A"],
            "num_conc_insc": [10, 6, 4],
            "num_conc_part": [8, 4, 3],
            "nota_bruta_fg": [-45.00, 60.44, 104.50],
            "nota_padronizada_fg": [3.48, 4.50, 4.02],
            "nota_bruta_ce": [60.10, 55.22, 34.90],
            "nota_padronizada_ce": [3.70, 4.21, 3.92],
            "conc_enade_cont": [3.40, 4.00, 4.12],
            "conc_enade_faixa": [3, 4, 4],
        },
        schema_overrides={
            "ano": pl.Int16,
            "num_conc_insc": pl.Int16,
            "num_conc_part": pl.Int16,
            "nota_bruta_fg": pl.Float32,
            "nota_padronizada_fg": pl.Float32,
            "nota_bruta_ce": pl.Float32,
            "nota_padronizada_ce": pl.Float32,
            "conc_enade_cont": pl.Float32,
            "conc_enade_faixa": pl.Int8,
        },
    )

    with pytest.raises(pa.errors.SchemaError):
        contrato_saida.EnadeSaida.validate(df_teste)


def test_nota_padronizada_fg_fora_do_intervalo():
    df_teste = pl.DataFrame(
        {
            "ano": [2017, 2018, 2021],
            "area_avaliacao": ["A", "A", "A"],
            "ies": ["A", "A", "A"],
            "org_acad": ["A", "A", "A"],
            "cat_acad": ["A", "A", "A"],
            "mod_ens": ["A", "A", "A"],
            "municipio_curso": ["A", "A", "A"],
            "sigla_uf": ["A", "A", "A"],
            "num_conc_insc": [10, 6, 4],
            "num_conc_part": [8, 4, 3],
            "nota_bruta_fg": [51.23, 60.44, 48.15],
            "nota_padronizada_fg": [-2.40, 4.50, 5.40],
            "nota_bruta_ce": [60.10, 55.22, 34.90],
            "nota_padronizada_ce": [3.70, 4.21, 3.92],
            "conc_enade_cont": [3.40, 4.00, 4.12],
            "conc_enade_faixa": [3, 4, 4],
        },
        schema_overrides={
            "ano": pl.Int16,
            "num_conc_insc": pl.Int16,
            "num_conc_part": pl.Int16,
            "nota_bruta_fg": pl.Float32,
            "nota_padronizada_fg": pl.Float32,
            "nota_bruta_ce": pl.Float32,
            "nota_padronizada_ce": pl.Float32,
            "conc_enade_cont": pl.Float32,
            "conc_enade_faixa": pl.Int8,
        },
    )

    with pytest.raises(pa.errors.SchemaError):
        contrato_saida.EnadeSaida.validate(df_teste)


def test_nota_bruta_ce_fora_do_intervalo():
    df_teste = pl.DataFrame(
        {
            "ano": [2017, 2018, 2021],
            "area_avaliacao": ["A", "A", "A"],
            "ies": ["A", "A", "A"],
            "org_acad": ["A", "A", "A"],
            "cat_acad": ["A", "A", "A"],
            "mod_ens": ["A", "A", "A"],
            "municipio_curso": ["A", "A", "A"],
            "sigla_uf": ["A", "A", "A"],
            "num_conc_insc": [10, 6, 4],
            "num_conc_part": [8, 4, 3],
            "nota_bruta_fg": [51.23, 60.44, 48.15],
            "nota_padronizada_fg": [3.48, 4.50, 4.02],
            "nota_bruta_ce": [-60.10, 55.22, 134.90],
            "nota_padronizada_ce": [3.70, 4.21, 3.92],
            "conc_enade_cont": [3.40, 4.00, 4.12],
            "conc_enade_faixa": [3, 4, 4],
        },
        schema_overrides={
            "ano": pl.Int16,
            "num_conc_insc": pl.Int16,
            "num_conc_part": pl.Int16,
            "nota_bruta_fg": pl.Float32,
            "nota_padronizada_fg": pl.Float32,
            "nota_bruta_ce": pl.Float32,
            "nota_padronizada_ce": pl.Float32,
            "conc_enade_cont": pl.Float32,
            "conc_enade_faixa": pl.Int8,
        },
    )

    with pytest.raises(pa.errors.SchemaError):
        contrato_saida.EnadeSaida.validate(df_teste)


def test_nota_padronizada_ce_fora_do_intervalo():
    df_teste = pl.DataFrame(
        {
            "ano": [2017, 2018, 2021],
            "area_avaliacao": ["A", "A", "A"],
            "ies": ["A", "A", "A"],
            "org_acad": ["A", "A", "A"],
            "cat_acad": ["A", "A", "A"],
            "mod_ens": ["A", "A", "A"],
            "municipio_curso": ["A", "A", "A"],
            "sigla_uf": ["A", "A", "A"],
            "num_conc_insc": [10, 6, 4],
            "num_conc_part": [8, 4, 3],
            "nota_bruta_fg": [51.23, 60.44, 48.15],
            "nota_padronizada_fg": [3.48, 4.50, 4.02],
            "nota_bruta_ce": [60.10, 55.22, 34.90],
            "nota_padronizada_ce": [-3.70, 4.21, 13.92],
            "conc_enade_cont": [3.40, 4.00, 4.12],
            "conc_enade_faixa": [3, 4, 4],
        },
        schema_overrides={
            "ano": pl.Int16,
            "num_conc_insc": pl.Int16,
            "num_conc_part": pl.Int16,
            "nota_bruta_fg": pl.Float32,
            "nota_padronizada_fg": pl.Float32,
            "nota_bruta_ce": pl.Float32,
            "nota_padronizada_ce": pl.Float32,
            "conc_enade_cont": pl.Float32,
            "conc_enade_faixa": pl.Int8,
        },
    )

    with pytest.raises(pa.errors.SchemaError):
        contrato_saida.EnadeSaida.validate(df_teste)


def test_conc_enade_cont_fora_do_intervalo():
    df_teste = pl.DataFrame(
        {
            "ano": [2017, 2018, 2021],
            "area_avaliacao": ["A", "A", "A"],
            "ies": ["A", "A", "A"],
            "org_acad": ["A", "A", "A"],
            "cat_acad": ["A", "A", "A"],
            "mod_ens": ["A", "A", "A"],
            "municipio_curso": ["A", "A", "A"],
            "sigla_uf": ["A", "A", "A"],
            "num_conc_insc": [10, 6, 4],
            "num_conc_part": [8, 4, 3],
            "nota_bruta_fg": [51.23, 60.44, 48.15],
            "nota_padronizada_fg": [3.48, 4.50, 4.02],
            "nota_bruta_ce": [60.10, 55.22, 34.90],
            "nota_padronizada_ce": [3.70, 4.21, 3.92],
            "conc_enade_cont": [-3.40, 4.00, 14.12],
            "conc_enade_faixa": [3, 4, 4],
        },
        schema_overrides={
            "ano": pl.Int16,
            "num_conc_insc": pl.Int16,
            "num_conc_part": pl.Int16,
            "nota_bruta_fg": pl.Float32,
            "nota_padronizada_fg": pl.Float32,
            "nota_bruta_ce": pl.Float32,
            "nota_padronizada_ce": pl.Float32,
            "conc_enade_cont": pl.Float32,
            "conc_enade_faixa": pl.Int8,
        },
    )

    with pytest.raises(pa.errors.SchemaError):
        contrato_saida.EnadeSaida.validate(df_teste)


def test_num_conc_insc_maior_ou_igual_num_conc_part():
    df_teste = pl.DataFrame(
        {
            "ano": [2017, 2018, 2021],
            "area_avaliacao": ["A", "A", "A"],
            "ies": ["A", "A", "A"],
            "org_acad": ["A", "A", "A"],
            "cat_acad": ["A", "A", "A"],
            "mod_ens": ["A", "A", "A"],
            "municipio_curso": ["A", "A", "A"],
            "sigla_uf": ["A", "A", "A"],
            "num_conc_insc": [10, 3, 4],
            "num_conc_part": [8, 4, 3],
            "nota_bruta_fg": [51.23, 60.44, 48.15],
            "nota_padronizada_fg": [3.48, 4.50, 4.02],
            "nota_bruta_ce": [60.10, 55.22, 34.90],
            "nota_padronizada_ce": [3.70, 4.21, 3.92],
            "conc_enade_cont": [3.40, 4.00, 4.12],
            "conc_enade_faixa": [3, 4, 4],
        },
        schema_overrides={
            "ano": pl.Int16,
            "num_conc_insc": pl.Int16,
            "num_conc_part": pl.Int16,
            "nota_bruta_fg": pl.Float32,
            "nota_padronizada_fg": pl.Float32,
            "nota_bruta_ce": pl.Float32,
            "nota_padronizada_ce": pl.Float32,
            "conc_enade_cont": pl.Float32,
            "conc_enade_faixa": pl.Int8,
        },
    )

    with pytest.raises(pa.errors.SchemaError):
        contrato_saida.EnadeSaida.validate(df_teste)
