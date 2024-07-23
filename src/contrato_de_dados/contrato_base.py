import pandera.polars as pa
import polars as pl


class EnadeBase(pa.DataFrameModel):
    ano: pl.Int64 = pa.Field(ge=2017, le=2021)
    area_avaliacao: pl.String
    ies: pl.String
    org_acad: pl.String
    cat_acad: pl.String
    mod_ens: pl.String
    municipio_curso: pl.String
    sigla_uf: pl.String
    num_conc_insc: pl.Int64 = pa.Field(ge=1)
    num_conc_part: pl.Int64 = pa.Field(ge=1)
    nota_bruta_fg: pl.Float64 = pa.Field(ge=0, le=100)
    nota_padronizada_fg: pl.Float64 = pa.Field(ge=0, le=5)
    nota_bruta_ce: pl.Float64 = pa.Field(ge=0, le=100)
    nota_padronizada_ce: pl.Float64 = pa.Field(ge=0, le=5)
    conc_enade_cont: pl.Float64 = pa.Field(ge=0, le=5)
    conc_enade_faixa: pl.String

    class Config:
        strict = True
        coerce = True
        # drop_invalid_rows = True
