import pandera.polars as pa
import polars as pl


class EnadeBase(pa.DataFrameModel):
    ano: pl.Int64 = pa.Field(ge=2017, le=2021, nullable=True)
    area_avaliacao: pl.String = pa.Field(nullable=True)
    ies: pl.String = pa.Field(nullable=True)
    org_acad: pl.String = pa.Field(nullable=True)
    cat_acad: pl.String = pa.Field(nullable=True)
    mod_ens: pl.String = pa.Field(nullable=True)
    municipio_curso: pl.String = pa.Field(nullable=True)
    sigla_uf: pl.String = pa.Field(nullable=True)
    num_conc_insc: pl.Int64 = pa.Field(ge=1, nullable=True)
    num_conc_part: pl.Int64 = pa.Field(ge=0, nullable=True)
    nota_bruta_fg: pl.Float64 = pa.Field(ge=0, le=100, nullable=True)
    nota_padronizada_fg: pl.Float64 = pa.Field(ge=0, le=5, nullable=True)
    nota_bruta_ce: pl.Float64 = pa.Field(ge=0, le=100, nullable=True)
    nota_padronizada_ce: pl.Float64 = pa.Field(ge=0, le=5, nullable=True)
    conc_enade_cont: pl.Float64 = pa.Field(ge=0, le=5, nullable=True)
    conc_enade_faixa: pl.String = pa.Field(nullable=True)

    class Config:
        strict = True
        # coerce = True
        # drop_invalid_rows = True
