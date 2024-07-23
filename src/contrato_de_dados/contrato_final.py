import pandera.polars as pa
import polars as pl

from src.contrato_de_dados import contrato_base


class EnadeFinal(contrato_base.EnadeBase):
    ano: pl.Int16 = pa.Field(ge=2017, le=2021, nullable=False)
    area_avaliacao: pl.String = pa.Field(nullable=False)
    ies: pl.String = pa.Field(nullable=False)
    org_acad: pl.String = pa.Field(nullable=False)
    cat_acad: pl.String = pa.Field(nullable=False)
    mod_ens: pl.String = pa.Field(nullable=False)
    municipio_curso: pl.String = pa.Field(nullable=False)
    sigla_uf: pl.String = pa.Field(nullable=False)
    num_conc_insc: pl.Int16 = pa.Field(ge=1, nullable=False)
    num_conc_part: pl.Int16 = pa.Field(ge=1, nullable=False)
    nota_bruta_fg: pl.Float32 = pa.Field(ge=0, le=100, nullable=False)
    nota_padronizada_fg: pl.Float32 = pa.Field(ge=0, le=5, nullable=False)
    nota_bruta_ce: pl.Float32 = pa.Field(ge=0, le=100, nullable=False)
    nota_padronizada_ce: pl.Float32 = pa.Field(ge=0, le=5, nullable=False)
    conc_enade_cont: pl.Float32 = pa.Field(ge=0, le=5, nullable=False)
    conc_enade_faixa: pl.Int8 = pa.Field(ge=1, le=5, nullable=False)
