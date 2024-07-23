import pandera.polars as pa
import polars as pl

from src.contrato_de_dados import contrato_base


class EnadeFinal(contrato_base.EnadeBase):
    conc_enade_faixa: pl.Int8 = pa.Field(ge=1, le=5)
