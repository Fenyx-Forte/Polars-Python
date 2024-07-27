from functools import partial

import pandera.polars as pa
import polars as pl

campo_string_padrao = partial(pa.Field, nullable=True)
campo_float_entre_0_e_100 = partial(pa.Field, nullable=True, ge=0, le=100)
campo_float_entre_0_e_5 = partial(pa.Field, nullable=True, ge=0, le=5)


class EnadeEntrada(pa.DataFrameModel):
    ano: pl.Int64 = pa.Field(isin=[2017, 2018, 2019, 2021], nullable=True)
    area_avaliacao: pl.String = campo_string_padrao()
    ies: pl.String = campo_string_padrao()
    org_acad: pl.String = campo_string_padrao()
    cat_acad: pl.String = campo_string_padrao()
    mod_ens: pl.String = campo_string_padrao()
    municipio_curso: pl.String = campo_string_padrao()
    sigla_uf: pl.String = campo_string_padrao()
    num_conc_insc: pl.Int64 = pa.Field(ge=1, nullable=True)
    num_conc_part: pl.Int64 = pa.Field(ge=0, nullable=True)
    nota_bruta_fg: pl.Float64 = campo_float_entre_0_e_100()
    nota_padronizada_fg: pl.Float64 = campo_float_entre_0_e_5()
    nota_bruta_ce: pl.Float64 = campo_float_entre_0_e_100()
    nota_padronizada_ce: pl.Float64 = campo_float_entre_0_e_5()
    conc_enade_cont: pl.Float64 = campo_float_entre_0_e_5()
    conc_enade_faixa: pl.String = pa.Field(nullable=True)

    class Config:
        strict = True
        # coerce = True
        # drop_invalid_rows = True
