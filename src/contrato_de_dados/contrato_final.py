from functools import partial
import pandera.polars as pa
import polars as pl

from src.contrato_de_dados import contrato_base

campo_string_padrao = partial(pa.Field, nullable=False)
campo_float_entre_0_e_100 = partial(pa.Field, nullable=False, ge=0, le=100)
campo_float_entre_0_e_5 = partial(pa.Field, nullable=False, ge=0, le=5)


class EnadeFinal(contrato_base.EnadeBase):
    ano: pl.Int16 = pa.Field(isin=[2017, 2018, 2019, 2021], nullable=False)
    area_avaliacao: pl.String = campo_string_padrao()
    ies: pl.String = campo_string_padrao()
    org_acad: pl.String = campo_string_padrao()
    cat_acad: pl.String = campo_string_padrao()
    mod_ens: pl.String = campo_string_padrao()
    municipio_curso: pl.String = campo_string_padrao()
    sigla_uf: pl.String = campo_string_padrao()
    num_conc_insc: pl.Int16 = pa.Field(ge=1, nullable=False)
    num_conc_part: pl.Int16 = pa.Field(ge=1, nullable=False)
    nota_bruta_fg: pl.Float32 = campo_float_entre_0_e_100()
    nota_padronizada_fg: pl.Float32 = campo_float_entre_0_e_5()
    nota_bruta_ce: pl.Float32 = campo_float_entre_0_e_100()
    nota_padronizada_ce: pl.Float32 = campo_float_entre_0_e_5()
    conc_enade_cont: pl.Float32 = campo_float_entre_0_e_5()
    conc_enade_faixa: pl.Int8 = pa.Field(ge=1, le=5, nullable=False)
