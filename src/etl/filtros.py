import polars as pl


def filtro_num_inteiro_positivo(nome_coluna: str) -> pl.Expr:
    return pl.col(nome_coluna) >= 1


def filtro_num_entre_0_e_100(nome_coluna: str) -> pl.Expr:
    return pl.col(nome_coluna).is_between(0, 100)


def filtro_num_entre_0_e_5(nome_coluna: str) -> pl.Expr:
    return pl.col(nome_coluna).is_between(0, 5)


def filtro_enade_ano() -> pl.Expr:
    return pl.col("ano").is_in([2017, 2018, 2019, 2021])


def filtro_enade_num_conc_insc() -> pl.Expr:
    return filtro_num_inteiro_positivo("num_conc_insc")


def filtro_enade_num_conc_part() -> pl.Expr:
    return filtro_num_inteiro_positivo("num_conc_part")


def filtro_enade_nota_bruta_fg() -> pl.Expr:
    return filtro_num_entre_0_e_100("nota_bruta_fg")


def filtro_enade_nota_padronizada_fg() -> pl.Expr:
    return filtro_num_entre_0_e_5("nota_padronizada_fg")


def filtro_enade_nota_bruta_ce() -> pl.Expr:
    return filtro_num_entre_0_e_100("nota_bruta_ce")


def filtro_enade_nota_padronizada_ce() -> pl.Expr:
    return filtro_num_entre_0_e_5("nota_padronizada_ce")


def filtro_enade_conc_enade_cont() -> pl.Expr:
    return filtro_num_entre_0_e_5("conc_enade_cont")


def filtro_enade_conc_enade_faixa() -> pl.Expr:
    return pl.col("conc_enade_faixa").is_between(1, 5)


def filtro_enade_num_conc_insc_maior_ou_igual_num_conc_part() -> pl.Expr:
    return pl.col("num_conc_insc").ge(pl.col("num_conc_part"))
