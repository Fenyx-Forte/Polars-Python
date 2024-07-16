import logging

import duckdb
import polars as pl

from src.utils import carregar_env, my_log

logger = logging.getLogger("duckdb_sql")


def read_query_content(query_path: str) -> str:
    query = ""
    with open(query_path, "r") as file:
        query = file.read()

    return query


def read_query(query_path: str) -> pl.DataFrame:
    query_content = read_query_content(query_path)

    pl_df = duckdb.sql(query_content).pl()

    return pl_df


def salvar_tabela_no_postgresql(path_file: str, table_name: str) -> None:
    con = duckdb.connect()

    string_conexao_postgresql = """
        PREPARE conexao_query AS
            INSTALL postgres;
            LOAD postgres;
            ATTACH "
                dbname=$DB
                host=$HOST
                port=$PORT
                user=$USER
                password=$PASSWORD$
            " AS db (TYPE postgres);
    """

    con_params = {
        "DB": carregar_env.POSTGRES_DB,
        "HOST": carregar_env.POSTGRES_HOST,
        "PORT": carregar_env.POSTGRES_HOST,
        "USER": carregar_env.POSTGRES_USER,
        "PASSWORD": carregar_env.POSTGRES_PASSWORD,
    }

    con.sql(string_conexao_postgresql, params=con_params)

    string_query = """
        INSERT INTO $table_name (
                ano
            ,   area_avaliacao
            ,   ies
            ,   org_acad
            ,   cat_acad
            ,   mod_ens
            ,   municipio_curso
            ,   sigla_uf
            ,   num_conc_insc
            ,   num_conc_part
            ,   nota_bruta_fg
            ,   nota_padronizada_fg
            ,   nota_bruta_ce
            ,   conc_enade_cont
            ,   conc_enade_faixa
        )
        SELECT
            *
        FROM
            "$path_file"
    """

    query_params = {"table_name": table_name, "path_file": path_file}

    con.sql(string_query, params=query_params)

    print("Fim")
