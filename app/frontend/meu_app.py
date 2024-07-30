import polars as pl
import streamlit as st
from uteis import duckdb_sql


def meu_primeiro_app():
    caminho_query = "sql/query/enade_2021.sql"

    df = duckdb_sql.read_query(caminho_query)

    st.write("Minha primeira tentativa")

    st.write(df)

    st.write("Deu certo")
