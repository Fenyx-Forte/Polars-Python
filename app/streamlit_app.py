import polars as pl
import streamlit as st
from uteis import duckdb_sql


def meu_primeiro_app():
    caminho_query = "sql/query/enade_2021.sql"

    df = duckdb_sql.read_query(caminho_query)

    st.write("Minha primeira tentativa")

    st.dataframe(df)

    # st.line_chart(df, x="Área de Avaliação", y="Conceito Enade Contínuo")

    x = st.slider("x")

    st.write(x, "squared is", x * x)

    if st.checkbox("Show dataframe"):
        chart_data = pl.DataFrame(
            {"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]}
        )

        st.dataframe(chart_data)


if __name__ == "__main__":
    meu_primeiro_app()
