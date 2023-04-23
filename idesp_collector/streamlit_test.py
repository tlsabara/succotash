import streamlit as st
import pandas as pd
import sqlalchemy as alch
from pathlib import Path
import matplotlib
import os
import seaborn as sns


st.write(
    """
    # Analise sobre os dados do IDESP
    
    Dados coletados em abril/2022 - por Thiago Sabará
    
    Pedro seu zé ruela

    """
)


ROOT_DIR = Path(os.path.abspath(os.path.curdir)).parent
src_data = ROOT_DIR / 'idesp_collector' / 'arquivo_scrap.csv'
df = pd.read_csv(src_data, low_memory=False)

df_dados_ausentes = df.isna().mean()
df_descibe = df.describe()

st.table(df_dados_ausentes)
st.table(df_descibe)
st.bar_chart(df_dados_ausentes)

