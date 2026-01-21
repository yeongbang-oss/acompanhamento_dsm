import streamlit as st
import pandas as pd
from google.oauth2.service_account import Credentials
import gspread

# =====================
# CONFIG
# =====================
st.set_page_config(
    page_title="Vendas e Cancelamentos por Dia",
    layout="wide"
)

# =====================
# GOOGLE SHEETS CONEXÃO
# =====================
@st.cache_resource
def get_gsheet_client():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=scope
    )

    return gspread.authorize(creds)

# =====================
# LOAD BASE (CACHE)
# =====================
@st.cache_data(ttl=3600)
def load_base(worksheet):
    gc = get_gsheet_client()

    # 🔹 Abre planilha pelo NOME ou URL
    sh = gc.open("Atualização_DSM")

    # 🔹 Aba
    ws = sh.worksheet(worksheet)
    df = pd.DataFrame(wd.get_all_records())

    # Padroniza nomes
    df.columns = df_vendas.columns.str.upper()

    return df

# =====================
# BUILD TABELA
# =====================
ORDEM_PACOTES = [
    "SUPER BUNDLE 6",
    "STREAMINGS",
    "TOP STREAMING",
    "SUPER BUNDLE 4"
]

def build_tabela(df, ano, mes, planos, canal_venda, gateway_pagamento):
    df = df[
        (df["ANO"] == ano) &
        (df["MES"] == mes) &
        (df["PLANO"].isin(planos)) &
	 (df["CANAL_VENDA"].isin(canal_venda)) &
	 (df["GATEWAY DE PAGAMENTO"].isin(gateway_pagamento))
    ]

    tabela = (
        df.pivot_table(
            index="DIA",
            columns="PACOTE",
            values="ORDER_NUMBER",
            aggfunc="nunique",
            fill_value=0
        )
        .reindex(columns=ORDEM_PACOTES, fill_value=0)
        .sort_index()
    )

    tabela["Total"] = tabela.sum(axis=1)
    return tabela.reset_index()

# =====================
# ESTILO
# =====================
def destacar_total(df):
    return df.style.apply(
        lambda _: ["background-color: #ffe6e6"] * len(df),
        subset=["Total"]
    )

# =====================
# APP
# =====================
st.title("📊 Vendas x Cancelamentos por Dia")

df_vendas = load_base('base_vendas')
df_cancel = load_base('base_cancelamento')

# -------- FILTROS --------
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    ano = st.selectbox("Ano", sorted(df_vendas["ANO"].unique(), reverse=True))

with col2:
    mes = st.selectbox("Mês", sorted(df_vendas[df_vendas["ANO"] == ano]["MES"].unique()))

with col3:
    planos = st.multiselect(
        "Plano",
        sorted(df_vendas["PLANO"].unique()),
        default=sorted(df_vendas["PLANO"].unique())
    )

with col4:
    canal_venda = st.multiselect(
        "Canal de Venda",
        sorted(df_vendas["CANAL_VENDA"].unique()),
        default=sorted(df_vendas["CANAL_VENDA"].unique())
    )

with col5:
    gateway_pagamento = st.multiselect(
        "Gateway de Pagamento",
        sorted(df_vendas["GATEWAY DE PAGAMENTO"].unique()),
        default=sorted(df_vendas["GATEWAY DE PAGAMENTO"].unique())
    )



# -------- TABELAS --------
col_v, col_c = st.columns(2)

with col_v:
    st.subheader("📈 Vendas")
    tab_v = build_tabela(df_vendas, ano, mes, planos, canal_venda, gateway_pagamento)
    st.dataframe(destacar_total(tab_v), use_container_width=True, hide_index=True)

with col_c:
    st.subheader("📉 Cancelamentos")
    tab_c = build_tabela(df_cancel, ano, mes, planos, canal_venda, gateway_pagamento)
    st.dataframe(destacar_total(tab_c), use_container_width=True, hide_index=True)






