import streamlit as st
import pandas as pd
import mysql.connector

# =====================
# CONFIG
# =====================
st.set_page_config(
    page_title="Vendas e Cancelamentos por Dia",
    layout="wide"
)

# =====================
# CONEXÃO
# =====================

@st.cache_resource
def get_connection():
    return mysql.connector.connect(
        host=st.secrets["mysql"]["host"],
        port=st.secrets["mysql"]["port"],
        database=st.secrets["mysql"]["database"],
        user=st.secrets["mysql"]["user"],
        password=st.secrets["mysql"]["password"]
    )

# =====================
# QUERY VENDAS
# =====================
query_vendas = """
SELECT
ss.accountOwnerId AS 'ACCOUNTOWNERID',
ps.orderNumber AS 'ORDER_NUMBER',
ps.id AS 'COD_PRODUTO',
case when ps.id in ('COM_LINEAR_TOPHD_000001','COM_OFFER_CLAROTV_GLOBOPLAYMAISCANAIS_00001', 'COM_OFFER_CLAROTV_GLOBOPLAYMAISCANAIS_ANUAL_00001')
then 'TOP STREAMING'
when ps.id in ('COM_OFFER_CTV_GLOBOPLAY_NETFLIXANUNCIOS_MAX_APPLETV_00001',
'COM_OFFER_CTV_GLOBOPLAY_NETFLIXANUNCIOS_MAX_APPLETV_ANUAL_00001',
'COM_OFFER_CTV_GLOBOPLAY_NETFLIXPADRAO_MAX_APPLETV_00001',
'COM_OFFER_CTV_GLOBOPLAY_NETFLIXPADRAO_MAX_APPLETV_ANUAL_00001',
'COM_OFFER_CTV_GLOBOPLAY_NETFLIXPREMIUM_MAX_APPLETV_00001',
'COM_OFFER_CTV_GLOBOPLAY_NETFLIXPREMIUM_MAX_APPLETV_ANUAL_00001')
then 'SUPER BUNDLE 4'
when ps.id in (
'COM_OFFER_CLRTV_STREAMINGS_NETANUNCIOS_00001',
'COM_OFFER_CLRTV_STREAMINGS_NETANUNCIOS_ANUAL_00001',
'COM_OFFER_CLRTV_STREAMINGS_NETPADRAO_00001',
'COM_OFFER_CLRTV_STREAMINGS_NETPADRAO_ANUAL_00001',
'COM_OFFER_CLRTV_STREAMINGS_NETPREMIUM_00001',
'COM_OFFER_CLRTV_STREAMINGS_NETPREMIUM_ANUAL_00001')
then 'SUPER BUNDLE 6'
when ps.id in  (
'COM_OFFER_CS_NETANUNCIO_MAX_APPLETV_PRIME_DISNEYANUNCIO_00001',
'COM_OFFER_CS_NETANUNCIO_MAX_APPLETV_PRIME_DISNEYANUNCIO_ANUAL_00001',
'COM_OFFER_CS_NETPADRAO_MAX_APPLETV_PRIME_DISNEYANUNCIO_00001',
'COM_OFFER_CS_NETPADRAO_MAX_APPLETV_PRIME_DISNEYANUNCIO_ANUAL_00001',
'COM_OFFER_CS_NETPREMIUM_MAX_APPLETV_PRIME_DISNEYANUNCIO_00001',
'COM_OFFER_CS_NETPREMIUM_MAX_APPLETV_PRIME_DISNEYANUNCIO_ANUAL_00001')
then 'STREAMINGS' END as PACOTE,
case when ps.id like '%ANUAL%' then 'ANUAL' ELSE 'MENSAL' END 'PLANO',
ss2.purchaseChannel as 'CANAL_VENDA',
ss2.purchaseGateway as 'GATEWAY DE PAGAMENTO',
ss2.purchaseDate AS 'DATA_COMPRA',
year(ss2.purchaseDate) as 'ANO',
month(ss2.purchaseDate) as 'MES',
day(ss2.purchaseDate) as 'DIA',
'VENDA' AS TIPO,
ps.validityEndDate AS 'DATA_CANCELAMENTO',
ps3.description AS 'STATUS_PRODUTO'
from subscriberSchema ss
inner join subscriptionSchema ss2 on ss2.idSubscriber = ss.idSubscriber
inner join productSchema ps on ps.idSubscription = ss2.idSubscription
inner join paymentSchema ps2 on ps2.idPayment = ps.idPayment
inner join productStatus ps3 on ps3.idStatusProduct = ps.idStatusProduct
where
#(ss2.purchaseDate between '2024-11-01 00:00:00' AND '2025-12-18 23:59:59' )
(ss2.purchaseDate between '2024-11-01 00:00:00' AND date(current_date()) )
and (ps.id IN ('COM_LINEAR_TOPHD_000001',
'COM_OFFER_CLAROTV_GLOBOPLAYMAISCANAIS_00001',
'COM_OFFER_CLAROTV_GLOBOPLAYMAISCANAIS_ANUAL_00001',
'COM_OFFER_CTV_GLOBOPLAY_NETFLIXANUNCIOS_MAX_APPLETV_00001',
'COM_OFFER_CTV_GLOBOPLAY_NETFLIXANUNCIOS_MAX_APPLETV_ANUAL_00001',
'COM_OFFER_CTV_GLOBOPLAY_NETFLIXPADRAO_MAX_APPLETV_00001',
'COM_OFFER_CTV_GLOBOPLAY_NETFLIXPADRAO_MAX_APPLETV_ANUAL_00001',
'COM_OFFER_CTV_GLOBOPLAY_NETFLIXPREMIUM_MAX_APPLETV_00001',
'COM_OFFER_CTV_GLOBOPLAY_NETFLIXPREMIUM_MAX_APPLETV_ANUAL_00001'
) or ps.id in (
'COM_OFFER_CLRTV_STREAMINGS_NETANUNCIOS_00001',
'COM_OFFER_CLRTV_STREAMINGS_NETANUNCIOS_ANUAL_00001',
'COM_OFFER_CLRTV_STREAMINGS_NETPADRAO_00001',
'COM_OFFER_CLRTV_STREAMINGS_NETPADRAO_ANUAL_00001',
'COM_OFFER_CLRTV_STREAMINGS_NETPREMIUM_00001',
'COM_OFFER_CLRTV_STREAMINGS_NETPREMIUM_ANUAL_00001',
'COM_OFFER_CS_NETANUNCIO_MAX_APPLETV_PRIME_DISNEYANUNCIO_00001',
'COM_OFFER_CS_NETANUNCIO_MAX_APPLETV_PRIME_DISNEYANUNCIO_ANUAL_00001',
'COM_OFFER_CS_NETPADRAO_MAX_APPLETV_PRIME_DISNEYANUNCIO_00001',
'COM_OFFER_CS_NETPADRAO_MAX_APPLETV_PRIME_DISNEYANUNCIO_ANUAL_00001',
'COM_OFFER_CS_NETPREMIUM_MAX_APPLETV_PRIME_DISNEYANUNCIO_00001',
'COM_OFFER_CS_NETPREMIUM_MAX_APPLETV_PRIME_DISNEYANUNCIO_ANUAL_00001')
)
"""

# =====================
# QUERY CANCELAMENTOS
# =====================
query_cancelamentos = """
with base as (SELECT
ss.accountOwnerId AS 'ACCOUNTOWNERID',
ps.orderNumber AS 'ORDER_NUMBER',
ps.id AS 'COD_PRODUTO',
case when ps.id in ('COM_LINEAR_TOPHD_000001','COM_OFFER_CLAROTV_GLOBOPLAYMAISCANAIS_00001', 'COM_OFFER_CLAROTV_GLOBOPLAYMAISCANAIS_ANUAL_00001')
then 'TOP STREAMING'
when ps.id in ('COM_OFFER_CTV_GLOBOPLAY_NETFLIXANUNCIOS_MAX_APPLETV_00001',
'COM_OFFER_CTV_GLOBOPLAY_NETFLIXANUNCIOS_MAX_APPLETV_ANUAL_00001',
'COM_OFFER_CTV_GLOBOPLAY_NETFLIXPADRAO_MAX_APPLETV_00001',
'COM_OFFER_CTV_GLOBOPLAY_NETFLIXPADRAO_MAX_APPLETV_ANUAL_00001',
'COM_OFFER_CTV_GLOBOPLAY_NETFLIXPREMIUM_MAX_APPLETV_00001',
'COM_OFFER_CTV_GLOBOPLAY_NETFLIXPREMIUM_MAX_APPLETV_ANUAL_00001')
then 'SUPER BUNDLE 4'
when ps.id in (
'COM_OFFER_CLRTV_STREAMINGS_NETANUNCIOS_00001',
'COM_OFFER_CLRTV_STREAMINGS_NETANUNCIOS_ANUAL_00001',
'COM_OFFER_CLRTV_STREAMINGS_NETPADRAO_00001',
'COM_OFFER_CLRTV_STREAMINGS_NETPADRAO_ANUAL_00001',
'COM_OFFER_CLRTV_STREAMINGS_NETPREMIUM_00001',
'COM_OFFER_CLRTV_STREAMINGS_NETPREMIUM_ANUAL_00001')
then 'SUPER BUNDLE 6'
when ps.id in  (
'COM_OFFER_CS_NETANUNCIO_MAX_APPLETV_PRIME_DISNEYANUNCIO_00001',
'COM_OFFER_CS_NETANUNCIO_MAX_APPLETV_PRIME_DISNEYANUNCIO_ANUAL_00001',
'COM_OFFER_CS_NETPADRAO_MAX_APPLETV_PRIME_DISNEYANUNCIO_00001',
'COM_OFFER_CS_NETPADRAO_MAX_APPLETV_PRIME_DISNEYANUNCIO_ANUAL_00001',
'COM_OFFER_CS_NETPREMIUM_MAX_APPLETV_PRIME_DISNEYANUNCIO_00001',
'COM_OFFER_CS_NETPREMIUM_MAX_APPLETV_PRIME_DISNEYANUNCIO_ANUAL_00001')
then 'STREAMINGS' END as PACOTE,
case when ps.id like '%ANUAL%' then 'ANUAL' ELSE 'MENSAL' END 'PLANO',
ss2.purchaseChannel as 'CANAL_VENDA',
ss2.purchaseGateway as 'GATEWAY DE PAGAMENTO',
#ps2.referenceValue AS 'VALOR_COMPRA',
#ps.description AS 'DSC_PRODUTO',
ss2.purchaseDate AS 'DATA_COMPRA',
ps.validityEndDate AS 'DATA_CANCELAMENTO',
year(ps.validityEndDate) as 'ANO',
month(ps.validityEndDate) as 'MES',
day(ps.validityEndDate) as 'DIA',
'CANCELAMENTO' AS TIPO,
ps3.description AS 'STATUS_PRODUTO'
from subscriberSchema ss
inner join subscriptionSchema ss2 on ss2.idSubscriber = ss.idSubscriber
inner join productSchema ps on ps.idSubscription = ss2.idSubscription
inner join paymentSchema ps2 on ps2.idPayment = ps.idPayment
inner join productStatus ps3 on ps3.idStatusProduct = ps.idStatusProduct
where
ps.validityEndDate >= '2024-11-01 00:00:00'
and (ps.id IN ('COM_LINEAR_TOPHD_000001',
'COM_OFFER_CLAROTV_GLOBOPLAYMAISCANAIS_00001',
'COM_OFFER_CLAROTV_GLOBOPLAYMAISCANAIS_ANUAL_00001',
'COM_OFFER_CTV_GLOBOPLAY_NETFLIXANUNCIOS_MAX_APPLETV_00001',
'COM_OFFER_CTV_GLOBOPLAY_NETFLIXANUNCIOS_MAX_APPLETV_ANUAL_00001',
'COM_OFFER_CTV_GLOBOPLAY_NETFLIXPADRAO_MAX_APPLETV_00001',
'COM_OFFER_CTV_GLOBOPLAY_NETFLIXPADRAO_MAX_APPLETV_ANUAL_00001',
'COM_OFFER_CTV_GLOBOPLAY_NETFLIXPREMIUM_MAX_APPLETV_00001',
'COM_OFFER_CTV_GLOBOPLAY_NETFLIXPREMIUM_MAX_APPLETV_ANUAL_00001'
) or ps.id in (
'COM_OFFER_CLRTV_STREAMINGS_NETANUNCIOS_00001',
'COM_OFFER_CLRTV_STREAMINGS_NETPADRAO_00001',
'COM_OFFER_CLRTV_STREAMINGS_NETPREMIUM_00001',
'COM_OFFER_CLRTV_STREAMINGS_NETPREMIUM_ANUAL_00001',
'COM_OFFER_CS_NETANUNCIO_MAX_APPLETV_PRIME_DISNEYANUNCIO_00001',
'COM_OFFER_CS_NETANUNCIO_MAX_APPLETV_PRIME_DISNEYANUNCIO_ANUAL_00001')
)
)
select * from base
where cod_produto<>'COM_LINEAR_TOPHD_000001'
and data_cancelamento BETWEEN '2025-05-01 00:00:00' AND date(current_date()-1)
#and data_cancelamento BETWEEN '2025-05-01 00:00:00' AND '2025-11-29 23:59:59'
union all
select * from base
where data_cancelamento < '2025-05-01 00:00:00'"""

# =====================
# LOAD BASES
# =====================
@st.cache_data(ttl=3600)
def load_base(query):
    conn = get_connection()
    df = pd.read_sql(query, conn)
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

df_vendas = load_base(query_vendas)
df_cancel = load_base(query_cancelamentos)

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




