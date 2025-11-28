import streamlit as st
import pandas as pd
import plotly.express as px
import os
from PIL import Image

# Configuração da Página
st.set_page_config(
    page_title="Dengue Radar AI | II GERES",
    page_icon="🦟",
    layout="wide"
)

# --- FUNÇÕES DE CARREGAMENTO ---
@st.cache_data
def carregar_dados_historicos():
    try:
        df = pd.read_parquet("dataset_dengue_II_GERES.parquet")
        df['DT_NOTIFIC'] = pd.to_datetime(df['DT_NOTIFIC'])
        return df
    except FileNotFoundError:
        return None

@st.cache_data
def carregar_previsoes_2024():
    """Carrega os dados gerados pelos modelos para calcular KPIs"""
    dados = {}
    try:
        # Modelo V1 (Sem Clima)
        if os.path.exists("previsao_2024_estimada.parquet"):
            df_v1 = pd.read_parquet("previsao_2024_estimada.parquet")
            dados['v1'] = df_v1['casos'].sum()
        
        # Modelo V2 (Com Clima)
        if os.path.exists("previsao_2024_com_clima.parquet"):
            df_v2 = pd.read_parquet("previsao_2024_com_clima.parquet")
            # Ajuste de nome de coluna dependendo do script que gerou
            col_casos = 'casos_previstos_ia' if 'casos_previstos_ia' in df_v2.columns else 'casos'
            dados['v2'] = df_v2[col_casos].sum()
            
    except Exception as e:
        st.warning(f"Não foi possível carregar métricas de 2024: {e}")
    return dados

def carregar_imagem(nome_arquivo):
    if os.path.exists(nome_arquivo):
        return Image.open(nome_arquivo)
    return None

# --- HEADER ---
st.title("🦟 Dengue Radar AI: Monitoramento e Previsão (II GERES - PE)")
st.markdown("""
**Desenvolvido por Pedro Araújo** | *Engenharia de Dados & Machine Learning End-to-End*

Painel de inteligência epidemiológica que une dados do SINAN e variáveis climáticas (Open-Meteo) 
para prever cenários de risco de arboviroses na Zona da Mata Norte e Agreste.
""")

df = carregar_dados_historicos()

if df is None:
    st.error("⚠️ Arquivo de dados históricos não encontrado.")
    st.stop()

# --- SIDEBAR ---
with st.sidebar:
    st.header("⚙️ Filtros")
    cidades = sorted(df['ID_MN_RESI'].unique())
    cidade_selecionada = st.selectbox("Município", ["Todos (Visão Regional)"] + list(cidades))
    
    st.markdown("---")
    st.info("""
    **Modelagem Híbrida:**
    * **V1:** Histórico Puro (XGBoost Autoregressivo)
    * **V2:** Histórico + Clima (Chuva/Temp com Lags Biológicos)
    """)

if cidade_selecionada != "Todos (Visão Regional)":
    df_filtrado = df[df['ID_MN_RESI'] == cidade_selecionada]
else:
    df_filtrado = df

# Agrupamento Semanal
df_semanal = df_filtrado.set_index('DT_NOTIFIC').resample('W').size().reset_index(name='casos')

# --- KPIs GERAIS ---
col1, col2, col3, col4 = st.columns(4)
total_casos = df_filtrado.shape[0]
pico_semanal = df_semanal['casos'].max()
data_pico = df_semanal.loc[df_semanal['casos'].idxmax(), 'DT_NOTIFIC'].strftime('%d/%m/%Y')
media_semanal = df_semanal['casos'].mean()

col1.metric("Total Notificações (19-23)", f"{total_casos:,.0f}".replace(",", "."))
col2.metric("Pior Semana Histórica", f"{pico_semanal}")
col3.metric("Data do Pico Histórico", data_pico)
col4.metric("Média Semanal", f"{media_semanal:.1f}")

st.markdown("---")

# --- ABAS ---
tab1, tab2, tab3 = st.tabs(["📊 Monitoramento", "🧠 A Mente da IA", "🔮 Validação & Impacto (2024)"])

# ABA 1: HISTÓRICO
with tab1:
    st.subheader("Curva Epidemiológica Histórica (2019-2023)")
    fig = px.line(df_semanal, x='DT_NOTIFIC', y='casos', markers=True)
    fig.update_traces(line_color='#8B0000', line_width=2)
    fig.update_layout(xaxis_title="Data", yaxis_title="Casos", hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

# ABA 2: FEATURE IMPORTANCE
with tab2:
    st.subheader("O que impulsiona a epidemia?")
    col_feat1, col_feat2 = st.columns([2, 1])
    
    with col_feat1:
        # Tenta carregar a imagem específica do modelo com clima
        img_feat = carregar_imagem("feature_importance_clima.png")
        if not img_feat:
            img_feat = carregar_imagem("feature_importance.png") # Fallback
            
        if img_feat:
            st.image(img_feat, caption="Peso das Variáveis na Decisão do Modelo", use_container_width=True)
        else:
            st.warning("Gráfico de importância não encontrado.")
            
    with col_feat2:
        st.info("""
        **Descobertas do Modelo V2:**
        
        🌡️ **Temperatura Máxima:** Foi o preditor nº 1. O calor extremo acelera o ciclo de vida do mosquito.
        
        📉 **Inércia (Lags):** O número de casos da semana passada continua sendo um forte indicador.
        
        🌧️ **Chuva Acumulada:** Fundamental para formação de criadouros, aparecendo com forte relevância nos lags de 3 a 4 semanas.
        """)

# ABA 3: O GRANDE FINAL (2024)
with tab3:
    st.subheader(" O Confronto Final: Realidade vs Potencial Biológico (2024)")
    
    st.markdown("""
    Comparativo entre o que aconteceu (Realidade), o que a tendência histórica dizia (IA V1) 
    e o **risco biológico real** impulsionado pelo El Niño (IA V2 com Clima).
    """)
    
    # Carregar Imagem Final
    img_confronto = carregar_imagem("confronto_final_modelos.png")
    
    if img_confronto:
        st.image(img_confronto, use_container_width=True, caption="Gráfico gerado pelo script 'comparativo_final_clima.py'")
    else:
        st.error("Imagem 'confronto_final_modelos.png' não encontrada. Rode o script de comparação.")

    st.markdown("---")
    
    # KPIs de 2024
    metricas = carregar_previsoes_2024()
    
    kpi1, kpi2, kpi3 = st.columns(3)
    
    # Se tivermos os dados carregados, mostramos. Senão, mostramos texto explicativo.
    if 'v1' in metricas and 'v2' in metricas:
        kpi2.metric("Previsão IA V1 (Só Histórico)", f"{metricas['v1']:.0f}", delta="Base Conservadora")
        kpi3.metric("Previsão IA V2 (Com Clima)", f"{metricas['v2']:.0f}", delta="Alto Risco Biológico", delta_color="off")
    
    st.success("""
    ### 🩺 Diagnóstico de Negócio: O "Delta da Eficiência"
    
    O gráfico revela uma história fascinante de Gestão Pública:
    
    1.  🔵 **A Linha Azul (IA com Clima)** mostra o **Potencial do Surto**. Com o calor e chuva de 2024, biologicamente, poderíamos ter tido um cenário catastrófico (~200 casos/semana no pico).
    2.  ⚫ **A Linha Preta (Realidade)** mostra que o surto começou a seguir a previsão climática, mas foi **"cortado"** bruscamente em Abril/Maio.
    
    **Conclusão:** A diferença entre a linha Azul (O que o clima permitia) e a Preta (O que ocorreu) representa o impacto das **Ações de Controle (ACE/Fumacê)** e a possível **Imunidade de Rebanho**. O modelo V2 serve, portanto, como um alerta de "Risco Máximo" para mobilizar recursos preventivos.
    
    *Nota Técnica: A subnotificação pós-pico (final de 2024) também contribui para o descolamento das curvas.*
    """)

# --- RODAPÉ ---
st.markdown("---")
st.caption("Portfólio de Data Science | Pedro Araújo | Dados: SINAN (MS) & Open-Meteo")