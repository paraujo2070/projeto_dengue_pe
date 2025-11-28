import os
import glob
import matplotlib

# Força o backend não-interativo para evitar erros de janela no Linux
matplotlib.use('Agg') 
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import pandas as pd
import pyarrow.dataset as ds
import locale

# Configuração visual e de idioma
sns.set_theme(style="whitegrid")
try:
    locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')
except:
    print("⚠️ Aviso: Locale PT-BR não disponível no sistema. Datas podem ficar em inglês.")

# Códigos da II GERES (Mata Norte + Agreste)
codigos_municipios = [
    "260290", "260410", "260845", "260850", "260950", "261060", "261560", "261640",
    "260190", "260415", "260500", "260540", "260800", "260890", "260900", "260990", 
    "261040", "261230", "261450", "261618"
]

def encontrar_caminho_dados():
    """Caça o arquivo ou pasta do DENGBR24 onde quer que ele esteja."""
    print("🔍 A procurar dados de 2024...")
    
    # Lista de locais prováveis
    locais = [
        "downloads_2024/DENGBR24.parquet",
        "downloads_2024/*.parquet",
        "DENGBR24.parquet",
        "*.parquet"
    ]
    
    for padrao in locais:
        # Busca recursiva simples
        candidatos = glob.glob(os.path.join(os.getcwd(), padrao))
        if not candidatos:
            continue
            
        for c in candidatos:
            # Se for pasta, verifica se tem parquets dentro
            if os.path.isdir(c):
                conteudo = glob.glob(os.path.join(c, "*.parquet"))
                if conteudo:
                    print(f"   📂 Encontrado Dataset (Pasta): {c}")
                    return c # Retorna o caminho da pasta
            # Se for arquivo
            elif os.path.isfile(c) and c.endswith('.parquet'):
                print(f"   📄 Encontrado Dataset (Arquivo): {c}")
                return c
                
    print("❌ NENHUM DADO ENCONTRADO. Por favor, rode 'validacao_final_v2.py' novamente.")
    return None

def carregar_real_2024_blindado():
    caminho = encontrar_caminho_dados()
    if not caminho:
        return None

    print(f"🚀 Iniciando leitura inteligente via PyArrow Dataset...")
    lista_dfs = []
    
    try:
        dataset = ds.dataset(caminho, format="parquet")
        
        # Iterar em batches (lotes) para não estourar a memória (8GB)
        # batch_size limita quantas linhas vem por vez
        scanner = dataset.scanner(batch_size=50000)
        
        for batch in scanner.to_batches():
            df_chunk = batch.to_pandas()
            
            # Padronização e Filtro
            if 'ID_MN_RESI' in df_chunk.columns:
                df_chunk['ID_MN_RESI'] = df_chunk['ID_MN_RESI'].astype(str).str.strip()
                
                # Filtra apenas a região II GERES
                df_filtrado = df_chunk[df_chunk['ID_MN_RESI'].isin(codigos_municipios)].copy()
                
                if not df_filtrado.empty:
                    lista_dfs.append(df_filtrado[['DT_NOTIFIC']])
            
            # Libera memória imediatamente
            del df_chunk

    except Exception as e:
        print(f"❌ Erro crítico na leitura: {e}")
        return None

    if lista_dfs:
        print(f"   ✅ Processamento concluído. Consolidando...")
        df_final = pd.concat(lista_dfs)
        df_final['DT_NOTIFIC'] = pd.to_datetime(df_final['DT_NOTIFIC'])
        
        # Agrupa por Semana
        df_real = df_final.set_index('DT_NOTIFIC').resample('W-SUN').size().reset_index(name='casos_real')
        return df_real
    else:
        print("⚠️ Dados lidos, mas nenhum caso de Pernambuco (II GERES) encontrado.")
        return None

def gerar_confronto_final():
    # 1. Carregar Previsões (IA)
    try:
        df_v1 = pd.read_parquet("previsao_2024_estimada.parquet") # Sem Clima
        df_v2 = pd.read_parquet("previsao_2024_com_clima.parquet") # Com Clima
        
        # Prepara colunas
        df_v1 = df_v1[['DT_NOTIFIC', 'casos']].rename(columns={'casos': 'casos_v1_sem_clima'})
        df_v2 = df_v2[['DT_SEMANA', 'casos_previstos_ia']].rename(columns={'DT_SEMANA': 'DT_NOTIFIC', 'casos_previstos_ia': 'casos_v2_com_clima'})
        
        # Garante datetime
        df_v1['DT_NOTIFIC'] = pd.to_datetime(df_v1['DT_NOTIFIC'])
        df_v2['DT_NOTIFIC'] = pd.to_datetime(df_v2['DT_NOTIFIC'])
        
    except FileNotFoundError:
        print("❌ Arquivos de previsão não encontrados. Rode os scripts de treino primeiro.")
        return

    # 2. Carregar Real (Blindado)
    df_real = carregar_real_2024_blindado()
    if df_real is None:
        return

    # 3. Merge e Filtro 2024
    print("🔗 Criando gráfico final...")
    df_master = pd.merge(df_real, df_v1, on='DT_NOTIFIC', how='left')
    df_master = pd.merge(df_master, df_v2, on='DT_NOTIFIC', how='left')
    
    # ZOOM EM 2024
    df_master = df_master[(df_master['DT_NOTIFIC'] >= '2024-01-01') & (df_master['DT_NOTIFIC'] <= '2024-12-31')]
    df_master = df_master.sort_values('DT_NOTIFIC')

    # 4. Plotagem
    plt.figure(figsize=(16, 8))
    
    # Realidade (Preto)
    plt.plot(df_master['DT_NOTIFIC'], df_master['casos_real'], 
             label='REALIDADE (SINAN 2024)', color='black', linewidth=3)
    
    # Modelo 1 (Tracejado Vermelho)
    plt.plot(df_master['DT_NOTIFIC'], df_master['casos_v1_sem_clima'], 
             label='IA V1 (Sem Clima)', color='red', linestyle='--', linewidth=2, alpha=0.7)
    
    # Modelo 2 (Azul Sólido)
    plt.plot(df_master['DT_NOTIFIC'], df_master['casos_v2_com_clima'], 
             label='IA V2 (Com Clima)', color='blue', linestyle='-', linewidth=3)
    
    plt.title('Casos de Dengue II regional de saúde de Pernambuco 2024', fontsize=18)
    plt.ylabel('Novos Casos Semanais')
    
    # Eixo X com Meses em Português
    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b')) # Jan, Fev, Mar...
    plt.xlabel('Evolução em 2024', fontsize=12)
    
    plt.legend(fontsize=12)
    plt.grid(True, alpha=0.3)
    
    # Salvar
    plt.savefig("confronto_final_modelos.png")
    print("\n✅ GRÁFICO SALVO COM SUCESSO: confronto_final_modelos.png")
    print("   Abra este arquivo para ver o resultado final do seu portfólio!")

if __name__ == "__main__":
    gerar_confronto_final()