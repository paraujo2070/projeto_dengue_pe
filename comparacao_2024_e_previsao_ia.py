import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pysus import SINAN
import pyarrow.parquet as pq
import os
import gc
import glob

# Configuração visual
sns.set_theme(style="whitegrid")

# Códigos da II GERES (Mata Norte + Agreste) - 6 Dígitos
codigos_municipios = [
    "260290", "260410", "260845", "260850", "260950", "261060", "261560", "261640",
    "260190", "260415", "260500", "260540", "260800", "260890", "260900", "260990", 
    "261040", "261230", "261450", "261618"
]

def baixar_e_filtrar_blindado_v2():
    print("🛡️ Iniciando Protocolo V2 (Suporte a Diretórios)...")
    
    # 1. Preparar Pasta Segura
    pasta_download = os.path.join(os.getcwd(), "downloads_2024")
    if not os.path.exists(pasta_download):
        os.makedirs(pasta_download)

    # 2. Localizar Arquivo/Pasta
    sinan = SINAN().load()
    files = sinan.get_files('DENG', year=2024)
    
    if not files:
        print("⚠️ Arquivo de 2024 não encontrado.")
        return None

    print("⬇️ Verificando dados de 2024...")
    try:
        # Tenta baixar (se já existir, o PySUS costuma avisar ou sobrescrever)
        parquet_set = sinan.download(files, local_dir=pasta_download)
    except Exception as e:
        print(f"⚠️ Aviso no download: {e}")

    # 3. IDENTIFICAR O QUE FOI BAIXADO (Arquivo ou Pasta?)
    # Procura por qualquer coisa com 'DENGBR24' no nome dentro da pasta
    caminho_base = os.path.join(pasta_download, "DENGBR24.parquet")
    
    lista_arquivos_para_ler = []

    if os.path.exists(caminho_base):
        if os.path.isdir(caminho_base):
            print(f"📂 Detectado formato DIRETÓRIO: {caminho_base}")
            # Lista todos os parquets dentro da pasta
            arquivos_internos = glob.glob(os.path.join(caminho_base, "*.parquet"))
            lista_arquivos_para_ler.extend(arquivos_internos)
        else:
            print(f"📄 Detectado formato ARQUIVO ÚNICO: {caminho_base}")
            lista_arquivos_para_ler.append(caminho_base)
    else:
        # Fallback: Procura qualquer parquet na pasta downloads se o nome não for exato
        print("🔍 Procurando arquivos alternativos...")
        lista_arquivos_para_ler = glob.glob(os.path.join(pasta_download, "*.parquet"))

    if not lista_arquivos_para_ler:
        print("❌ Nenhum arquivo de dados encontrado.")
        return None

    print(f"📊 Total de arquivos a processar: {len(lista_arquivos_para_ler)}")

    # 4. PROCESSAMENTO EM STREAMING (Loop nos arquivos -> Loop nos chunks)
    lista_dfs = []
    total_linhas = 0
    
    for arq in lista_arquivos_para_ler:
        print(f"   🔨 Processando: {os.path.basename(arq)}...")
        try:
            parquet_file = pq.ParquetFile(arq)
            
            # Batch size seguro
            for batch in parquet_file.iter_batches(batch_size=50000):
                df_chunk = batch.to_pandas()
                
                if 'ID_MN_RESI' in df_chunk.columns:
                    df_chunk['ID_MN_RESI'] = df_chunk['ID_MN_RESI'].astype(str).str.strip()
                    
                    # Filtra Região
                    df_filtrado = df_chunk[df_chunk['ID_MN_RESI'].isin(codigos_municipios)].copy()
                    
                    if not df_filtrado.empty:
                        df_filtrado = df_filtrado[['DT_NOTIFIC', 'ID_MN_RESI']]
                        lista_dfs.append(df_filtrado)
                        total_linhas += len(df_filtrado)
                
                del df_chunk
                
        except Exception as e:
            print(f"   ⚠️ Erro ao ler {arq}: {e}")
            continue
        
        gc.collect()

    # 5. Consolidação
    if lista_dfs:
        print(f"🔗 Consolidando {len(lista_dfs)} fragmentos...")
        df_final = pd.concat(lista_dfs)
        df_final['DT_NOTIFIC'] = pd.to_datetime(df_final['DT_NOTIFIC'])
        
        df_real = df_final.set_index('DT_NOTIFIC').resample('W-SUN').size().reset_index(name='casos_reais')
        print(f"🏆 SUCESSO! Recuperados {total_linhas} casos da sua região.")
        return df_real
    else:
        print("⚠️ Nenhum caso encontrado na região (Verifique se o ano 2024 já tem dados para PE).")
        return None

def gerar_grafico_final():
    # 1. Carregar Previsão
    try:
        df_previsto = pd.read_parquet("previsao_2024_estimada.parquet")
        df_previsto['DT_NOTIFIC'] = pd.to_datetime(df_previsto['DT_NOTIFIC'])
    except:
        print("❌ 'previsao_2024_estimada.parquet' não encontrado.")
        return

    # 2. Baixar Real (V2)
    df_real = baixar_e_filtrar_blindado_v2()
    
    if df_real is None:
        return

    # 3. Visualizar
    plt.figure(figsize=(15, 7))
    plt.plot(df_real['DT_NOTIFIC'], df_real['casos_reais'], label='REAL (2024)', color='black', linewidth=3)
    plt.plot(df_previsto['DT_NOTIFIC'], df_previsto['casos'], label='PREVISÃO IA', color='red', linestyle='--', linewidth=2)
    
    plt.title('Validação Final: Realidade vs Modelo', fontsize=16)
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.savefig("validacao_2024_final.png")
    print("\n📊 Gráfico salvo: validacao_2024_final.png")
    
    # Cálculo final
    total_real = df_real['casos_reais'].sum()
    total_prev = df_previsto['casos'].sum()
    
    print(f"\n📢 CONCLUSÃO:")
    print(f"   Real 2024: {total_real} casos")
    print(f"   Previsto IA: {total_prev:.0f} casos")

if __name__ == "__main__":
    gerar_grafico_final()