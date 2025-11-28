import pandas as pd
import numpy as np

def processar_merge_final():
    print("🔄 Iniciando Fusão de Dados (Dengue + Clima)...")

    # 1. Carregar Dados Brutos
    try:
        df_dengue = pd.read_parquet("dataset_dengue_II_GERES.parquet")
        df_clima = pd.read_parquet("dados_climaticos_regional_detalhado.parquet")
    except FileNotFoundError as e:
        print(f"❌ Erro: Arquivo não encontrado ({e}). Rode os scripts de coleta anteriores.")
        return

    # 2. Tratamento do Clima (Agregação Regional e Semanal)
    print("   ⛈️ Processando dados climáticos...")
    df_clima['date'] = pd.to_datetime(df_clima['date'])
    
    # Agrupamos por Data primeiro (Média de todas as cidades da região naquele dia)
    # Isso cria um "Clima Médio da II GERES"
    df_clima_regional_diario = df_clima.groupby('date').agg({
        'temp_max': 'mean',
        'temp_min': 'mean',
        'temp_media': 'mean',
        'chuva_mm': 'mean', # Média de chuva na região (se somar tudo, fica gigante)
        'umidade': 'mean'
    }).reset_index()

    # Agora agrupamos por SEMANA (Para bater com a Dengue)
    df_clima_semanal = df_clima_regional_diario.set_index('date').resample('W-SUN').agg({
        'temp_max': 'max',       # Máxima da semana
        'temp_min': 'min',       # Mínima da semana
        'temp_media': 'mean',    # Média da semana
        'chuva_mm': 'sum',       # Chuva ACUMULADA na semana (importante!)
        'umidade': 'mean'
    }).reset_index()
    
    df_clima_semanal.rename(columns={'date': 'DT_SEMANA'}, inplace=True)

    # 3. Tratamento da Dengue (Agregação Semanal)
    print("   🦟 Processando dados de Dengue...")
    df_dengue['DT_NOTIFIC'] = pd.to_datetime(df_dengue['DT_NOTIFIC'])
    
    # Conta casos por semana na região toda
    df_dengue_semanal = df_dengue.set_index('DT_NOTIFIC').resample('W-SUN').size().reset_index(name='casos')
    df_dengue_semanal.rename(columns={'DT_NOTIFIC': 'DT_SEMANA'}, inplace=True)

    # 4. O Grande Merge (Left Join para manter datas da Dengue ou Outer para tudo)
    # Usaremos Outer para garantir que temos clima mesmo em semanas sem dengue (zero casos)
    print("   🔗 Unificando bases...")
    df_final = pd.merge(df_clima_semanal, df_dengue_semanal, on='DT_SEMANA', how='outer')
    
    # Preencher vazios (Semanas sem notificação = 0 casos)
    df_final['casos'] = df_final['casos'].fillna(0)
    
    # Filtrar período de interesse (2019 a 2024)
    df_final = df_final[(df_final['DT_SEMANA'] >= '2019-01-01') & (df_final['DT_SEMANA'] <= '2024-12-31')].copy()
    df_final = df_final.sort_values('DT_SEMANA').reset_index(drop=True)

    # 5. Engenharia de Features (Recriar Lags + Features Climáticas)
    print("   🧠 Criando Inteligência (Features)...")
    
    # Sazonalidade
    df_final['semana_do_ano'] = df_final['DT_SEMANA'].dt.isocalendar().week.astype(int)
    df_final['semana_sin'] = np.sin(2 * np.pi * df_final['semana_do_ano'] / 53)
    df_final['semana_cos'] = np.cos(2 * np.pi * df_final['semana_do_ano'] / 53)

    # Lags de Dengue (Autoregressivo)
    for lag in [1, 2, 4, 8]:
        df_final[f'lag_casos_w{lag}'] = df_final['casos'].shift(lag)

    # Lags CLIMÁTICOS (O Segredo!) 
    # O mosquito demora ~2 a 4 semanas para nascer após a chuva.
    # A chuva de hoje não causa dengue hoje. Causa dengue mês que vem.
    for lag in [2, 3, 4, 8]: 
        df_final[f'lag_chuva_w{lag}'] = df_final['chuva_mm'].shift(lag)
        df_final[f'lag_temp_w{lag}'] = df_final['temp_media'].shift(lag)
        df_final[f'lag_umid_w{lag}'] = df_final['umidade'].shift(lag)

    # Remover linhas vazias geradas pelos lags
    df_ml = df_final.dropna().reset_index(drop=True)

    # Salvar
    arquivo_saida = "dataset_ml_completo_com_clima.parquet"
    df_ml.to_parquet(arquivo_saida, index=False)
    
    print(f"\n✅ SUCESSO! Dataset final pronto para treino: {arquivo_saida}")
    print(f"📊 Colunas geradas: {len(df_ml.columns)}")
    print("   Novas variáveis: lag_chuva_wX, lag_temp_wX...")
    print(df_ml[['DT_SEMANA', 'casos', 'chuva_mm', 'lag_chuva_w2']].tail())

if __name__ == "__main__":
    processar_merge_final()