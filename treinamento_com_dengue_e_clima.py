import pandas as pd
import numpy as np
import xgboost as xgb
import matplotlib.pyplot as plt
import seaborn as sns

# Configuração visual
sns.set_theme(style="whitegrid")

def rodar_revanche_com_clima():
    print("🥊 Iniciando a Revanche do Modelo (Agora com Clima!)...")
    
    # 1. Carregar Dataset Completo
    df = pd.read_parquet("dataset_ml_completo_com_clima.parquet")
    df = df.sort_values('DT_SEMANA').reset_index(drop=True)
    
    # 2. Separar Treino (Até 2023) e Futuro (2024)
    df_treino = df[df['DT_SEMANA'] < '2024-01-01'].copy()
    df_2024_clima = df[df['DT_SEMANA'] >= '2024-01-01'].copy()
    
    print(f"📚 Treinando com {len(df_treino)} semanas (2019-2023)...")
    
    # 3. Definir Features
    # Removemos DT_SEMANA e o alvo 'casos' da lista de input
    features = [c for c in df.columns if c not in ['DT_SEMANA', 'casos']]
    target = 'casos'
    
    # 4. Treinar XGBoost
    model = xgb.XGBRegressor(
        n_estimators=1000,
        learning_rate=0.01,
        max_depth=6, # Um pouco mais profundo para capturar nuances do clima
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42
    )
    
    model.fit(df_treino[features], df_treino[target])
    
    # 5. O Loop de Previsão Recursiva (Walk-Forward)
    print("🔮 Prevendo 2024 semana a semana...")
    
    # Precisamos do histórico para calcular os lags de CASOS
    # (Os lags de CLIMA já estão prontos no dataframe df_2024_clima, pois baixamos o real)
    historico_casos = list(df_treino['casos'].values)
    previsoes_2024 = []
    
    # Itera sobre cada semana de 2024
    for i, row in df_2024_clima.iterrows():
        # A. Montar a linha de input baseada no que já sabemos (Clima + Calendário)
        input_data = row[features].to_dict()
        
        # B. Atualizar os Lags de CASOS com base nas previsões anteriores (Recursão)
        # Ex: lag_casos_w1 é a previsão da semana passada, não o zero que estava lá
        input_data['lag_casos_w1'] = historico_casos[-1]
        input_data['lag_casos_w2'] = historico_casos[-2]
        input_data['lag_casos_w4'] = historico_casos[-4]
        input_data['lag_casos_w8'] = historico_casos[-8]
        
        # Converter para DataFrame para o XGBoost
        df_input = pd.DataFrame([input_data])
        
        # C. Prever
        pred = model.predict(df_input)[0]
        pred = max(0, pred) # Sem casos negativos
        
        # D. Salvar e Atualizar Histórico
        previsoes_2024.append(pred)
        historico_casos.append(pred) # Adiciona a previsão como "fato" para a próxima semana
        
    # 6. Salvar Resultado
    df_2024_resultado = df_2024_clima[['DT_SEMANA']].copy()
    df_2024_resultado['casos_previstos_ia'] = previsoes_2024
    
    df_2024_resultado.to_parquet("previsao_2024_com_clima.parquet", index=False)
    print("💾 Previsão salva: previsao_2024_com_clima.parquet")
    

    # Feature Importance (Para ver se o clima foi usado)
    plt.figure(figsize=(10, 8))
    xgb.plot_importance(model, max_num_features=15, height=0.5)
    plt.title("O que a IA considerou mais importante? (Com Clima)")
    plt.savefig("feature_importance_clima.png")
    print("📊 Gráfico de Importância salvo.")

if __name__ == "__main__":
    rodar_revanche_com_clima()