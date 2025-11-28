import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import time

def coletar_clima_regional():
    print("🌤️ Iniciando Coleta Climática de Precisão (Por Município)...")

    # 1. Configurar Cliente API com Cache
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # 2. Dicionário de Coordenadas (II GERES - PE)
    # Fonte: IBGE / Google Maps
    municipios = {
        "260290": {"nome": "Buenos Aires", "lat": -7.7258, "lon": -35.3122},
        "260410": {"nome": "Carpina", "lat": -7.8502, "lon": -35.2474},
        "260845": {"nome": "Lagoa do Carro", "lat": -7.7569, "lon": -35.3217},
        "260850": {"nome": "Lagoa de Itaenga", "lat": -7.9352, "lon": -35.2902},
        "260950": {"nome": "Nazaré da Mata", "lat": -7.7431, "lon": -35.2217},
        "261060": {"nome": "Paudalho", "lat": -7.9011, "lon": -35.1708},
        "261560": {"nome": "Tracunhaém", "lat": -7.8033, "lon": -35.2325},
        "261640": {"nome": "Vicência", "lat": -7.6575, "lon": -35.3275},
        "260190": {"nome": "Bom Jardim", "lat": -7.7958, "lon": -35.5869},
        "260415": {"nome": "Casinhas", "lat": -7.9258, "lon": -35.7172},
        "260500": {"nome": "Cumaru", "lat": -8.0055, "lon": -35.6989},
        "260540": {"nome": "Feira Nova", "lat": -7.9511, "lon": -35.3889},
        "260800": {"nome": "João Alfredo", "lat": -7.8558, "lon": -35.5889},
        "260890": {"nome": "Limoeiro", "lat": -7.8742, "lon": -35.4519},
        "260900": {"nome": "Machados", "lat": -7.6750, "lon": -35.5233},
        "260990": {"nome": "Orobó", "lat": -7.7458, "lon": -35.6022},
        "261040": {"nome": "Passira", "lat": -7.9422, "lon": -35.5819},
        "261230": {"nome": "Salgadinho", "lat": -7.9372, "lon": -35.6358},
        "261450": {"nome": "Surubim", "lat": -7.8336, "lon": -35.7533},
        "261618": {"nome": "Vertente do Lério", "lat": -7.7803, "lon": -35.7336}
    }

    url = "https://archive-api.open-meteo.com/v1/archive"
    
    lista_dados = []

    # 3. Loop de Coleta
    for codigo_ibge, coords in municipios.items():
        print(f"   📍 Baixando: {coords['nome']} ({codigo_ibge})...")
        
        params = {
            "latitude": coords['lat'],
            "longitude": coords['lon'],
            "start_date": "2019-01-01",
            "end_date": "2024-12-31", # Pega até o final de 2024 para bater com a validação
            "daily": ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean", 
                      "precipitation_sum", "relative_humidity_2m_mean"],
            "timezone": "America/Sao_Paulo"
        }

        sucesso = False
        tentativas = 0
        
        # Loop de Tentativa (Retry Manual)
        while not sucesso and tentativas < 3:
            try:
                # Faz a requisição
                responses = openmeteo.weather_api(url, params=params)
                response = responses[0]
                
                # Processa
                daily = response.Daily()
                
                daily_data = {
                    "date": pd.date_range(
                        start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
                        end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
                        freq = pd.Timedelta(seconds = daily.Interval()),
                        inclusive = "left"
                    ),
                    "temp_max": daily.Variables(0).ValuesAsNumpy(),
                    "temp_min": daily.Variables(1).ValuesAsNumpy(),
                    "temp_media": daily.Variables(2).ValuesAsNumpy(),
                    "chuva_mm": daily.Variables(3).ValuesAsNumpy(),
                    "umidade": daily.Variables(4).ValuesAsNumpy()
                }
                
                df_cidade = pd.DataFrame(data = daily_data)
                
                # Adiciona identificadores para o JOIN futuro
                df_cidade['ID_MN_RESI'] = codigo_ibge # A chave para cruzar com o SINAN
                df_cidade['municipio_nome'] = coords['nome']
                df_cidade['date'] = df_cidade['date'].dt.date
                
                lista_dados.append(df_cidade)
                sucesso = True
                
                # Pausa aumentada para evitar bloqueio (5 segundos)
                time.sleep(5)
                
            except Exception as e:
                tentativas += 1
                erro_msg = str(e)
                if 'limit exceeded' in erro_msg or '429' in erro_msg:
                    print(f"      ⚠️ Limite de API atingido. Esperando 60 segundos... (Tentativa {tentativas}/3)")
                    time.sleep(60) # Espera 1 minuto se for bloqueado
                else:
                    print(f"❌ Erro em {coords['nome']}: {e}")
                    break # Se for outro erro, desiste dessa cidade

    # 4. Consolidação
    if lista_dados:
        df_final = pd.concat(lista_dados)
        arquivo_saida = "dados_climaticos_regional_detalhado.parquet"
        df_final.to_parquet(arquivo_saida, index=False)
        
        print(f"\n✅ SUCESSO! Base climática gerada: {arquivo_saida}")
        print(f"📊 Total de registros diários: {len(df_final)}")
        print(df_final.head())
    else:
        print("⚠️ Nenhum dado coletado.")

if __name__ == "__main__":
    coletar_clima_regional()