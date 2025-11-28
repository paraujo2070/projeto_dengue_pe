import pandas as pd
from pysus import SINAN
import gc  # Garbage Collector (Limpeza de memória)
import os

# 1. Configurações Iniciais
# CORREÇÃO CRÍTICA: Códigos IBGE convertidos para 6 dígitos (Padrão SINAN)
# Região: II GERES - Limoeiro (Mata Norte e Agreste Setentrional)
codigos_municipios_6_digitos = [
    # Cidades da II GERES
    "260290", # Buenos Aires
    "260410", # Carpina
    "260845", # Lagoa do Carro
    "260850", # Lagoa de Itaenga
    "260950", # Nazaré da Mata
    "261060", # Paudalho
    "261560", # Tracunhaém
    "261640", # Vicência
    "260190", # Bom Jardim
    "260415", # Casinhas
    "260500", # Cumaru
    "260540", # Feira Nova
    "260800", # João Alfredo
    "260890", # Limoeiro
    "260900", # Machados
    "260990", # Orobó
    "261040", # Passira
    "261230", # Salgadinho
    "261450", # Surubim
    "261618"  # Vertente do Lério
]

def processar_ano_a_ano(anos):
    """
    Baixa o arquivo Brasil, filtra a II GERES e salva parciais.
    """
    sinan = SINAN().load()
    arquivos_gerados = []

    for ano in anos:
        print(f"\n🔄 INICIANDO CICLO: {ano}")
        try:
            # 1. Localizar o arquivo
            files = sinan.get_files('DENG', year=ano)
            if not files:
                print(f"⚠️ Arquivo de {ano} não encontrado.")
                continue

            # 2. Baixar
            print(f"   ⬇️ Baixando Brasil {ano}...")
            parquet_set = sinan.download(files) 

            # 3. Converter para DataFrame
            print(f"   🔨 Carregando na memória...")
            df_br = parquet_set.to_dataframe()
            
            # 4. Filtragem (Usando coluna ID_MN_RESI - Município de Residência)
            if 'ID_MN_RESI' in df_br.columns:
                # Garante formato string e remove espaços
                df_br['ID_MN_RESI'] = df_br['ID_MN_RESI'].astype(str).str.strip()
                
                # Filtra pela lista de 6 dígitos
                df_pe = df_br[df_br['ID_MN_RESI'].isin(codigos_municipios_6_digitos)].copy()
                df_pe['ano_base'] = ano
                
                registros = len(df_pe)
                print(f"   ✅ SUCESSO! Encontrados {registros} casos na II GERES.")
                
                # 5. Salvar checkpoint
                if registros > 0:
                    nome_arquivo = f"temp_dengue_{ano}.parquet"
                    df_pe.to_parquet(nome_arquivo, index=False)
                    arquivos_gerados.append(nome_arquivo)
                    print(f"   💾 Salvo: {nome_arquivo}")
                
            else:
                print(f"   ⚠️ Coluna ID_MN_RESI não encontrada.")

            # 6. Faxina na Memória (Essencial!)
            del df_br
            if 'df_pe' in locals(): del df_pe
            del parquet_set
            gc.collect()
            print(f"   🧹 Memória limpa.")

        except Exception as e:
            print(f"❌ Erro em {ano}: {e}")
            gc.collect()

    return arquivos_gerados

def consolidar_dados(lista_arquivos):
    """Junta os pedaços em um arquivo final"""
    print(f"\n🔗 Consolidando {len(lista_arquivos)} arquivos...")
    if not lista_arquivos:
        return pd.DataFrame()
    
    df_final = pd.concat([pd.read_parquet(f) for f in lista_arquivos])
    
    # Limpa arquivos temporários
    for f in lista_arquivos:
        try:
            os.remove(f)
        except:
            pass
        
    return df_final

if __name__ == "__main__":
    # Vamos tentar os últimos 5 anos disponíveis
    anos_estudo = [2019, 2020, 2021, 2022, 2023] 
    
    print("🚀 Coletando dados da II GERES (Limoeiro/PE)...")
    arquivos_temp = processar_ano_a_ano(anos_estudo)
    
    if arquivos_temp:
        df_completo = consolidar_dados(arquivos_temp)
        
        # Salva o arquivo final
        arquivo_final = "dataset_dengue_II_GERES.parquet"
        df_completo.to_parquet(arquivo_final, index=False)
        
        print(f"\n🏆 CONCLUÍDO! Arquivo gerado: {arquivo_final}")
        print(f"📊 Total acumulado de notificações: {len(df_completo)}")
        print(df_completo.head())
    else:
        print("\n⚠️ Nenhum dado encontrado. Verifique os códigos ou anos.")