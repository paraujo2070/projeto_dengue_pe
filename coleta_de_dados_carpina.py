import pandas as pd
from pysus import SINAN

# Vamos testar apenas com CARPINA para ver se achamos
# Código 7 dígitos: 2604106
# Código 6 dígitos: 260410
municipio_teste_7 = "2604106"
municipio_teste_6 = "260410"

def diagnosticar_dados():
    print("🕵️‍♂️ Iniciando Diagnóstico em 2023...")
    sinan = SINAN().load()
    
    # Pega os arquivos
    files = sinan.get_files('DENG', year=2023)
    
    if not files:
        print("❌ Arquivo não encontrado no servidor.")
        return

    print("⬇️ Baixando arquivo BRASIL (Aguarde)...")
    # Baixa e converte
    parquet_set = sinan.download(files)
    df_br = parquet_set.to_dataframe()
    
    print(f"\n📊 Tamanho do DataFrame Brasil: {df_br.shape}")
    print("📋 Colunas encontradas:", df_br.columns.tolist()[:10]) # Mostra as 10 primeiras
    
    if 'ID_MN_RESI' in df_br.columns:
        # Pega amostra de 5 municípios para vermos o formato
        amostra = df_br['ID_MN_RESI'].unique()[:5]
        print(f"\n🔍 Amostra da coluna ID_MN_RESI (O que tem dentro?): {amostra}")
        
        # Teste de compatibilidade
        df_br['ID_MN_RESI'] = df_br['ID_MN_RESI'].astype(str).str.strip()
        
        # Tenta achar Carpina com 7 dígitos
        achei_7 = df_br[df_br['ID_MN_RESI'] == municipio_teste_7]
        print(f"teste 7 dígitos ({municipio_teste_7}): Encontrados {len(achei_7)} casos.")
        
        # Tenta achar Carpina com 6 dígitos
        achei_6 = df_br[df_br['ID_MN_RESI'] == municipio_teste_6]
        print(f"teste 6 dígitos ({municipio_teste_6}): Encontrados {len(achei_6)} casos.")
        
    else:
        print("❌ A coluna ID_MN_RESI não existe! O nome pode ter mudado.")

if __name__ == "__main__":
    diagnosticar_dados()