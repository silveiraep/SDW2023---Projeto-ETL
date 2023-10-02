import pandas as pd
from faker import Faker

# Inicializando o objeto Faker para gerar nomes aleatórios
fake = Faker()

def executar_pipeline_etl(arquivo_csv_entrada, arquivo_csv_saida):
    def extrair_dados(arquivo_csv):
        df = pd.read_csv(arquivo_csv)
        return df

    def nomear_ids(df):
        # Gerar nomes aleatórios para cada ID
        df['Nome'] = [fake.name() for _ in range(len(df))]
        return df

    def gerar_novo_arquivo(df, nome_do_novo_arquivo):
        df.to_csv(nome_do_novo_arquivo, index=False)

    # Etapa de Extração
    dados_extraidos = extrair_dados(arquivo_csv_entrada)

    # Etapa de Transformação
    dados_transformados = nomear_ids(dados_extraidos)

    # Printar os nomes atribuídos à cada ID
    print('Nomes atribuídos a cada ID:')
    for i, nome in enumerate(dados_transformados['Nome'], start=1):
        print(f'ID {i}: {nome}')

    # Etapa de Carregamento
    gerar_novo_arquivo(dados_transformados, arquivo_csv_saida)

    print('\nPipeline ETL concluída com sucesso.')

# Executando o Pipeline ETL
arquivo_csv_entrada = 'id_usuarios.csv'
arquivo_csv_saida = 'ids_transformadas.csv'
executar_pipeline_etl(arquivo_csv_entrada, arquivo_csv_saida)
