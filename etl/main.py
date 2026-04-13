import os
import numpy as np
import pandas as pd

from utils import *

def main():
    print("--- Iniciando Pipeline ETL SIASUS ---\n")
    
    # 1. Definimos os caminhos UMA ÚNICA VEZ
    diretorio_script = os.path.dirname(os.path.abspath(__file__))
    pasta_dados = os.path.join(diretorio_script, 'data')
    
    # 2. Extraindo Tabela Fato
    arquivos_pars = ['PARS2501.csv', 'PARS2505.csv', 'PARS2508.csv']
    print("Extraindo e empilhando dados de Produção Ambulatorial (Fato)...")
    df_fato_bruto = parse_stack(pasta_dados=pasta_dados, arquivos_pars=arquivos_pars)
    
    # ADICIONE ESTA LINHA PARA DESCOBRIR O NOME REAL DAS COLUNAS:
    print("\nNomes reais das colunas no arquivo:")
    print(df_fato_bruto.columns.tolist())
    
    # Pequena limpeza na fato antes de subir (removendo nulos críticos nas chaves)
    print("\nLimpando dados nulos críticos da Fato...")
    df_fato_limpo = df_fato_bruto.dropna(subset=['PA_PROC_ID', 'PA_MUNPCN', 'PA_CODUNI'])
    
    # Pequena limpeza na fato antes de subir (removendo nulos críticos nas chaves)
    print("Limpando dados nulos críticos da Fato...")
    df_fato_limpo = df_fato_bruto.dropna(subset=['PA_PROC_ID', 'PA_MUNPCN', 'PA_CODUNI'])
    
    # 3. Configurar Conexão com o Banco de Dados
    engine = get_db_engine()
    
   # 4. Lendo os arquivos auxiliares (Dimensões) do disco
    print("\nLendo arquivos auxiliares do disco...")
    
    print("Lendo Municípios...")
    df_municipios = pd.read_csv(os.path.join(pasta_dados, 'tb_municip.csv'), sep=',', encoding='latin1', low_memory=False)
    
    print("Lendo Procedimentos...")
    df_procedimentos = pd.read_csv(os.path.join(pasta_dados, 'TB_SIGTAW.csv'), sep=',', encoding='latin1', low_memory=False)
    
    print("Lendo CID...")
    df_cid = pd.read_csv(os.path.join(pasta_dados, 'S_CID.csv'), sep=',', encoding='latin1', low_memory=False)
    
    print("Lendo CBO...")
    df_cbo = pd.read_csv(os.path.join(pasta_dados, 'CBO.csv'), sep=',', encoding='latin1', low_memory=False)
    
    print("Lendo Estabelecimentos (CADGERRS)...")
    df_estabelecimentos = pd.read_csv(os.path.join(pasta_dados, 'CADGERRS.csv'), sep=',', encoding='latin1', low_memory=False)

    
    # 5. Carga das Dimensões no PostgreSQL
    print("\nIniciando carga das Dimensões...")
    
    carregar_dimensao(
        df=df_municipios, 
        nome_tabela='dim_municipio', 
        colunas_renomear={'CO_MUNICIP': 'id_municipio', 'DS_NOME': 'nome_municipio', 'CO_UF': 'uf'},
        engine=engine
    )
    
    carregar_dimensao(
        df=df_procedimentos,
        nome_tabela='dim_procedimento',
        colunas_renomear={'IP_COD': 'id_procedimento', 'IP_DSCR': 'nome_procedimento'},
        engine=engine
    )
    
    carregar_dimensao(
        df=df_cid,
        nome_tabela='dim_diagnostico',
        colunas_renomear={'CD_COD': 'id_cid', 'CD_DESCR': 'descricao_cid'},
        engine=engine
    )

    carregar_dimensao(
        df=df_cbo,
        nome_tabela='dim_ocupacao',
        colunas_renomear={'CBO': 'id_cbo', 'DS_CBO': 'nome_ocupacao'},
        engine=engine
    )

    carregar_dimensao(
        df=df_estabelecimentos,
        nome_tabela='dim_estabelecimento',
        colunas_renomear={'CNES': 'id_cnes', 'FANTASIA': 'nome_fantasia', 'RAZ_SOCI': 'razao_social'},
        engine=engine
    )

    # 6. Carga da Tabela Fato
    print("\nIniciando carga da Tabela Fato...")
    
    dicionario_colunas_fato = {
        'PA_MUNPCN': 'id_municipio',
        'PA_PROC_ID': 'id_procedimento',
        'PA_CODUNI': 'id_cnes',
        'PA_CIDPRI': 'id_cid',
        'PA_CBOCOD': 'id_cbo',
        'PA_QTDAPR': 'qtd_aprovada',
        'PA_VALAPR': 'valor_aprovado'
    }
    
    carregar_fato(df_fato_limpo, dicionario_colunas_fato, engine)

    print("\nProcesso de ETL finalizado com sucesso! Seu Data Warehouse está pronto.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Erro durante a execução: {e}")