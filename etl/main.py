import os
import pandas as pd

# Mantenha os seus imports conforme a estrutura das suas pastas
from utils import *
from utils.stack_pars import parse_stack 

def main():
    print("--- Iniciando Pipeline ETL SIASUS ---\n")
    
    # 1. Definimos os caminhos
    diretorio_script = os.path.dirname(os.path.abspath(__file__))
    pasta_dados = os.path.join(diretorio_script, 'data')
    
    # 2. Configurar Conexão com o Banco de Dados
    engine = get_db_engine()
    
    # ---------------------------------------------------------
    # FASE 1: CARGA DAS DIMENSÕES (Tabelas Auxiliares)
    # ---------------------------------------------------------
    print("Lendo arquivos auxiliares (Dimensões) do disco...")
    df_municipios = pd.read_csv(os.path.join(pasta_dados, 'tb_municip.csv'), sep=',', encoding='latin1', low_memory=False)
    df_regioes = pd.read_csv(os.path.join(pasta_dados, 'regioes_saude.csv'), sep=',', encoding='utf-8')
    df_municipios_enriquecido = pd.merge(
        df_municipios, 
        df_regioes, 
        left_on='CO_MUNICIP',   # Código IBGE na tabela do DATASUS
        right_on='CódigoIBGE',     # Código IBGE na planilha
        how='left'              # Mantém todos os municípios, mesmo os que não acharem região
    )
    df_procedimentos = pd.read_csv(os.path.join(pasta_dados, 'TB_SIGTAW.csv'), sep=',', encoding='latin1', low_memory=False)
    df_cid = pd.read_csv(os.path.join(pasta_dados, 'S_CID.csv'), sep=',', encoding='latin1', low_memory=False)
    df_cbo = pd.read_csv(os.path.join(pasta_dados, 'CBO.csv'), sep=',', encoding='latin1', low_memory=False)
    df_estabelecimentos = pd.read_csv(os.path.join(pasta_dados, 'CADGERRS.csv'), sep=',', encoding='latin1', low_memory=False)

    print("\nIniciando carga das Dimensões no banco...")
    carregar_dimensao(df_municipios_enriquecido, 'dim_municipio', {'CO_MUNICIP': 'id_municipio', 'DS_NOME': 'nome_municipio', 'CO_UF': 'uf', 'Região de Saúde' : 'regiao_saude', 'Macrorregião de Saúde': 'macrorregiao_saude'}, engine)
    carregar_dimensao(df_procedimentos, 'dim_procedimento', {'IP_COD': 'id_procedimento', 'IP_DSCR': 'nome_procedimento'}, engine)
    carregar_dimensao(df_cid, 'dim_diagnostico', {'CD_COD': 'id_cid', 'CD_DESCR': 'descricao_cid'}, engine)
    carregar_dimensao(df_cbo, 'dim_ocupacao', {'CBO': 'id_cbo', 'DS_CBO': 'nome_ocupacao'}, engine)
    carregar_dimensao(df_estabelecimentos, 'dim_estabelecimento', {'CNES': 'id_cnes', 'FANTASIA': 'nome_fantasia', 'RAZ_SOCI': 'razao_social'}, engine)

    # ---------------------------------------------------------
    # FASE 2: CARGA DA TABELA FATO (Processamento em Lotes / Streaming)
    # ---------------------------------------------------------
    arquivos_pars = ['PARS2501.csv', 'PARS2505.csv', 'PARS2508.csv']
    
    dicionario_colunas_fato = {
        'PA_CMP': 'competencia',
        'PA_MUNPCN': 'id_municipio',
        'PA_PROC_ID': 'id_procedimento',
        'PA_CODUNI': 'id_cnes',
        'PA_CIDPRI': 'id_cid',
        'PA_CBOCOD': 'id_cbo',
        'PA_QTDAPR': 'qtd_aprovada',
        'PA_VALAPR': 'valor_aprovado'
    }

    # A função parse_stack agora orquestra tudo: lê os pedaços, limpa os nulos e salva no banco.
    parse_stack(
        pasta_dados=pasta_dados, 
        arquivos_pars=arquivos_pars, 
        engine=engine, 
        dicionario_colunas_fato=dicionario_colunas_fato,
        carregar_fato_func=carregar_fato 
    )

    print("\nProcesso de ETL finalizado com sucesso! Seu Data Warehouse está pronto.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nErro crítico durante a execução: {e}")