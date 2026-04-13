from sqlalchemy import create_engine
import pandas as pd

def get_db_engine():
    """
    Cria e retorna a conexão com o banco.
    Repare que no lugar de 'localhost', usamos 'postgres_dw', 
    que é o nome do serviço lá no docker-compose.yml!
    """
    engine = create_engine('postgresql://postgres:admin@postgres_dw:5432/dw_siasus')
    return engine

def carregar_dimensao(df, nome_tabela, colunas_renomear, engine):
    """
    Função genérica para carregar qualquer dimensão.
    """
    print(f"Carregando {nome_tabela}...")
    
    # Seleciona apenas as colunas que vieram no dicionário e renomeia
    df_dimensao = df[list(colunas_renomear.keys())].rename(columns=colunas_renomear)
    
    # Remove duplicatas (uma dimensão não pode ter IDs repetidos)
    df_dimensao = df_dimensao.drop_duplicates()
    
    # Carrega no banco
    df_dimensao.to_sql(nome_tabela, engine, if_exists='replace', index=False)
    print(f"{nome_tabela} carregada com sucesso!")

def carregar_fato(df_fato_limpo, colunas_renomear, engine, modo='replace'):
    """
    Carrega a tabela fato principal.
    """
    print("Carregando Fato_Producao...")
    df_fato = df_fato_limpo[list(colunas_renomear.keys())].rename(columns=colunas_renomear)
    df_fato.to_sql('fato_producao', engine, if_exists=modo, index=False)
    print("Fato_Producao carregada com sucesso!")