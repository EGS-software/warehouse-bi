import os
import pandas as pd

def parse_stack(pasta_dados, arquivos_pars):
    """
    Lê e concatena uma lista de arquivos CSV de uma pasta específica.
    """
    lista_de_dataframes = []

    # Leitura e Concatenação
    for arquivo in arquivos_pars:
        caminho_completo = os.path.join(pasta_dados, arquivo)
        print(f"Lendo {caminho_completo}...")
        
        # A SOLUÇÃO ESTÁ AQUI: Adicionamos o encoding='latin1'
        df_temp = pd.read_csv(caminho_completo, sep=',', encoding='latin1', low_memory=False) 
        
        lista_de_dataframes.append(df_temp)

    # Empilhando
    print("\nEmpilhando os dados...")
    df_fato = pd.concat(lista_de_dataframes, ignore_index=True)

    # Verificando
    print(f"Pronto! Sua tabela Fato bruta tem {len(df_fato)} linhas.\n")
    
    # Fundamental: Retorna o DataFrame para quem chamou a função
    return df_fato