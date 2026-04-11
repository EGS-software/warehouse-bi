import pandas as pd
import os

# 1. Como a pasta 'data' agora está JUNTO com o script dentro de 'etl':
diretorio_script = os.path.dirname(os.path.abspath(__file__))
pasta_dados = os.path.join(diretorio_script, 'data')

# 2. Arquivos
arquivos_pars = ['PARS2501.csv', 'PARS2505.csv', 'PARS2508.csv']
lista_de_dataframes = []

# 3. Leitura e Concatenação
for arquivo in arquivos_pars:
    caminho_completo = os.path.join(pasta_dados, arquivo)
    print(f"Lendo {caminho_completo}...")
    
    # A SOLUÇÃO ESTÁ AQUI: Adicionamos o encoding='latin1'
    df_temp = pd.read_csv(caminho_completo, sep=';', encoding='latin1', low_memory=False) 
    
    lista_de_dataframes.append(df_temp)

# 4. Empilhando
print("\nEmpilhando os dados...")
df_fato = pd.concat(lista_de_dataframes, ignore_index=True)

# 5. Verificando
print(f"Pronto! Sua tabela Fato agora tem {len(df_fato)} linhas.\n")
print(df_fato.head())