import pandas as pd

# 1. Definimos a lista com os nomes dos arquivos que você tem
arquivos_pars = ['data/PARS2501.csv', 'data/PARS2505.csv', 'data/PARS2508.csv']

# 2. Criamos uma lista vazia para armazenar os dados temporariamente
lista_de_dataframes = []

# 3. Lemos cada arquivo e adicionamos à lista
for arquivo in arquivos_pars:
    print(f"Lendo {arquivo}...")
    
    # DICA: Verifique qual é o separador do seu CSV. 
    # Dados do governo/DATASUS costumam usar ponto e vírgula (sep=';').
    # Se der erro de "coluna única", mude para sep=','
    df_temp = pd.read_csv(arquivo, sep=';', low_memory=False) 
    
    lista_de_dataframes.append(df_temp)

# 4. Concatenamos tudo de uma vez só
print("Empilhando os dados...")
df_fato = pd.concat(lista_de_dataframes, ignore_index=True)

# 5. Verificamos o resultado final
print(f"Pronto! Sua tabela Fato agora tem {len(df_fato)} linhas.")
display(df_fato.head())