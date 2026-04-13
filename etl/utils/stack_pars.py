import os
import pandas as pd

def parse_stack(pasta_dados, arquivos_pars, engine, dicionario_colunas_fato, carregar_fato_func):
    """
    Processa os arquivos PARS em streaming e carrega direto no banco.
    Não retorna o DataFrame para evitar estouro de memória (Erro 137).
    """
    print("\nIniciando carga da Tabela Fato (Streaming de Big Data)...", flush=True)
    
    # IMPORTANTE: Fora do loop de arquivos para não apagar os dados do arquivo anterior
    primeiro_lote_geral = True

    for arquivo in arquivos_pars:
        caminho_completo = os.path.join(pasta_dados, arquivo)
        print(f"\n--- Lendo arquivo: {arquivo} ---", flush=True)
        
        # Leitura em pedaços de 100k linhas
        chunks = pd.read_csv(caminho_completo, sep=',', encoding='latin1', low_memory=False, chunksize=100000)
        
        lote_num = 1 
        for chunk in chunks:
            print(f"Processando lote {lote_num} do arquivo {arquivo}...", flush=True)

            # Limpeza rápida
            chunk.dropna(subset=['PA_PROC_ID', 'PA_MUNPCN', 'PA_CODUNI'], inplace=True)
            
            # Define se cria a tabela ou apenas anexa
            modo = 'replace' if primeiro_lote_geral else 'append'
            primeiro_lote_geral = False # Após o primeiro pedaço do primeiro arquivo, tudo vira append
            
            # Chama a função de carga (passando a engine de verdade)
            carregar_fato_func(chunk, dicionario_colunas_fato, engine, modo)
            
            lote_num += 1

    print("\nCarga da Tabela Fato concluída com sucesso!", flush=True)
    return True # Retorna apenas um sinal de sucesso