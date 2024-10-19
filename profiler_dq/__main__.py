import pandas as pd
import argparse
import json
import os


from .ambientes import Ambiente
from .info_colunas import analise_colunas_sample, analise_colunas_sql

AMOSTRA_PADRAO = 10000

EXEMPLO_AMBIENTES = {
    "<nome_do_ambiente_1>": {
        "url": "mssql+pyodbc://@DSN"
    },
    "<nome_do_ambiente_2>": {
        "url": "oracle://{user}:{password}@host:port/SID"
    }
}

def tratar_argumentos(id_ambientes):
    """
    Processa os argumentos de linha de comando

    :param argv: argunentos de linha de comando
    :return: Objeto do tipo argpasrse.Namespace contendo os argumentos passados por linha de comando.
    """
    parser = argparse.ArgumentParser(description='Analisa um banco de dados para auxiliar no processo de ETL.')
    parser.add_argument('--ambiente', type=str, default=list(id_ambientes)[0], metavar="ID",
                        help=f'id do ambiente: {list(id_ambientes)}.')
    
    parser.add_argument('--usuario', type=str, metavar="LOGIN", 
                        help=f'nome de usuario.')
    
    parser.add_argument('--database', type=str, metavar="BD",
                        help=f'Nome do banco de dados.')
    
    parser.add_argument('--schema', type=str, metavar="SCHEMA",
                        help=f'Nome do schema ou do owner do banco de dados (opcional).')
    
    parser.add_argument('--tables', type=str, nargs='+', metavar="TABELA", required=False,
                        help=f'Nome das tabelas de interesse (opcional).')
    
    parser.add_argument('--where', type=str, metavar="FILTRO", required=False,
                        help=f'Filtro adicional para as tabelas (opcional).')

    parser.add_argument('--amostra', type=int, metavar="N", default=AMOSTRA_PADRAO,
                        help=f'Número de registros na amostra.')
    
    parser.add_argument('--output', type=str, metavar="ARQUIVO", default='dicionario.xlsx',
                        help=f'Nome do arquivo de saída.')
    
    args = parser.parse_args()

    # Verifica se o ambiente passado existe
    if args.ambiente not in id_ambientes:
        print(f"O ambiente {args.ambiente} não foi encontrado.")
        print(f"Os ambientes disponíveis são: {list(id_ambientes)}")
        exit(1)

    return args 
    
def carregar_parametros_ambientes(arquivo='ambientes.json'):
    """
    Carrega os parâmetros dos ambientes de um arquivo JSON

    :param arquivo: Nome do arquivo JSON com os parâmetros dos ambientes
    """

    # Se o arquivo não existir, emite um erro fatal, explicando o problema.
    if not os.path.exists(arquivo):
        # Explica que o arquivo precisa existir apresentando um exemplo de conteúdo. Em seguida, aborta o programa.
        print(f"O arquivo {arquivo} com os parâmetros dos ambientes não foi encontrado.")
        print("Crie o arquivo para poder prosseguir. Exemplo de conteúdo:")
        print(json.dumps(EXEMPLO_AMBIENTES, indent=4))
        exit(1)

    with open(arquivo) as f:
        parametros = json.load(f)
    return parametros    
    
def main():
    parametros_ambientes = carregar_parametros_ambientes()
    args = tratar_argumentos(id_ambientes=parametros_ambientes.keys())
    
    ambiente = Ambiente(ambiente=parametros_ambientes[args.ambiente], 
                        usuario=args.usuario, 
                        database=args.database, 
                        schema=args.schema,
                        tabelas=args.tables,
                        filtro=args.where)
    
    df_colunas = ambiente.obter_colunas()
    df_tabelas = ambiente.obter_tabelas()

    if len(df_colunas) == 0 or len(df_tabelas) == 0:
        print("FATAL: Nenhuma tabela/coluna encontrada. Execute o programa novamente com filtros diferentes.")
        exit(1)

    df_colunas_sample = analise_colunas_sample(ambiente, sample_size=args.amostra, filtro=args.where)
    df_colunas_validacao = analise_colunas_sql(ambiente, df_colunas_sample, filtro=args.where)

    df_colunas_validacao_1 = df_colunas_validacao[df_colunas_validacao['num_columns'] == 1]
    df_colunas_validacao_1 = df_colunas_validacao_1.pivot(index=('database_name', 'schema_name', 'table_name', 'column_name'), columns='title', values='result').reset_index()
    df_colunas_sample = df_colunas_sample.merge(df_colunas_validacao_1, on=('database_name', 'schema_name', 'table_name', 'column_name'), how='outer')

    print(f"Gerando arquivo {args.output}")
    
    with pd.ExcelWriter(args.output) as writer:
        # Salvando Planilhas
        df_tabelas.to_excel(writer, sheet_name="Tabelas", index=False)
        df_colunas.to_excel(writer, sheet_name="Colunas", index=False)
        df_colunas_sample.to_excel(writer, sheet_name="Amostra", index=False)
        df_colunas_validacao.to_excel(writer, sheet_name="Validacao", index=False)


if __name__ == '__main__':
    main()