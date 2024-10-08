import os
import re
import pandas as pd

from pycpfcnpj import cpfcnpj

from . import data_types

funcoes_analises = []

# Decorator que indica o nome das colunas retornadas por cada função
def analise_colunas(*args):
    def inner(func):
        funcoes_analises.append((args, func))
        return func
    return inner 

@analise_colunas('missing','unique','chave_candidata')
def analise_conteudo_unicidade(nome_coluna, tipo_coluna, s):
    l = len(s)
    if l == 0: return (None, None, None)
    n_nunique = s.nunique()
    n_missing = s.isnull().sum() 
    is_chave = (n_nunique == l - n_missing) and (n_nunique > 0)
    
    return (n_missing/l, n_nunique/l, ('-', 'SIM')[is_chave])

@analise_colunas('min', 'P01', 'P25', 'P50', 'P75', 'P99', 'max')
def analise_conteudo_percentiles(nome_coluna, tipo_coluna, s):
    p = s.quantile([0, 0.01, 0.25, 0.5, 0.75, 0.99, 1], interpolation='nearest')
    return p.tolist()
    
@analise_colunas('moda')
def analise_conteudo_moda(nome_coluna, tipo_coluna, s):
    l = len(s)
    if l == 0: return None

    p = s.mode(dropna=True)
    
    if len(p):
        return (p[0],)  
    else:
        return (None,)


@analise_colunas('CPF/CNPJ', 'CPF', 'CNPJ')
def analise_conteudo_cpf_cnpj(nome_coluna, tipo_coluna, s):
    if tipo_coluna != data_types.STRING: return None

    info = []
    
    l = len(s)-s.isnull().sum()
    if l == 0: return None
    
    r = re.compile(r'[^0-9]')
    cpf_cnpj_digitos = s.apply(lambda x : len(r.sub('', str(x))))
    is_11 = cpf_cnpj_digitos==11
    is_14 = cpf_cnpj_digitos==14
    c_cpf = sum(is_11)
    c_cnpj = sum(is_14)

    cpf_cnpj_validos = s.apply(lambda x : cpfcnpj.validate(x))
    c_cpf_cnpj_validos = sum(cpf_cnpj_validos)
    c_cpf_validos = sum(cpf_cnpj_validos & is_11)
    c_cnpj_validos = sum(cpf_cnpj_validos & is_14)

    return c_cpf_cnpj_validos/l, c_cpf_validos/l, c_cnpj_validos/l    
    
@analise_colunas('hash')
def analise_conteudo_hash(nome_coluna, tipo_coluna, s):
    if tipo_coluna != data_types.STRING: return None
    
    m = s.str.match(r'\s*[0-9a-fA-F]{32,}\s*$').astype(bool).sum()

    l = len(s)
    if l == 0: return None

    return (m/l,)
    

def analise_colunas_sample(ambiente, sample_size, filtro=None):
    
    df_tabelas = ambiente.obter_tabelas()
    df_colunas = ambiente.obter_colunas()
    
    info = []
    df_tabelas_num_registros = df_tabelas.set_index(['database_name', 'schema_name', 'table_name'])['num_registros']
    df_tabelas_groupby = df_colunas.groupby(['database_name', 'schema_name', 'table_name'])
    print(df_tabelas.shape)
    
    info_analise_colunas = []
    
    for (database, schema, table),v in df_tabelas_groupby:
        print(f'Analisando Sample {database}.{schema}.{table}')
        #print(v)
        num_registros = df_tabelas_num_registros.loc[database, schema, table]
        if num_registros == -1:
            print('Unknown')
            continue
        
        colunas_selecionadas = v[v.tipo != data_types.BLOB]
        colunas_nomes = list(colunas_selecionadas.column_name)
        colunas_tipos = v.set_index('column_name').tipo
        df_sample = ambiente.obter_amostra(database, schema, table, colunas_nomes, num_registros, sample_size, filtro)
        
        print(df_sample.shape)
        
        v['num_registros'] = num_registros
        v['tamanho_amostra'] = df_sample.shape[0]
        v['filtro'] = filtro

        
        for analise_colunas, analise_f in funcoes_analises:
            #print(analise_colunas, analise_f)
            info = [[] for _ in analise_colunas]
            for c in v.column_name:
                x = None
                if c in df_sample.columns:
                    x = analise_f(c, colunas_tipos.loc[c], df_sample[c])
                    
                if x is None:
                    x = [None]*len(analise_colunas)
                    
                for i,j in enumerate(x):
                    info[i].append(j)
            
            for c,i in zip(analise_colunas, info):
                v[c] = i
        
        info_analise_colunas.append(v)
        
        #df = pd.read_sql(SQL, engine)
        

    # Carregar todos os arquivos sql da pasta "validacao" (se existir)

    validacoes_sql = {}
    dir_validacao = 'validacao'
    if os.path.exists(dir_validacao):
        for f in os.listdir(dir_validacao):
            if f.endswith('.sql'):
                with open(os.path.join(dir_validacao, f)) as file:
                    sql = file.read()
                    validacoes_sql[f] = sql
    
    #df = df_tabelas.copy()
    #df['count'] = info
    #return df
    
    df = pd.concat(info_analise_colunas)

    for k, sql in validacoes_sql.items():
        df[k] = None
        df_filtrado = df
        import re
        m = re.match(r'#\s*filtro:\s*(.*?)\s*\n(.*)', sql, re.DOTALL | re.IGNORECASE)
        if m:
            filtro, sql = m.groups()
            try:
                df_filtrado = df_filtrado.query(filtro)
            except Exception as e:
                print(f"Erro ao aplicar filtro da validação {k}")
                print(f"Filtro: {filtro} - ", e)
                continue
                

        # para cada registro de df que tenha a coluna CPF>0, imprima a linha
        for _,r in df_filtrado.iterrows():
            df_validacao = ambiente.read_sql(sql, database=r['database_name'], schema=r['schema_name'], table=r['table_name'], column=r['column_name'])
            ret = df_validacao.iloc[0,0]
            df.loc[_,k] = ret


    return df