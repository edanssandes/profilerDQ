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

    cpf_cnpj_validos = s.apply(lambda x : cpfcnpj.validate(str(x)) if x else False)
    c_cpf_cnpj_validos = sum(cpf_cnpj_validos)
    c_cpf_validos = sum(cpf_cnpj_validos & is_11)
    c_cnpj_validos = sum(cpf_cnpj_validos & is_14)

    return c_cpf_cnpj_validos/l, c_cpf_validos/l, c_cnpj_validos/l    
    
@analise_colunas('hash')
def analise_conteudo_hash(nome_coluna, tipo_coluna, s):
    if (tipo_coluna != data_types.STRING) or s.dtype != 'object':
        return None
    
    m = s.str.match(r'\s*[0-9a-fA-F]{32,}\s*$').astype(bool).sum()

    l = len(s)
    if l == 0: return None

    return (m/l,)
    
@analise_colunas('prenome')
def analise_conteudo_prenome(nome_coluna, tipo_coluna, s):
    if tipo_coluna != data_types.STRING or s.dtype != 'object':
        return None

    l = len(s)
    if l == 0: return None

    # https://censo2010.ibge.gov.br/nomes/#/ranking
    prenomes_masculinos_comuns = ['jose', 'joao', 'antonio', 'francisco', 'carlos', 'paulo', 'pedro', 
                                  'lucas', 'luiz', 'marcos', 'luis', 'gabriel', 'rafael', 'daniel', 'marcelo', 
                                  'bruno', 'eduardo', 'felipe', 'raimundo', 'rodrigo']

    prenomes_femininos_comuns = ['maria', 'ana', 'francisca', 'antonia', 'adriana', 'juliana', 'marcia',
                                  'fernanda', 'patricia', 'aline', 'sandra', 'camila', 'amanda', 'bruna', 'jessica',
                                  'leticia', 'julia', 'luciana', 'vanessa', 'mariana']
    
    sobrenomes_comuns = ['silva', 'santos', 'oliveira', 'souza', 'sousa', 'lima', 'costa', 'pereira', 'rodrigues',
                            'almeida', 'ferreira', 'araujo', 'carvalho', 'gomes', 'martins', 'barbosa',
                            'alves', 'melo', 'ribeiro', 'moura', 'cavalcante', 'castro', 'cardoso', 'fernandes',
                            'torres', 'ribeiro', 'mendes', 'barros', 'freitas', 'barros', 'nunes', 'peixoto', 'junior']

    nomes_columns = prenomes_masculinos_comuns + prenomes_femininos_comuns + sobrenomes_comuns

    # Remove acentos
    s = s.str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

    tokens = s.str.split()#.explode()
    # Identifica tokens que possui ao menos um nome comum
    tokens = tokens.apply(lambda x : any([t in nomes_columns for t in x]) if x else False)
    

    return (tokens.sum()/l,)
    

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
        colunas_tipos = v.set_index('column_name').tipo
        df_sample = ambiente.obter_amostra(database, schema, table, colunas_selecionadas, num_registros, sample_size, filtro)
        
        print(df_sample.shape)
        
        v['num_registros'] = num_registros
        v['tamanho_amostra'] = df_sample.shape[0]
        v['registros_unique'] = df_sample.drop_duplicates().shape[0]
        v['filtro'] = filtro

        
        for analise_colunas, analise_f in funcoes_analises:
            #print(analise_colunas, analise_f)
            info = [[] for _ in analise_colunas]
            for c in v.column_name:
                x = None
                if c in df_sample.columns:
                    x = analise_f(c, colunas_tipos.loc[c], df_sample[c])
                    assert(isinstance(x, tuple), f"Erro na função {analise_f.__name__} para a coluna {c}. O retorno deve ser uma tupla.")
                        
                    
                if x is None:
                    x = [None]*len(analise_colunas)

                for i,j in enumerate(x):
                    info[i].append(j)
            
            for c,i in zip(analise_colunas, info):
                v[c] = i
        
        info_analise_colunas.append(v)
        
        #df = pd.read_sql(SQL, engine)
        

    
    #df = df_tabelas.copy()
    #df['count'] = info
    #return df
    
    df = pd.concat(info_analise_colunas)

    return df


def analise_colunas_sql(ambiente, df_colunas_sample, filtro=None):

    # Carregar todos os arquivos sql da pasta "validacao" (se existir)

    validacoes_sql = {}
    dir_validacao = 'validacao'
    if os.path.exists(dir_validacao):
        for f in os.listdir(dir_validacao):
            if f.endswith('.sql'):
                with open(os.path.join(dir_validacao, f)) as file:
                    sql = file.read()
                    validacoes_sql[f] = sql


    df_tabelas_groupby = df_colunas_sample.groupby(['database_name', 'schema_name', 'table_name'])

    returns = []

    for (database, schema, table),v in df_tabelas_groupby:
        print('**---', database, schema, table, v.shape)

        for sql_name, sql in validacoes_sql.items():
            #df[k] = None
            import re
            # Separete header (all first lines starting with #) from sql body
            lines = sql.split('\n')
            headers = []
            while lines and lines[0].startswith('#'):
                headers.append(lines.pop(0))
            sql = '\n'.join(lines)    
            print(headers)
            print('****')
            print(sql)


            column_vars = {}

            DEFAULT_VARIABLE = 'coluna'

            # process header
            for h in headers:
                m = re.match(r'^#\s*filtro(\[\w+\])?:\s*(.*?)\s*$', h, re.IGNORECASE)
                if m:
                    variable, filtro = m.groups()
                    if variable is None:
                        variable = DEFAULT_VARIABLE
                else:
                    variable = DEFAULT_VARIABLE
                    filtro = '*'

                try:
                    if filtro == '*' or not filtro:
                        columns = v
                    else:
                        columns = v.query(filtro)
                except Exception as e:
                    print(f"Erro ao aplicar filtro da validação {sql_name}")
                    print(f"Filtro: {filtro} - ", e)
                    continue

                column_names_list = columns['column_name'].tolist()

                if variable == DEFAULT_VARIABLE and variable not in column_vars:
                    column_vars[variable] = column_names_list

                if variable in column_vars:
                    # Sufixes variable name with a number until it is unique
                    i = 1
                    while f"{variable}_{i}" in column_vars:
                        i += 1
                    print(f"Redefining variable {variable} to {variable}_{i}")
                    variable = f"{variable}_{i}"

                column_vars[variable] = columns['column_name'].tolist()
                if variable == DEFAULT_VARIABLE:
                    column_vars[variable] = column_vars[variable]
                print(f"Aplicando filtro {filtro} da validação {sql_name}: {variable}={column_vars[variable]}")

            # iterates through a cartesian product combining every row in column_vars dataframes
            from itertools import product

            print(column_vars)
            # produtório de todas as combinações de colunas
            num_combinacoes = 1
            for var in column_vars.values():
                num_combinacoes *= len(var)

            if num_combinacoes == 0:
                print(f"WARNING: Ignorando validação {sql_name}. Nenhuma combinação de colunas encontrada.")
                continue

            MAX_COMBINACOES = 16
            if num_combinacoes > MAX_COMBINACOES:
                # Ignora combinações com mais de MAX_COMBINACOES colunas
                print(f"WARNING: Ignorando combinações de colunas. {num_combinacoes}>{MAX_COMBINACOES} combinações.")
                continue

            print(f"Gerando {num_combinacoes} combinações de colunas")


            for r in product(*[[(k,i) for i in v] for k,v in column_vars.items()]):
                kwargs = dict(r)
                print('---', database, schema, table, kwargs)
                df_validacao = ambiente.read_sql(sql, database=database, schema=schema, table=table, **kwargs)
                ret = df_validacao.iloc[0,0]

                kwargs.pop(DEFAULT_VARIABLE)
                column_names = list(kwargs.values())
                print(column_names)
                column_names_strings = ','.join(column_names)
                num_columns = len(column_names)

                returns.append({
                    'database_name': database,
                    'schema_name': schema,
                    'table_name': table,
                    'title': sql_name if num_columns == 1 else f"{sql_name}[{column_names_strings}]",
                    'result': ret,
                    'column_name': column_names_strings,
                    'num_columns': num_columns
                })
                #df.loc[_,k] = ret    

    return pd.DataFrame(returns)
    