import sqlalchemy
import pandas as pd
import getpass

import profiler_dq.flavors.mssql
import profiler_dq.flavors.oracle
import profiler_dq.flavors.sqlite

def carregar_flavor(flavor_name):
    """
    Carrega o módulo de flavor correspondente ao nome passado como argumento.
    """

    for name, module in profiler_dq.flavors.__dict__.items():
        if name.startswith('__'):
            continue

        if name == flavor_name:
            return module

    return None            


class Ambiente:
    def __init__(self, ambiente, usuario=None, senha=None, database=None, schema=None, tabelas=None, filtro=None):

        # Verifica se a url do ambiente contém os campos de usuário e senha. 
        # Se sim, solicita os valores caso já não tenham sido passados como argumento
        if not usuario and '{user}' in ambiente['url']:
            usuario = input('Usuário: ')

        if not senha and '{password}' in ambiente['url']:
            senha = getpass.getpass('Senha: ')

        # Substituindo os valores de usuário e senha na url
        url = ambiente['url'].format(user=usuario, password=senha, database=database)

        # Adicionando o nome do banco de dados nos argumentos de conexão, se necessário
        if url.startswith("mssql+pyodbc:") and database:
            connect_args = {'database': database}
        else:
            connect_args = {}

        # Criando a engine de conexão com o banco
        engine = sqlalchemy.create_engine(url, connect_args=connect_args)

        # Identifica qual é o SGBD/flavor do ambiente a partir do engine
        flavor = carregar_flavor(engine.dialect.name)

        if not flavor:
            raise ValueError(f"Flavor não encontrado para o SGBD {engine.dialect.name}")

        
        # Armazenando os valores do ambiente nos atributos do objeto
        self._ambiente = ambiente
        self._engine = engine
        self._database = database
        self._schema = schema
        self._tabelas = tabelas
        self._filtro = filtro
        self._df_colunas = None
        self._df_tabelas = None
        self._flavor = flavor

    def obter_colunas(self):
        if self._df_colunas is None:
            map_types = self._flavor.MAP_TYPES
            sql = self._flavor.lista_colunas(database=self._database, schema=self._schema)

            print("Obtendo lista de colunas")
            print(sql)
            df = pd.read_sql(sql, self._engine)

            if self._schema:
                df = df[df.schema_name == self._schema]

            if self._tabelas:
                df = df[df.table_name.isin(self._tabelas)]

            df['tipo'] = df.data_type.str.lower().map(map_types)

            self._df_colunas = df

        return self._df_colunas
        
    def obter_tabelas(self, numero_de_registros=True):
        if self._df_tabelas is None:
            df_colunas = self.obter_colunas()
            df = df_colunas.groupby(['database_name', 'schema_name', 'table_name'], dropna=False).size().reset_index(name='numero_colunas')
            self._df_tabelas = df
        
        CAMPO_NUM_REGISTROS = 'num_registros'
        if numero_de_registros and CAMPO_NUM_REGISTROS not in self._df_tabelas.columns:
            num_registros = []

            for x,v in self._df_tabelas.iterrows():
                print(f'Contando registros {x}: {v.database_name}.{v.schema_name}.{v.table_name}')

                num = self.obter_numero_registros(v.database_name, v.schema_name, v.table_name)
                num_registros.append(num)

                print(num)

            self._df_tabelas[CAMPO_NUM_REGISTROS] = num_registros
            self._df_tabelas['filtro'] = self._filtro
        
        return self._df_tabelas
        

    def obter_numero_registros(self, database, schema, table):
        try:
            df = self.read_sql('select count(1) as v FROM {tabela} x', database=database, schema=schema, table=table)
            count = df.iloc[0,0]
            return count
        except Exception as e:
            print(e)
            return -1

    def obter_numero_registros_distintos(self, database, schema, table):
        try:
            df = self.read_sql('select count(1) as v FROM (select distinct * from {tabela} y ) x', database=database, schema=schema, table=table)
            count = df.iloc[0,0]
            return count
        except Exception as e:
            print(e)
            return -1

    def get_where_clause(self):
        if self._filtro:
            where_clause = f" WHERE {self._filtro}"
        else:
            where_clause = ""
        return where_clause

        
    def obter_amostra(self, database, schema, table, colunas, num_registros, sample_size, filtro):
        if num_registros is None:
            num_registros = self.obter_numero_registros(database, schema, table)

        sql = self._flavor.sample(database, schema, table, colunas, num_registros, sample_size, filtro)
        print(sql)
        df_sample = pd.read_sql(sql, self._engine)
        print(df_sample.shape)
        
        return df_sample


    def read_sql(self, sql, database=None, schema=None, table=None, **kwargs):
        tabela = self.get_table_name(database, schema, table)
        where_clause = self.get_where_clause()
        tabela = f'(SELECT * FROM {tabela} {where_clause})'
        
        #print(sql)
        sql = sql.format(tabela=tabela, **kwargs)
        print(sql)
        df = pd.read_sql(sql, self._engine)
        
        return df

    def get_table_name(self, database, schema, table):
        if table:
            tabela = self._flavor.nome_tabela(database=database, schema=schema, table=table)
        else:
            tabela = ''
        return tabela

        
    