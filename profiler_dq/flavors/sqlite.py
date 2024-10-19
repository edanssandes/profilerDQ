from profiler_dq import data_types


MAP_TYPES = {
    'date': data_types.DATE,
    'integer': data_types.NUMERIC,
    'real': data_types.FLOAT,
    'text': data_types.STRING,
    'blob': data_types.BLOB,
}


def nome_tabela(database, schema, table):
    return f"main.{table}"

def lista_colunas(database, schema):
    return """
    WITH all_tables AS (
        SELECT name AS table_name 
        FROM sqlite_master 
        WHERE type = 'table'
    )
    SELECT 
    '' as database_name,
    '' as schema_name,
    table_name, 
    'TABLE' as table_type,
    cid,
    name AS column_name, 
    type AS data_type,
    [notnull]
    FROM all_tables, pragma_table_info(table_name)
    ORDER BY table_name, column_name
    """    

def sample(database, schema, table, colunas, num_registros, sample_size, filtro):
    print(colunas.table_type.iloc[0])

    nome_colunas = ", ".join(colunas.column_name)

    where_clause = f"WHERE {filtro}" if filtro else ""

    return f"select {nome_colunas} FROM {table} {where_clause} ORDER BY RANDOM() LIMIT {sample_size}"