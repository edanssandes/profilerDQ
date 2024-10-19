from profiler_dq import data_types


MAP_TYPES = {
    'date': data_types.DATE,
    'datetime': data_types.DATE,
    'datetime2': data_types.DATE,
    'time': data_types.DATE,
    
    'bigint': data_types.NUMERIC,
    'numeric': data_types.NUMERIC,
    'bit': data_types.NUMERIC,
    'smallint': data_types.NUMERIC,
    'decimal': data_types.NUMERIC,
    'int': data_types.NUMERIC,

    'float': data_types.FLOAT,
    'real': data_types.FLOAT,

    'char': data_types.STRING,
    'varchar': data_types.STRING,
    'text': data_types.STRING,
    'nchar': data_types.STRING,
    'nvarchar': data_types.STRING,
    'ntext': data_types.STRING,
    
    'binary': data_types.BLOB,
    'varbinary': data_types.BLOB,
    'image': data_types.BLOB,
}

def nome_tabela(database, schema, table):
    if not schema:
        schema = 'dbo'
    return f"[{database}].{schema}.[{table}]"

def lista_colunas(database, schema):
    return """
        select 
            DB_NAME() as database_name,
            schema_name(tab.schema_id) as schema_name,
            tab.name as table_name, 
            tab.table_type,
            col.column_id,
            col.name as column_name, 
            t.name as data_type,    
            col.max_length,
            col.precision
        from (
            select object_id, schema_id, name, 'TABLE' as table_type from sys.tables 
            union all
            select object_id, schema_id, name, 'VIEW' as table_type from sys.views
        ) as tab
        inner join sys.columns as col
            on tab.object_id = col.object_id
        left join sys.types as t
            on col.user_type_id = t.user_type_id
        order by schema_name,
            table_name, 
            column_id;
    """

def sample(database, schema, table, colunas, num_registros, sample_size, filtro):
    table_type = colunas.table_type.iloc[0]

    nome_colunas = ", ".join([f'[{x}]' for x in colunas.column_name])
        
    where_clause = f"WHERE {filtro}" if filtro else ""

    if num_registros <= sample_size:
        return f'select {nome_colunas} FROM {nome_tabela(database, schema, table)} {where_clause}'
    else:
        if table_type == 'TABLE':
            return f'select {nome_colunas} FROM {nome_tabela(database, schema, table)} TABLESAMPLE({sample_size} rows) {where_clause}'
        elif table_type == 'VIEW':
            # Se for VIEW, não é possível usar TABLESAMPLE. Embora seja bem mais lento, o NEWID() pode ser usado para embaralhar os registros.
            return f'select top {sample_size} {nome_colunas} FROM {nome_tabela(database, schema, table)} {where_clause} order by NEWID()'
