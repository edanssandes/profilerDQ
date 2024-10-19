from profiler_dq import data_types

MAP_TYPES = {
    'date': data_types.DATE,
    'numeric': data_types.NUMERIC,
    'float': data_types.FLOAT,
    'string': data_types.STRING,
    'blob': data_types.BLOB,
}

def nome_tabela(database, schema, table):
    return f"{database}.{table}"

def lista_colunas(database, schema):
    filtro = f"where col.owner = '{database}'" if database else ""
    
    return f"""
        select 
            database_name,
            NULL as schema_name,
            table_name,
            'TABLE' as table_type, 
            column_id, 
            column_name, 
            data_type, 
            data_length, 
            data_precision, 
            data_scale, 
            nullable, 
            num_distinct, 
            num_nulls, 
            sample_size, 
            avg_col_len
         from (

        select col.column_id, 
               col.owner as database_name,
               col.table_name, 
               col.column_name, 
               col.data_type, 
               col.data_length, 
               col.data_precision, 
               col.data_scale, 
               col.nullable,
               col.NUM_DISTINCT,
           low_value, high_value,
               col.SAMPLE_SIZE,
               col.num_nulls,
               col.AVG_COL_LEN,
               col.CHAR_LENGTH,
               col.CHAR_USED

        from sys.all_tab_columns col
        inner join sys.all_tables t on col.owner = t.owner 
                                      and col.table_name = t.table_name
        {filtro}                              
        ) x
        order by database_name, schema_name, table_name, column_id, column_name
"""
    
    

def sample(database, schema, table, colunas, num_registros, sample_size, filtro):
    sample_p = sample_size*100/num_registros
    sample_p = max(0.1, sample_p)

    if num_registros > sample_size:
        sample = f'SAMPLE({sample_p:.2f})'
    else:
        sample = ''
        
    nome_colunas = ", ".join(colunas.column_name)
    where_clause = f"WHERE {filtro}" if filtro else ""
    
    return f'select {nome_colunas} FROM {database}.{table} {where_clause} {sample}'    