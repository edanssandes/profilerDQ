# ProfilerDQ

ProfilerDQ is a CLI tool for database profiling and data quality assessment in ETL processes. It generates an xlsx report with metrics for all analyzed tables and columns, based on record sampling. Supporting relational databases, it ensures accurate and efficient quality evaluations.

## Install

Use pip to install ProfilerDQ in your local machine:

pip install git+https://github.com/edanssandes/profilerDQ

## Usage

```
usage: profiler-dq [-h] [--ambiente ID] [--usuario LOGIN] [--database BD] [--schema SCHEMA] [--tables TABELA [TABELA ...]] [--where FILTRO]  
                   [--amostra N] [--output ARQUIVO]

Analisa um banco de dados para auxiliar no processo de ETL.

optional arguments:
  -h, --help            show this help message and exit
  --ambiente ID         id do ambiente: ['sample', ...].
  --usuario LOGIN       nome de usuario.
  --database BD         Nome do banco de dados.
  --schema SCHEMA       Nome do schema ou do owner do banco de dados (opcional).
  --tables TABELA [TABELA ...]
                        Nome das tabelas de interesse (opcional).
  --where FILTRO        Filtro adicional para as tabelas (opcional).
  --amostra N           Número de registros na amostra.
  --output ARQUIVO      Nome do arquivo de saída.
```
