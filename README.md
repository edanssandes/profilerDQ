# ProfilerDQ

ProfilerDQ is a CLI tool for database profiling and data quality assessment in ETL processes. It generates an xlsx report with metrics for all analyzed tables and columns, based on record sampling. Supporting relational databases, it ensures accurate and efficient quality evaluations.

## Install

Use pip to install ProfilerDQ in your local machine:

pip install git+https://github.com/edanssandes/profilerDQ

## Usage

```
usage: explorar.py [-h] [--ambiente ID] [--usuario LOGIN] [--database BD] [--schema SCHEMA] [--output ARQUIVO]

Analisa um banco de dados para auxiliar no processo de ETL.

options:
  -h, --help        show this help message and exit
  --ambiente ID     id do ambiente: 1:LabContas; 2:RepTCU; 3:RepTCU-Desenvol; 4:RepTCU-Desenvol-Finep.
  --usuario LOGIN   nome de usuario.
  --database BD     Nome do banco de dados.
  --schema SCHEMA   Nome do schema ou do owner do banco de dados (opcional).
  --output ARQUIVO  Nome do arquivo de saída.
```
