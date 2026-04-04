{{ config(materialized='view') }}

-- IBGE usa 7 digitos, DATASUS usa 6 (sem digito verificador).
-- Trunca pra 6 aqui pra facilitar o join.

select
    id_municipio as id_municipio_ibge,
    cast(left(cast(id_municipio as varchar), 6) as integer) as codigo_municipio,
    nome_municipio,
    id_uf,
    sigla_uf,
    nome_uf,
    nome_regiao,
    sigla_regiao
from {{ source('raw', 'raw_municipios') }}
