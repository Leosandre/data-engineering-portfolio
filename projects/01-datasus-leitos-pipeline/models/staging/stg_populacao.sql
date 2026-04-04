{{ config(materialized='view') }}

-- populacao por municipio (Censo 2022), codigo truncado pra 6 digitos

select
    cast(left(cast(id_municipio as varchar), 6) as integer) as codigo_municipio,
    populacao,
    ano
from {{ source('raw', 'raw_populacao') }}
where populacao is not null
