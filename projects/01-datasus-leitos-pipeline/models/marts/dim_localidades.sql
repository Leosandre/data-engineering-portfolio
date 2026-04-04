{{ config(materialized='table') }}

select
    m.codigo_municipio,
    m.nome_municipio,
    m.sigla_uf,
    m.nome_uf,
    m.sigla_regiao,
    m.nome_regiao,
    p.populacao
from {{ ref('stg_municipios') }} m
left join {{ ref('stg_populacao') }} p using (codigo_municipio)
