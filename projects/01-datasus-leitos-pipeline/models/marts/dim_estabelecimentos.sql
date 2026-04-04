{{ config(materialized='table') }}

select
    codigo_cnes,
    nome_fantasia,
    nome_razao_social,
    tipo_unidade,
    esfera_administrativa,
    tipo_gestao,
    codigo_municipio,
    cep,
    endereco,
    bairro,
    latitude,
    longitude,
    turno_atendimento,
    atendimento_ambulatorial_sus,
    data_atualizacao
from {{ ref('stg_estabelecimentos') }}
