{{ config(materialized='table') }}

-- score_complexidade = soma das 6 capacidades binarias (0 a 6)

select
    codigo_cnes,
    codigo_municipio,
    codigo_tipo_unidade,
    tipo_unidade,
    tem_centro_cirurgico,
    tem_centro_obstetrico,
    tem_centro_neonatal,
    tem_atendimento_hospitalar,
    tem_servico_apoio,
    tem_atendimento_ambulatorial,
    (tem_centro_cirurgico::int + tem_centro_obstetrico::int + tem_centro_neonatal::int
     + tem_atendimento_hospitalar::int + tem_servico_apoio::int
     + tem_atendimento_ambulatorial::int) as score_complexidade
from {{ ref('stg_estabelecimentos') }}
