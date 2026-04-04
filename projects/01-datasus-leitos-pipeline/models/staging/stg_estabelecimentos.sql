{{ config(materialized='view') }}

-- dedup por codigo_cnes (fica o mais recente), remove desabilitados, limpa campos texto

with dedup as (
    select *,
        row_number() over (partition by codigo_cnes order by data_atualizacao desc) as rn
    from {{ source('raw', 'raw_estabelecimentos') }}
)

select
    codigo_cnes,
    trim(nome_razao_social) as nome_razao_social,
    trim(nome_fantasia) as nome_fantasia,
    codigo_tipo_unidade,
    case codigo_tipo_unidade
        when 5  then 'Hospital Geral'
        when 7  then 'Hospital Especializado'
        when 15 then 'Unidade Mista'
    end as tipo_unidade,
    trim(descricao_esfera_administrativa) as esfera_administrativa,
    trim(tipo_gestao) as tipo_gestao,
    codigo_uf,
    codigo_municipio,
    codigo_cep_estabelecimento as cep,
    trim(endereco_estabelecimento) as endereco,
    trim(bairro_estabelecimento) as bairro,
    latitude_estabelecimento_decimo_grau as latitude,
    longitude_estabelecimento_decimo_grau as longitude,
    coalesce(estabelecimento_possui_centro_cirurgico, 0)::boolean as tem_centro_cirurgico,
    coalesce(estabelecimento_possui_centro_obstetrico, 0)::boolean as tem_centro_obstetrico,
    coalesce(estabelecimento_possui_centro_neonatal, 0)::boolean as tem_centro_neonatal,
    coalesce(estabelecimento_possui_atendimento_hospitalar, 0)::boolean as tem_atendimento_hospitalar,
    coalesce(estabelecimento_possui_servico_apoio, 0)::boolean as tem_servico_apoio,
    coalesce(estabelecimento_possui_atendimento_ambulatorial, 0)::boolean as tem_atendimento_ambulatorial,
    trim(descricao_turno_atendimento) as turno_atendimento,
    estabelecimento_faz_atendimento_ambulatorial_sus as atendimento_ambulatorial_sus,
    cast(data_atualizacao as date) as data_atualizacao
from dedup
where rn = 1
  and codigo_motivo_desabilitacao_estabelecimento is null
