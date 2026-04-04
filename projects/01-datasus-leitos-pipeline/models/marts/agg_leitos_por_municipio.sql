{{ config(materialized='table') }}

select
    l.codigo_municipio,
    l.nome_municipio,
    l.sigla_uf,
    l.nome_uf,
    l.populacao,
    count(*) as total_hospitais,
    sum(f.tem_centro_cirurgico::int) as hospitais_com_centro_cirurgico,
    sum(f.tem_centro_obstetrico::int) as hospitais_com_centro_obstetrico,
    sum(f.tem_centro_neonatal::int) as hospitais_com_centro_neonatal,
    sum(f.tem_atendimento_hospitalar::int) as hospitais_com_atend_hospitalar,
    round(100000.0 * count(*) / nullif(l.populacao, 0), 2) as hospitais_por_100k_hab,
    round(avg(f.score_complexidade), 2) as score_complexidade_medio
from {{ ref('fct_leitos') }} f
join {{ ref('dim_localidades') }} l using (codigo_municipio)
group by l.codigo_municipio, l.nome_municipio, l.sigla_uf, l.nome_uf, l.populacao
order by total_hospitais desc
