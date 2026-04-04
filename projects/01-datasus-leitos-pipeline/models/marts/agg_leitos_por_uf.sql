{{ config(materialized='table') }}

-- populacao vem da dim_localidades (1 linha por municipio), nao do join com fct
-- senao multiplica a populacao pelo numero de hospitais do municipio

with hospitais_uf as (
    select
        l.sigla_uf,
        count(*) as total_hospitais,
        sum(f.tem_centro_cirurgico::int) as hospitais_com_centro_cirurgico,
        sum(f.tem_centro_obstetrico::int) as hospitais_com_centro_obstetrico,
        sum(f.tem_centro_neonatal::int) as hospitais_com_centro_neonatal,
        sum(f.tem_atendimento_hospitalar::int) as hospitais_com_atend_hospitalar,
        round(100.0 * sum(f.tem_centro_cirurgico::int) / count(*), 1) as pct_centro_cirurgico,
        round(100.0 * sum(f.tem_centro_neonatal::int) / count(*), 1) as pct_centro_neonatal,
        round(avg(f.score_complexidade), 2) as score_complexidade_medio
    from {{ ref('fct_leitos') }} f
    join {{ ref('dim_localidades') }} l using (codigo_municipio)
    group by l.sigla_uf
),

pop_uf as (
    select sigla_uf, nome_uf, nome_regiao, sum(populacao) as populacao
    from {{ ref('dim_localidades') }}
    group by sigla_uf, nome_uf, nome_regiao
)

select
    p.sigla_uf,
    p.nome_uf,
    p.nome_regiao,
    p.populacao,
    h.total_hospitais,
    h.hospitais_com_centro_cirurgico,
    h.hospitais_com_centro_obstetrico,
    h.hospitais_com_centro_neonatal,
    h.hospitais_com_atend_hospitalar,
    h.pct_centro_cirurgico,
    h.pct_centro_neonatal,
    round(100000.0 * h.total_hospitais / nullif(p.populacao, 0), 2) as hospitais_por_100k_hab,
    h.score_complexidade_medio
from hospitais_uf h
join pop_uf p using (sigla_uf)
order by h.total_hospitais desc
