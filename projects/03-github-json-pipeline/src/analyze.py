"""
Gera agregacoes analiticas a partir das tabelas normalizadas.
"""

import logging
from pathlib import Path

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE = Path(__file__).resolve().parent.parent
NORM = BASE / "data" / "normalized"
GOLD = BASE / "data" / "analytics"


def run():
    GOLD.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()

    # registra as tabelas
    for f in NORM.glob("*.parquet"):
        nome = f.stem
        con.sql(f"create view {nome} as select * from read_parquet('{f}')")

    # --- tempo de resolucao por repo ---
    con.sql(f"""
        copy (
            select
                repo,
                count(*) as total_issues,
                sum(case when state = 'closed' then 1 else 0 end) as closed,
                sum(case when is_pull_request then 1 else 0 end) as pull_requests,
                sum(case when not is_pull_request then 1 else 0 end) as issues_only,
                round(avg(resolution_hours), 1) as avg_resolution_hours,
                round(median(resolution_hours), 1) as median_resolution_hours,
                round(avg(case when not is_pull_request then resolution_hours end), 1) as avg_issue_resolution_hours,
                round(avg(comments_count), 1) as avg_comments,
                sum(reactions_total) as total_reactions
            from fct_issues
            group by repo
        ) to '{GOLD}/agg_repos.parquet' (format parquet)
    """)
    log.info("  agg_repos")

    # --- top contributors (por volume de issues/PRs criados) ---
    con.sql(f"""
        copy (
            select
                u.login,
                u.type as user_type,
                count(*) as total_created,
                sum(case when i.is_pull_request then 1 else 0 end) as prs_created,
                sum(case when not i.is_pull_request then 1 else 0 end) as issues_created,
                round(avg(i.resolution_hours), 1) as avg_resolution_hours,
                sum(i.reactions_total) as total_reactions_received
            from fct_issues i
            join dim_users u on i.user_id = u.user_id
            group by u.login, u.type
            order by total_created desc
            limit 50
        ) to '{GOLD}/agg_contributors.parquet' (format parquet)
    """)
    log.info("  agg_contributors")

    # --- labels mais usados e impacto no tempo de resolucao ---
    con.sql(f"""
        copy (
            select
                l.name as label_name,
                l.color,
                count(*) as usage_count,
                round(avg(i.resolution_hours), 1) as avg_resolution_hours,
                round(median(i.resolution_hours), 1) as median_resolution_hours,
                round(avg(i.comments_count), 1) as avg_comments,
                round(avg(i.reactions_total), 1) as avg_reactions
            from bridge_issue_labels bl
            join dim_labels l on bl.label_id = l.label_id
            join fct_issues i on bl.issue_id = i.issue_id
            group by l.name, l.color
            having count(*) >= 5
            order by usage_count desc
        ) to '{GOLD}/agg_labels.parquet' (format parquet)
    """)
    log.info("  agg_labels")

    # --- issues vs PRs ---
    con.sql(f"""
        copy (
            select
                repo,
                is_pull_request,
                count(*) as total,
                round(avg(resolution_hours), 1) as avg_resolution_hours,
                round(median(resolution_hours), 1) as median_resolution_hours,
                round(avg(comments_count), 1) as avg_comments,
                round(avg(n_labels), 1) as avg_labels,
                round(avg(reactions_total), 1) as avg_reactions
            from fct_issues
            group by repo, is_pull_request
            order by repo, is_pull_request
        ) to '{GOLD}/agg_issue_vs_pr.parquet' (format parquet)
    """)
    log.info("  agg_issue_vs_pr")

    con.close()
    log.info("Analise concluida.")


if __name__ == "__main__":
    run()
