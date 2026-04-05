"""
Flatten de JSONs aninhados da API do GitHub em tabelas relacionais.

Um JSON de issue vira 7 tabelas:
  fct_issues          - 1 linha por issue (campos flat + FKs)
  dim_users           - usuarios unicos (extraidos de user, assignees)
  dim_labels          - labels unicos
  dim_milestones      - milestones unicos
  dim_repos           - repos com metadados
  bridge_issue_labels - N:N entre issues e labels
  bridge_issue_assignees - N:N entre issues e assignees
"""

import json
import logging
from datetime import datetime
from pathlib import Path

import polars as pl

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE = Path(__file__).resolve().parent.parent
RAW = BASE / "data" / "raw"
NORM = BASE / "data" / "normalized"


def _ler_jsonl(caminho):
    with open(caminho) as f:
        return [json.loads(line) for line in f if line.strip()]


def _ts(val):
    """Parse ISO timestamp ou None."""
    if not val:
        return None
    try:
        return datetime.fromisoformat(val.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def flatten_issues():
    log.info("Flatten de issues...")

    # le todos os JSONL de issues
    issues_raw = []
    for f in sorted(RAW.glob("issues_*.jsonl")):
        repo_slug = f.stem.replace("issues_", "").replace("_", "/", 1)
        for issue in _ler_jsonl(f):
            issue["_repo"] = repo_slug
            issues_raw.append(issue)

    log.info("  %d issues brutas carregadas", len(issues_raw))

    # --- fct_issues (flatten do nivel raiz + objetos aninhados) ---
    fct_rows = []
    all_users = {}       # id -> dict
    all_labels = {}      # id -> dict
    all_milestones = {}  # id -> dict
    bridge_labels = []   # (issue_id, label_id)
    bridge_assignees = []  # (issue_id, user_id)

    for raw in issues_raw:
        issue_id = raw["id"]

        # user (objeto aninhado)
        user = raw.get("user") or {}
        if user.get("id"):
            all_users[user["id"]] = {
                "user_id": user["id"],
                "login": user.get("login"),
                "type": user.get("type"),
                "site_admin": user.get("site_admin", False),
            }

        # labels (array de objetos)
        for label in raw.get("labels") or []:
            if label.get("id"):
                all_labels[label["id"]] = {
                    "label_id": label["id"],
                    "name": label.get("name"),
                    "color": label.get("color"),
                    "description": label.get("description"),
                }
                bridge_labels.append({"issue_id": issue_id, "label_id": label["id"]})

        # assignees (array de objetos)
        for assignee in raw.get("assignees") or []:
            if assignee.get("id"):
                all_users[assignee["id"]] = {
                    "user_id": assignee["id"],
                    "login": assignee.get("login"),
                    "type": assignee.get("type"),
                    "site_admin": assignee.get("site_admin", False),
                }
                bridge_assignees.append({"issue_id": issue_id, "user_id": assignee["id"]})

        # milestone (objeto aninhado opcional)
        ms = raw.get("milestone")
        ms_id = None
        if ms and ms.get("id"):
            ms_id = ms["id"]
            all_milestones[ms_id] = {
                "milestone_id": ms_id,
                "title": ms.get("title"),
                "state": ms.get("state"),
                "due_on": _ts(ms.get("due_on")),
                "open_issues": ms.get("open_issues"),
                "closed_issues": ms.get("closed_issues"),
            }

        # reactions (objeto com chaves dinamicas -> colunas flat)
        rx = raw.get("reactions") or {}

        # pull_request (objeto opcional -> flag booleano)
        pr = raw.get("pull_request")

        created = _ts(raw.get("created_at"))
        closed = _ts(raw.get("closed_at"))

        fct_rows.append({
            "issue_id": issue_id,
            "repo": raw["_repo"],
            "number": raw.get("number"),
            "title": raw.get("title"),
            "state": raw.get("state"),
            "is_pull_request": pr is not None,
            "user_id": user.get("id"),
            "milestone_id": ms_id,
            "comments_count": raw.get("comments", 0),
            "created_at": created,
            "updated_at": _ts(raw.get("updated_at")),
            "closed_at": closed,
            "resolution_hours": round((closed - created).total_seconds() / 3600, 1) if created and closed else None,
            "author_association": raw.get("author_association"),
            "is_locked": raw.get("locked", False),
            "reactions_total": rx.get("total_count", 0),
            "reactions_thumbs_up": rx.get("+1", 0),
            "reactions_thumbs_down": rx.get("-1", 0),
            "reactions_heart": rx.get("heart", 0),
            "reactions_rocket": rx.get("rocket", 0),
            "n_labels": len(raw.get("labels") or []),
            "n_assignees": len(raw.get("assignees") or []),
        })

    # --- Salvar tudo ---
    NORM.mkdir(parents=True, exist_ok=True)

    df = pl.DataFrame(fct_rows, infer_schema_length=len(fct_rows))
    df.write_parquet(NORM / "fct_issues.parquet")
    log.info("  fct_issues: %d registros", len(df))

    df = pl.DataFrame(list(all_users.values()))
    df.write_parquet(NORM / "dim_users.parquet")
    log.info("  dim_users: %d usuarios unicos", len(df))

    df = pl.DataFrame(list(all_labels.values()))
    df.write_parquet(NORM / "dim_labels.parquet")
    log.info("  dim_labels: %d labels unicos", len(df))

    df = pl.DataFrame(list(all_milestones.values()))
    if len(df) > 0:
        df.write_parquet(NORM / "dim_milestones.parquet")
    else:
        pl.DataFrame({"milestone_id": [], "title": []}).write_parquet(NORM / "dim_milestones.parquet")
    log.info("  dim_milestones: %d milestones", len(df))

    df = pl.DataFrame(bridge_labels)
    df.write_parquet(NORM / "bridge_issue_labels.parquet")
    log.info("  bridge_issue_labels: %d relacoes", len(df))

    df = pl.DataFrame(bridge_assignees)
    if len(df) > 0:
        df.write_parquet(NORM / "bridge_issue_assignees.parquet")
    else:
        pl.DataFrame({"issue_id": [], "user_id": []}).write_parquet(NORM / "bridge_issue_assignees.parquet")
    log.info("  bridge_issue_assignees: %d relacoes", len(df))


def flatten_repos():
    log.info("Flatten de repos...")
    repos_raw = _ler_jsonl(RAW / "repos.jsonl")

    rows = []
    for r in repos_raw:
        owner = r.get("owner") or {}
        lic = r.get("license") or {}
        rows.append({
            "repo_id": r["id"],
            "full_name": r.get("full_name"),
            "description": r.get("description"),
            "language": r.get("language"),
            "stargazers_count": r.get("stargazers_count", 0),
            "forks_count": r.get("forks_count", 0),
            "open_issues_count": r.get("open_issues_count", 0),
            "owner_login": owner.get("login"),
            "owner_type": owner.get("type"),
            "license_name": lic.get("name"),
            "created_at": _ts(r.get("created_at")),
            "topics": ",".join(r.get("topics") or []),
        })

    df = pl.DataFrame(rows)
    df.write_parquet(NORM / "dim_repos.parquet")
    log.info("  dim_repos: %d repos", len(df))


if __name__ == "__main__":
    flatten_issues()
    flatten_repos()
    log.info("Flatten concluido.")
