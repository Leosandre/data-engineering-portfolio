"""
Ingere issues e repos da API do GitHub.
Salva JSON bruto em data/raw/ como JSONL (1 JSON por linha).

Lida com:
- Paginacao via Link header (cursor-based)
- Rate limiting (60 req/hora sem token, 5000 com token)
- Cache local (nao re-baixa se o arquivo ja existe)
"""

import json
import logging
import os
import re
import time
from pathlib import Path

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE = Path(__file__).resolve().parent.parent
RAW = BASE / "data" / "raw"
API = "https://api.github.com"

# repos com issues ricas (labels, milestones, assignees)
REPOS = [
    "kubernetes/kubernetes",
    "apache/spark",
    "microsoft/vscode",
]

ISSUES_PER_REPO = 500  # max issues por repo (pra nao estourar rate limit)
PER_PAGE = 100

# token opcional via env var (sem token = 60 req/hora)
TOKEN = os.environ.get("GITHUB_TOKEN", "")


def _headers():
    h = {"Accept": "application/vnd.github.v3+json"}
    if TOKEN:
        h["Authorization"] = f"Bearer {TOKEN}"
    return h


def _parse_link_next(headers):
    """Extrai URL da proxima pagina do header Link."""
    link = headers.get("link", "")
    m = re.search(r'<([^>]+)>;\s*rel="next"', link)
    return m.group(1) if m else None


def _check_rate_limit(headers):
    """Se rate limit estiver baixo, espera ate resetar."""
    remaining = int(headers.get("x-ratelimit-remaining", 999))
    if remaining <= 2:
        reset = int(headers.get("x-ratelimit-reset", 0))
        wait = max(reset - int(time.time()), 0) + 5
        log.warning("Rate limit quase esgotado (%d restantes). Esperando %ds...", remaining, wait)
        time.sleep(wait)


def ingerir_issues(client, repo):
    """Baixa issues (inclui PRs) de um repo com paginacao."""
    slug = repo.replace("/", "_")
    saida = RAW / f"issues_{slug}.jsonl"

    if saida.exists() and saida.stat().st_size > 0:
        linhas = sum(1 for _ in open(saida))
        log.info("  %s: cache encontrado (%d issues), pulando", repo, linhas)
        return

    log.info("  %s: baixando issues...", repo)
    url = f"{API}/repos/{repo}/issues"
    params = {"state": "all", "per_page": PER_PAGE, "sort": "created", "direction": "desc"}

    total = 0
    with open(saida, "w") as f:
        while url and total < ISSUES_PER_REPO:
            resp = client.get(url, params=params if total == 0 else None)
            resp.raise_for_status()
            _check_rate_limit(resp.headers)

            issues = resp.json()
            if not issues:
                break

            for issue in issues:
                f.write(json.dumps(issue, ensure_ascii=False) + "\n")
                total += 1
                if total >= ISSUES_PER_REPO:
                    break

            url = _parse_link_next(resp.headers)
            params = None  # params ja estao na URL do Link

    log.info("  %s: %d issues salvas", repo, total)


def ingerir_repos(client):
    """Baixa metadados dos repos."""
    saida = RAW / "repos.jsonl"

    log.info("Baixando metadados dos repos...")
    with open(saida, "w") as f:
        for repo in REPOS:
            resp = client.get(f"{API}/repos/{repo}")
            resp.raise_for_status()
            _check_rate_limit(resp.headers)
            f.write(json.dumps(resp.json(), ensure_ascii=False) + "\n")
            log.info("  %s: ok", repo)


if __name__ == "__main__":
    RAW.mkdir(parents=True, exist_ok=True)

    with httpx.Client(headers=_headers(), timeout=30, follow_redirects=True) as client:
        # checa rate limit antes de comecar
        resp = client.get(f"{API}/rate_limit")
        rate = resp.json()["rate"]
        log.info("Rate limit: %d/%d (reset em %ds)",
                 rate["remaining"], rate["limit"],
                 max(rate["reset"] - int(time.time()), 0))

        if rate["remaining"] < 10 and not TOKEN:
            log.error("Rate limit muito baixo e sem token. Defina GITHUB_TOKEN ou espere.")
            exit(1)

        ingerir_repos(client)
        for repo in REPOS:
            ingerir_issues(client, repo)

    log.info("Ingestao concluida.")
