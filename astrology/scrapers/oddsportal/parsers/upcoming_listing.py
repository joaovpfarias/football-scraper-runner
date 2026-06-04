"""
Parse da pagina /matches/ do OddsPortal (jogos futuros).
Reutiliza a logica de results_listing, forcando status="scheduled" e score vazio.
"""
from .results_listing import parse as _parse_results


def parse(html: str, base_url: str = "https://www.oddsportal.com") -> list[dict]:
    rows = _parse_results(html, base_url)
    for r in rows:
        r["score_home"] = ""
        r["score_away"] = ""
        r["status"] = "scheduled"
    return rows
