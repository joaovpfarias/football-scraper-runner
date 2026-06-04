"""
Parse do mercado Half-time / Full-time (9 combinacoes HT x FT).
Tab: 'half-time-full-time'.

Cada combinacao usa o codigo "HT:{H|D|A};FT:{H|D|A}" como submarket.
outcome = 'yes', uma linha por (bookmaker, combinacao).
"""
from ._base import parse_grid_market


def parse(html: str, context: dict) -> list[dict]:
    rows = parse_grid_market(html, context, market="ht_ft")
    for r in rows:
        r["outcome"] = "yes"
    return rows
