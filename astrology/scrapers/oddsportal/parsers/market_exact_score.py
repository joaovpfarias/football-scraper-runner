"""
Parse do mercado Exact Score (tenis - placar de sets, ex: 2-0, 2-1, 0-2, 1-2).
Compartilha a mesma tab 'correct-score' que futebol, mas context.sport = 'tennis'.
"""
from ._base import parse_grid_market

def parse(html: str, context: dict) -> list[dict]:
    rows = parse_grid_market(html, context, market="exact_score")
    return rows
