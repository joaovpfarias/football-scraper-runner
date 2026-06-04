"""
Parse do mercado Correct Score (futebol).
Tab: 'correct-score'.

Estrutura: tabela onde linhas = bookmakers, colunas = placares (1-0, 2-0, ...).
Emite uma linha por (bookmaker, placar). submarket = placar, outcome = 'yes'.
"""
from ._base import parse_grid_market

def parse(html: str, context: dict) -> list[dict]:
    return parse_grid_market(html, context, market="correct_score")
