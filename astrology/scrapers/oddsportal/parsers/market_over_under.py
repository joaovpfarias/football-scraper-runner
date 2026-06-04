"""
Parse do mercado Over/Under em todas as linhas (1.5, 2.5, 3.5, ...).
Tab: 'over-under'.

Cada linha de O/U tem um container accordion com data-total="2.5" etc.
O parser itera todos os containers e emite (over, under) por bookmaker por linha.
"""
from ._base import parse_collapsed_line_market

def parse(html: str, context: dict) -> list[dict]:
    return parse_collapsed_line_market(html, context, market="over_under", outcomes=["over", "under"])
