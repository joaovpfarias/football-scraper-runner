"""
Parse do mercado Total Games Over/Under (tenis - games totais do match).
Tab: 'total-games'.
"""
from ._base import parse_line_market

def parse(html: str, context: dict) -> list[dict]:
    return parse_line_market(html, context, market="total_games", outcomes=["over", "under"])
