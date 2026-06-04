"""
Parse do mercado Total Sets Over/Under (tenis, volei).
Tab: 'total-sets'.
"""
from ._base import parse_line_market

def parse(html: str, context: dict) -> list[dict]:
    return parse_line_market(html, context, market="total_sets", outcomes=["over", "under"])
