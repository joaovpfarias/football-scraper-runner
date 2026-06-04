"""
Parse do mercado Asian Handicap em todas as linhas (-2, -1.5, -1, -0.5, 0, +0.5, ...).
Tab: 'asian-handicap'.
"""
from ._base import parse_collapsed_line_market

def parse(html: str, context: dict) -> list[dict]:
    return parse_collapsed_line_market(html, context, market="asian_handicap", outcomes=["home", "away"])
