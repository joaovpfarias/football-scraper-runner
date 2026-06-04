"""
Parse do mercado European Handicap (3-way handicap).
Tab: 'european-handicap'.
"""
from ._base import parse_line_market

def parse(html: str, context: dict) -> list[dict]:
    return parse_line_market(html, context, market="european_handicap",
                             outcomes=["home", "draw", "away"])
