"""
Parse do mercado 1X2 (Home / Draw / Away).
Tab: pagina principal (slug '').
"""
from ._base import parse_fixed_outcomes

def parse(html: str, context: dict) -> list[dict]:
    return parse_fixed_outcomes(html, context, market="1x2", outcomes=["home", "draw", "away"])
