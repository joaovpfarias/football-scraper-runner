"""Parse do mercado Draw No Bet. Tab: 'draw-no-bet'."""
from ._base import parse_fixed_outcomes

def parse(html: str, context: dict) -> list[dict]:
    return parse_fixed_outcomes(html, context, market="draw_no_bet", outcomes=["home", "away"])
