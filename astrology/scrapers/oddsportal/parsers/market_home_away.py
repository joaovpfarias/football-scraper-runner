"""Parse do mercado Home/Away (moneyline 2-way). Tab: pagina principal."""
from ._base import parse_fixed_outcomes

def parse(html: str, context: dict) -> list[dict]:
    return parse_fixed_outcomes(html, context, market="home_away", outcomes=["home", "away"])
