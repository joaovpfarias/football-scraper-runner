"""Parse do mercado Double Chance (1X / 12 / X2). Tab: 'double-chance'."""
from ._base import parse_fixed_outcomes

def parse(html: str, context: dict) -> list[dict]:
    return parse_fixed_outcomes(html, context, market="double_chance", outcomes=["1x", "12", "x2"])
