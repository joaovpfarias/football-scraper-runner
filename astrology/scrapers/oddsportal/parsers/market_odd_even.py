"""Parse do mercado Odd or Even. Tab: 'odd-or-even'."""
from ._base import parse_fixed_outcomes

def parse(html: str, context: dict) -> list[dict]:
    return parse_fixed_outcomes(html, context, market="odd_even", outcomes=["odd", "even"])
