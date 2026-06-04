"""Parse do mercado Both Teams to Score (BTTS). Tab: 'both-teams-to-score'."""
from ._base import parse_fixed_outcomes

def parse(html: str, context: dict) -> list[dict]:
    return parse_fixed_outcomes(html, context, market="btts", outcomes=["yes", "no"])
