"""
Parse do mercado Method of Victory (MMA).
Tab: 'method-of-victory'.

Outcomes tipicos: fighter_a_ko, fighter_a_submission, fighter_a_decision,
                  fighter_b_ko, fighter_b_submission, fighter_b_decision, draw.
submarket = descricao do metodo/vencedor do header da coluna.
outcome = 'yes'.
"""
from ._base import parse_grid_market

def parse(html: str, context: dict) -> list[dict]:
    rows = parse_grid_market(html, context, market="method_of_victory")
    for r in rows:
        r["outcome"] = "yes"
    return rows
