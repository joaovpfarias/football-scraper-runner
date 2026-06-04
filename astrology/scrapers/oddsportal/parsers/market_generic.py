"""
Parser generico de fallback para tabs desconhecidas.
Usa parse_grid_market da base com market_name configuravel.
"""
from ._base import parse_grid_market


def parse(html: str, context: dict, market_name: str = "unknown") -> list[dict]:
    return parse_grid_market(html, context, market=market_name)
