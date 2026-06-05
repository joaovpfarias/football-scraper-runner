"""
Catalogo de mercados.

tab_label: label da tab de mercado (como aparece na UI do oddsagora.com.br).
tab_slug:  slug de URL legado (mantido para referencia/cache).
parser_module: modulo em parsers/.
"""

MARKETS = {
    "1x2":               {"tab_label": "1X2",                        "tab_slug": "",                     "parser_module": "market_1x2",               "has_submarket_lines": False},
    "home_away":         {"tab_label": "Home/Away",                   "tab_slug": "",                     "parser_module": "market_home_away",         "has_submarket_lines": False},
    "over_under":        {"tab_label": "Acima/Abaixo",                "tab_slug": "over-under",           "parser_module": "market_over_under",        "has_submarket_lines": True},
    "asian_handicap":    {"tab_label": "Handicap Asiático",           "tab_slug": "asian-handicap",       "parser_module": "market_asian_handicap",    "has_submarket_lines": True},
    "european_handicap": {"tab_label": "Handicap Europeu",            "tab_slug": "european-handicap",    "parser_module": "market_european_handicap", "has_submarket_lines": True},
    "draw_no_bet":       {"tab_label": "Draw No Bet",                 "tab_slug": "draw-no-bet",          "parser_module": "market_draw_no_bet",       "has_submarket_lines": False},
    "double_chance":     {"tab_label": "Double Chance",               "tab_slug": "double-chance",        "parser_module": "market_double_chance",     "has_submarket_lines": False},
    "btts":              {"tab_label": "Ambas equipes marcam",        "tab_slug": "both-teams-to-score",  "parser_module": "market_btts",              "has_submarket_lines": False},
    "odd_even":          {"tab_label": "Par ou Ímpar",                "tab_slug": "odd-or-even",          "parser_module": "market_odd_even",          "has_submarket_lines": False},
    "correct_score":     {"tab_label": "Resultado correto",           "tab_slug": "correct-score",        "parser_module": "market_correct_score",     "has_submarket_lines": False},
    "ht_ft":             {"tab_label": "Intervalo/Final do jogo",     "tab_slug": "half-time-full-time",  "parser_module": "market_ht_ft",             "has_submarket_lines": False},
    "total_sets":        {"tab_label": "Total de Sets",               "tab_slug": "total-sets",           "parser_module": "market_total_sets",        "has_submarket_lines": True},
    "total_games":       {"tab_label": "Total de Games",              "tab_slug": "total-games",          "parser_module": "market_total_games",       "has_submarket_lines": True},
    "exact_score":       {"tab_label": "Resultado correto",           "tab_slug": "correct-score",        "parser_module": "market_exact_score",       "has_submarket_lines": False},
    "placar_exato":      {"tab_label": "Placar exato",                "tab_slug": "correct-score",        "parser_module": "market_exact_score",       "has_submarket_lines": False},
    "method_of_victory": {"tab_label": "Método de Vitória",          "tab_slug": "method-of-victory",    "parser_module": "market_method_of_victory", "has_submarket_lines": False},
}


def get_market(market_key: str) -> dict:
    if market_key not in MARKETS:
        raise KeyError(f"Mercado desconhecido: {market_key}. Validos: {list(MARKETS)}")
    return MARKETS[market_key]
