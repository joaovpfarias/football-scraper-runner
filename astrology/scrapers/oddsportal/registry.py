"""
Catalogo de esportes e ligas suportadas no OddsPortal.

SPORTS: chave = nome canonico usado pelo scraper, valor = metadados.
  - slug: caminho no OP (ex: "football" para soccer, "basketball", etc)
  - has_draw: se o mercado 1X2 eh aplicavel (True) ou apenas H/A (False)
  - default_markets: lista de mercados coletados quando --markets all.

LEAGUES: atalhos. Usuario pode passar --league nba ou --league-slug usa/nba.
  - sport: chave de SPORTS
  - path: sub-caminho no OP depois do slug esportivo (ex: "usa/nba")
"""

SPORTS = {
    "soccer": {
        "slug": "football",
        "has_draw": True,
        "default_markets": [
            "1x2", "over_under", "btts", "double_chance", "draw_no_bet",
            "european_handicap", "asian_handicap", "correct_score", "ht_ft", "odd_even",
        ],
    },
    "basketball": {
        "slug": "basketball",
        "has_draw": False,
        "default_markets": ["home_away", "asian_handicap", "over_under", "odd_even"],
    },
    "tennis": {
        "slug": "tennis",
        "has_draw": False,
        "default_markets": [
            "home_away", "asian_handicap", "total_games", "total_sets",
            "exact_score", "correct_score",
        ],
    },
    "mma": {
        "slug": "mma",
        "has_draw": False,
        "default_markets": ["home_away", "over_under", "method_of_victory"],
    },
    "hockey": {
        "slug": "hockey",
        "has_draw": True,
        "default_markets": [
            "1x2", "home_away", "double_chance", "draw_no_bet", "btts", "over_under",
        ],
    },
    "baseball": {
        "slug": "baseball",
        "has_draw": False,
        "default_markets": ["home_away", "over_under", "asian_handicap"],
    },
    "american-football": {
        "slug": "american-football",
        "has_draw": False,
        "default_markets": ["home_away", "over_under", "asian_handicap"],
    },
    "rugby-union": {
        "slug": "rugby-union",
        "has_draw": True,
        "default_markets": ["1x2", "home_away", "double_chance", "draw_no_bet", "over_under"],
    },
    "rugby-league": {
        "slug": "rugby-league",
        "has_draw": True,
        "default_markets": ["1x2", "home_away", "double_chance", "draw_no_bet", "over_under"],
    },
    "handball": {
        "slug": "handball",
        "has_draw": True,
        "default_markets": ["1x2", "home_away", "over_under", "asian_handicap"],
    },
    "volleyball": {
        "slug": "volleyball",
        "has_draw": False,
        "default_markets": ["home_away", "total_sets", "asian_handicap"],
    },
}

LEAGUES = {
    "nba":            {"sport": "basketball",        "path": "usa/nba"},
    "euroleague":     {"sport": "basketball",        "path": "europe/euroleague"},
    "premier-league": {"sport": "soccer",            "path": "england/premier-league"},
    "la-liga":        {"sport": "soccer",            "path": "spain/laliga"},
    "serie-a":        {"sport": "soccer",            "path": "italy/serie-a"},
    "bundesliga":     {"sport": "soccer",            "path": "germany/bundesliga"},
    "ligue-1":        {"sport": "soccer",            "path": "france/ligue-1"},
    "atp":            {"sport": "tennis",            "path": "atp-singles"},
    "wta":            {"sport": "tennis",            "path": "wta-singles"},
    "ufc":            {"sport": "mma",               "path": "ufc"},
    "nhl":            {"sport": "hockey",            "path": "usa/nhl"},
    "mlb":            {"sport": "baseball",          "path": "usa/mlb"},
    "nfl":            {"sport": "american-football", "path": "usa/nfl"},
}


def resolve_league(sport_key: str, league_or_slug: str) -> str:
    """
    Retorna o path da liga (ex: 'usa/nba').
    Aceita tanto atalho ('nba') quanto slug cru ('usa/nba', 'germany/bundesliga').
    """
    if league_or_slug in LEAGUES:
        entry = LEAGUES[league_or_slug]
        if entry["sport"] != sport_key:
            raise ValueError(
                f"Liga '{league_or_slug}' pertence ao esporte '{entry['sport']}', "
                f"nao '{sport_key}'"
            )
        return entry["path"]
    return league_or_slug.strip("/")
