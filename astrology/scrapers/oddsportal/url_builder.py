"""
Constroi URLs do OddsPortal.

Estrutura observada no site:
  - Listagem resultados liga atual: https://www.oddsportal.com/{sport_slug}/{league_path}/results/
  - Listagem resultados season antiga: https://www.oddsportal.com/{sport_slug}/{league_path}-{YYYY-YYYY}/results/
  - Paginacao: append "#/page/{N}/" (JS-driven; ler via Playwright)
  - Listagem futuros: https://www.oddsportal.com/matches/{sport_slug}/
  - Match page principal: https://www.oddsportal.com/{sport_slug}/{league_path}/{match-slug}/
  - Match page tab: https://www.oddsportal.com/{sport_slug}/{league_path}/{match-slug}/#{tab-slug}
"""

# oddsportal.com redireciona IPs do Brasil para oddsagora.com.br (mesmo conteudo, mesmo path)
BASE = "https://www.oddsagora.com.br"


def build_results_url(sport_slug: str, league_path: str, season: str | None) -> str:
    """
    Ex: build_results_url("basketball", "usa/nba", "2023-2024")
      -> "https://www.oddsportal.com/basketball/usa/nba-2023-2024/results/"
        build_results_url("basketball", "usa/nba", None)
      -> "https://www.oddsportal.com/basketball/usa/nba/results/"
    """
    if season:
        return f"{BASE}/{sport_slug}/{league_path}-{season}/results/"
    return f"{BASE}/{sport_slug}/{league_path}/results/"


def build_upcoming_url(sport_slug: str, league_path: str | None = None) -> str:
    if league_path:
        return f"{BASE}/{sport_slug}/{league_path}/"
    return f"{BASE}/matches/{sport_slug}/"


def build_results_page_url(base_results_url: str, page: int) -> str:
    """
    OP usa hash para paginacao. Retorna URL com #/page/N/.
    """
    return f"{base_results_url}#/page/{page}/"


def build_match_tab_url(match_url: str, tab_slug: str) -> str:
    """
    Ex: build_match_tab_url("https://www.oddsportal.com/basketball/usa/nba/lakers-celtics/", "over-under")
      -> "https://www.oddsportal.com/basketball/usa/nba/lakers-celtics/#over-under"

    Para tab vazia (mercado principal), retorna match_url sem alteracao.
    """
    if not tab_slug:
        return match_url
    return f"{match_url.rstrip('/')}/#{tab_slug}"
