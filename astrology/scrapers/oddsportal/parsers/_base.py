"""
Funcoes base compartilhadas pelos parsers de mercado.

Estrutura HTML do oddsagora.com.br (2026-04):

Mercados fixos (Home/Away, Odd/Even, etc.):
  [data-testid="over-under-expanded-row"]  <- linha por bookmaker
    [data-testid="outrights-expanded-bookmaker-name"]  <- nome do bookmaker
    [data-testid="odd-container"] x N      <- odds
    [data-testid="payout-container"]       <- payout %

Mercados com linhas (Over/Under, Handicap Asiatico) - visao colapsada:
  [data-testid="over-under-collapsed-row"]  <- uma linha por valor (ex: +199.5)
    [data-testid="over-under-collapsed-option-box"]  <- texto "Acima/Abaixo +199.5"
    div[data-testid="odd-container-default"] x 2    <- container outer (div)
      p[data-testid="odd-container-default"]        <- valor da odd (p)
  Nao ha nome de bookmaker; representa a melhor odd disponivel.
"""
import re
from bs4 import BeautifulSoup
from ..normalizer import parse_odd

ROW_SEL    = '[data-testid="over-under-expanded-row"]'
BM_SEL     = '[data-testid="outrights-expanded-bookmaker-name"]'
ODD_SEL    = '[data-testid="odd-container"]'
PAYOUT_SEL = '[data-testid="payout-container"]'


def _extract_rows(soup: BeautifulSoup) -> list:
    return soup.select(ROW_SEL)


def _parse_row(row, context: dict, market: str, submarket: str,
               outcomes: list[str]) -> list[dict]:
    bm_el = row.select_one(BM_SEL)
    bm = bm_el.get_text(strip=True) if bm_el else ""
    if not bm:
        return []

    odds_cells = row.select(ODD_SEL)
    payout_el  = row.select_one(PAYOUT_SEL)
    payout     = payout_el.get_text(strip=True) if payout_el else ""

    ctx = dict(context)
    ctx["payout_pct"] = payout

    result = []
    for i, outcome in enumerate(outcomes):
        if i >= len(odds_cells):
            break
        closing = parse_odd(odds_cells[i].get_text(strip=True))
        result.append({
            **ctx,
            "market":       market,
            "submarket":    submarket,
            "outcome":      outcome,
            "bookmaker":    bm,
            "odds_opening": "",
            "odds_closing": closing if closing is not None else "",
        })
    return result


def parse_fixed_outcomes(html: str, context: dict, market: str,
                          outcomes: list[str]) -> list[dict]:
    """Mercados simples sem submercados: Home/Away, 1X2, BTTS, etc."""
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for row in _extract_rows(soup):
        rows.extend(_parse_row(row, context, market, "", outcomes))
    return rows


def parse_line_market(html: str, context: dict, market: str,
                       outcomes: list[str]) -> list[dict]:
    """
    Mercados com multiplas linhas: Over/Under, Handicap Asiatico, etc.

    Tenta detectar grupos de linhas por elementos de cabecalho entre os rows.
    Se nao detectar agrupamento, trata tudo como submarket="".
    """
    soup = BeautifulSoup(html, "lxml")
    rows = []

    # Tenta encontrar containers de linha com titulo
    # Procura divs/sections que tenham um titulo + rows filhos
    line_containers = soup.select(
        '[data-testid*="line"], [data-line], [data-total], [data-handicap]'
    )
    if line_containers:
        for c in line_containers:
            line = (c.get("data-line") or c.get("data-total") or
                    c.get("data-handicap") or "")
            if not line:
                title_el = c.select_one("h2, h3, h4, .title, .line-value")
                if title_el:
                    m = re.search(r'([+-]?\d+(?:\.\d+)?)', title_el.get_text())
                    if m:
                        line = m.group(1)
            for row in c.select(ROW_SEL):
                rows.extend(_parse_row(row, context, market, str(line), outcomes))
        if rows:
            return rows

    # Fallback: itera todos os rows e tenta detectar linha via elemento anterior
    all_rows = _extract_rows(soup)
    if not all_rows:
        return rows

    # Verifica se ha elementos de "linha" entre os rows no DOM
    current_line = ""
    for el in soup.find_all(True):
        # Detecta elementos de titulo de linha
        if el.get("data-testid") in ("line-header", "handicap-row", "total-row"):
            m = re.search(r'([+-]?\d+(?:\.\d+)?)', el.get_text())
            if m:
                current_line = m.group(1)
            continue
        # Se for um row de odds, parseia com a linha atual
        if el.get("data-testid") == "over-under-expanded-row":
            rows.extend(_parse_row(el, context, market, current_line, outcomes))

    if not rows:
        # Ultimo fallback: sem linha
        for row in all_rows:
            rows.extend(_parse_row(row, context, market, "", outcomes))
    return rows


COLLAPSED_ROW_SEL    = '[data-testid="over-under-collapsed-row"]'
COLLAPSED_OPTION_SEL = '[data-testid="over-under-collapsed-option-box"]'
COLLAPSED_ODD_SEL    = 'p[data-testid="odd-container-default"]'


def parse_collapsed_line_market(html: str, context: dict, market: str,
                                 outcomes: list[str]) -> list[dict]:
    """
    Mercados com linhas na visao colapsada (Over/Under, Handicap Asiatico).

    Cada collapsed-row exibe a melhor odd disponivel para uma linha.
    Bookmaker = "best" (view agrega todos os bookmakers).
    """
    soup = BeautifulSoup(html, "lxml")
    rows = []

    for row in soup.select(COLLAPSED_ROW_SEL):
        # Extrai valor da linha, ex: "Acima/Abaixo +199.5" -> "199.5"
        opt = row.select_one(COLLAPSED_OPTION_SEL)
        if not opt:
            continue
        # Usa o primeiro <p> (texto completo) para extrair o numero
        first_p = opt.find("p")
        raw_line = first_p.get_text(strip=True) if first_p else opt.get_text(strip=True)
        m = re.search(r'([+-]?\d+(?:\.\d+)?)\s*$', raw_line)
        if not m:
            continue
        line_val = m.group(1)

        # Odds: pega somente os <p> com data-testid="odd-container-default"
        # (ignora os <div> com mesmo testid que sao containers externos)
        odd_ps = row.select(COLLAPSED_ODD_SEL)
        # odd_ps[0] = primeiro outcome, odd_ps[1] = segundo outcome
        for i, outcome in enumerate(outcomes):
            if i >= len(odd_ps):
                break
            closing = parse_odd(odd_ps[i].get_text(strip=True))
            rows.append({
                **context,
                "market":       market,
                "submarket":    line_val,
                "outcome":      outcome,
                "bookmaker":    "best",
                "odds_opening": "",
                "odds_closing": closing if closing is not None else "",
                "payout_pct":   "",
            })

    return rows


def parse_grid_market(html: str, context: dict, market: str) -> list[dict]:
    """
    Mercados em grade: colunas = outcomes/placares, linhas = bookmakers.
    Usa os mesmos rows de over-under-expanded-row: os odd-containers mapeiam
    para headers detectados na pagina.
    """
    soup = BeautifulSoup(html, "lxml")
    rows = []

    # Tenta encontrar cabecalho com outcomes
    header_el = soup.select_one('[data-testid="bookmaker-table-header-line"], thead, .table-header')
    headers = []
    if header_el:
        # Pega celulas apos a primeira (bookmaker)
        cells = header_el.select("th, td, div")
        headers = [c.get_text(strip=True) for c in cells[1:] if c.get_text(strip=True)]

    for row in _extract_rows(soup):
        bm_el = row.select_one(BM_SEL)
        bm = bm_el.get_text(strip=True) if bm_el else ""
        if not bm:
            continue
        odds_cells = row.select(ODD_SEL)
        payout_el  = row.select_one(PAYOUT_SEL)
        payout     = payout_el.get_text(strip=True) if payout_el else ""
        ctx = dict(context)
        ctx["payout_pct"] = payout
        for i, cell in enumerate(odds_cells):
            submarket = headers[i] if i < len(headers) else f"col_{i}"
            closing = parse_odd(cell.get_text(strip=True))
            rows.append({
                **ctx,
                "market":       market,
                "submarket":    submarket,
                "outcome":      "yes",
                "bookmaker":    bm,
                "odds_opening": "",
                "odds_closing": closing if closing is not None else "",
            })
    return rows
