"""
Extrai informacoes do header da match page do oddsagora.com.br.

Seletores (2026-05):
  [data-testid="game-host"]      - jogador/time da casa
  [data-testid="game-guest"]     - jogador/time visitante
  [data-testid="game-time-item"] - "Sabado, 01 Out 2022, 10:10"
  [data-testid="live-info"]      - "Resultado final <strong>2:1</strong> (sets/placar)"
                                   Para esportes com placar simples (futebol/basquete):
                                   texto livre com padrao "N - N" ou "N:N"
"""
import re
from bs4 import BeautifulSoup
from ..normalizer import normalize_team_name, parse_pt_datetime


def parse(html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")

    # Times
    host_el  = soup.select_one('[data-testid="game-host"]')
    guest_el = soup.select_one('[data-testid="game-guest"]')
    home = normalize_team_name(host_el.get_text()  if host_el  else "")
    away = normalize_team_name(guest_el.get_text() if guest_el else "")

    # Fallback: breadcrumb
    if not (home and away):
        bc = soup.select_one('[data-testid="breadcrumb-current-page"]')
        if bc:
            txt = bc.get_text(" ", strip=True)
            for sep in [" - ", " \u2013 ", " vs ", " v "]:
                if sep in txt:
                    parts = txt.split(sep, 1)
                    home = normalize_team_name(parts[0])
                    away = normalize_team_name(parts[1])
                    break

    # Horario
    gt = soup.select_one('[data-testid="game-time-item"]')
    iso = ""
    if gt:
        iso = parse_pt_datetime(gt.get_text(" ", strip=True)) or ""

    score_home, score_away, status, sets_detail = "", "", "scheduled", ""

    # Placar via live-info: "Resultado final <strong>2:1</strong> (sets detail)"
    li = soup.select_one('[data-testid="live-info"]')
    if li:
        # Sets vencidos: extrai do <strong>
        strong = li.find("strong")
        if strong:
            score_txt = strong.get_text(strip=True)
            m = re.match(r'^(\d+)\s*[:\-]\s*(\d+)$', score_txt)
            if m:
                try:
                    score_home = int(m.group(1))
                    score_away = int(m.group(2))
                    status = "finished"
                except ValueError:
                    pass

        # Sets detalhados: "(6:7, 7:6, 6:2)" \u2014 remove superscripts (tiebreak pts) antes
        li_copy = BeautifulSoup(str(li), "lxml")
        for sup in li_copy.find_all("sup"):
            sup.decompose()
        li_text = li_copy.get_text(" ", strip=True)
        paren = re.search(r'\(([^)]+)\)', li_text)
        if paren and status == "finished":
            # Normaliza: "6:7, 7:6, 6:2" -> "6-7 7-6 6-2"
            raw = paren.group(1)
            sets = re.findall(r'(\d+)\s*[:\-]\s*(\d+)', raw)
            if sets:
                sets_detail = " ".join(f"{a}-{b}" for a, b in sets)

        # Fallback: texto livre sem <strong> (outros esportes)
        if status == "scheduled":
            m = re.search(r'(\d+)\s*[:\-]\s*(\d+)', li_text)
            if m and any(kw in li_text for kw in ["Resultado", "Result", "Final", "final"]):
                try:
                    score_home = int(m.group(1))
                    score_away = int(m.group(2))
                    status = "finished"
                except ValueError:
                    pass

    # Fallback: game-participants (futebol/basquete: "Lakers 107 - 98 Rockets")
    if status == "scheduled":
        gp = soup.select_one('[data-testid="game-participants"]')
        if gp:
            txt = gp.get_text(" ", strip=True)
            m = re.search(r'(\d+)\s*[\u2013\u2014\-]\s*(\d+)', txt)
            if m:
                try:
                    score_home = int(m.group(1))
                    score_away = int(m.group(2))
                    status = "finished"
                except ValueError:
                    pass

    return {
        "event_datetime_utc": iso,
        "home": home,
        "away": away,
        "venue": "",
        "venue_city": "",
        "venue_country": "",
        "score_home": score_home,
        "score_away": score_away,
        "sets_detail": sets_detail,
        "status": status,
    }
