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
import json
import re
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from ..normalizer import normalize_team_name, parse_pt_datetime

def _extract_jsonld_event(html: str) -> dict | None:
    """Extrai o bloco JSON-LD schema.org SportsEvent (2026-09-04): mais
    robusto que os seletores data-testid (que o site ja trocou mais de uma
    vez durante esta investigacao -- ver participant-name/game-row).
    Da startDate (ISO com timezone), homeTeam/awayTeam.name, e pais da sede.
    """
    for blk in re.findall(r'<script type="application/ld\+json">(.*?)</script>', html, re.S):
        try:
            d = json.loads(blk)
        except Exception:
            continue
        if "SportsEvent" in str(d.get("@type", "")):
            return d
    return None


def _jsonld_iso_utc(start_date: str) -> str:
    """'2026-08-25T13:00:00+02:00' -> '2026-08-25T11:00:00+00:00' (UTC)."""
    try:
        dt = datetime.fromisoformat(start_date)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    except Exception:
        return ""



def _parse_partials(paren_text: str) -> tuple:
    """
    Extrai score_home_ht, score_away_ht do primeiro grupo parentetico.
    Ex: "(2:0, 3:0)" -> (2, 0)  [1o tempo]
        "(6:7, 7:6, 6:2)" -> (6, 7)  [1o set no tenis]
    Retorna ("", "") se nao encontrar.
    """
    sets = re.findall(r'(\d+)\s*[:\-]\s*(\d+)', paren_text)
    if sets:
        return int(sets[0][0]), int(sets[0][1])
    return "", ""


def parse(html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")

    jsonld = _extract_jsonld_event(html)

    # Times: JSON-LD primeiro (mais robusto -- schema.org e padrao estavel de
    # SEO, sofre bem menos com redesigns de UI que os data-testid, que o site
    # ja trocou 2x durante esta investigacao). Cai pro seletor antigo se
    # ausente.
    home, away = "", ""
    if jsonld:
        home = normalize_team_name(jsonld.get("homeTeam", {}).get("name", "") or "")
        away = normalize_team_name(jsonld.get("awayTeam", {}).get("name", "") or "")
    if not (home and away):
        host_el  = soup.select_one('[data-testid="game-host"]')
        guest_el = soup.select_one('[data-testid="game-guest"]')
        home = home or normalize_team_name(host_el.get_text()  if host_el  else "")
        away = away or normalize_team_name(guest_el.get_text() if guest_el else "")

    # Fallback: breadcrumb (DESATIVADO 2026-09-04 -- bug real, ~24k eventos
    # afetados no futebol). O breadcrumb tipico e a trilha de navegacao
    # "Futebol > Espanha > Tercera RFEF > Group 1 2025/2026", NAO os 2 times.
    # O split ingenuo por " - " pegava competicao/subgrupo como se fossem
    # nomes de time (ex: home="Tercera RFEF", away="Group 1 2025/2026") -- e
    # por serem strings NAO-VAZIAS, ganhavam do fallback correto em
    # _process_match() (`header.get("home") or m.get("home")`), que teria
    # usado o nome real ja extraido corretamente pela listagem. Sem fallback
    # aqui, home/away ficam vazios e o `or` cai no valor certo da listagem.

    # Horario: JSON-LD primeiro (25,8% do tenis / 17,9% do futebol tinham
    # 'T00:00:00' de placeholder porque [data-testid="game-time-item"] some
    # da pagina -- confirmado ao vivo em 2026-09-04, o site removeu TODOS os
    # data-testid das paginas de partida. JSON-LD sobrevive porque e um
    # padrao de dados estruturados diferente do CSS-in-JS interno.)
    iso = ""
    if jsonld and jsonld.get("startDate"):
        iso = _jsonld_iso_utc(jsonld["startDate"])
    if not iso:
        gt = soup.select_one('[data-testid="game-time-item"]')
        if gt:
            iso = parse_pt_datetime(gt.get_text(" ", strip=True)) or ""

    # Pais da sede: JSON-LD tem location.address.addressCountry quando
    # disponivel (nem sempre populado, mas quando esta e mais confiavel que
    # inferir do slug da liga).
    venue_country = ""
    if jsonld:
        venue_country = (jsonld.get("location", {}) or {}).get("address", {}).get("addressCountry", "") or ""

    # Placar via live-info: "Resultado final <strong>2:1</strong> (sets detail)"
    status = "scheduled"
    score_home = score_away = ""
    score_home_ht = score_away_ht = ""
    score_home_2h = score_away_2h = ""
    partials = ""
    li = soup.select_one('[data-testid="live-info"]')
    if li:
        # Score final: extrai do <strong>
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

        # Parciais: "(6:7, 7:6, 6:2)" ou "(2:0, 3:0)" — remove superscripts antes
        li_copy = BeautifulSoup(str(li), "lxml")
        for sup in li_copy.find_all("sup"):
            sup.decompose()
        li_text = li_copy.get_text(" ", strip=True)
        paren = re.search(r'\(([^)]+)\)', li_text)
        if paren and status == "finished":
            raw = paren.group(1)
            sets = re.findall(r'(\d+)\s*[:\-]\s*(\d+)', raw)
            if sets:
                partials = " ".join(f"{a}-{b}" for a, b in sets)
                # 1o parcial = 1o tempo (futebol) ou 1o set (tenis)
                try:
                    score_home_ht = int(sets[0][0])
                    score_away_ht = int(sets[0][1])
                except (ValueError, IndexError):
                    pass
                # 2o parcial = 2o tempo (futebol) ou 2o set (tenis)
                if len(sets) >= 2:
                    try:
                        score_home_2h = int(sets[1][0])
                        score_away_2h = int(sets[1][1])
                    except (ValueError, IndexError):
                        pass

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
            m = re.search(r'(\d+)\s*[–—\-]\s*(\d+)', txt)
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
        "venue_country": venue_country,
        "score_home":    score_home,
        "score_away":    score_away,
        "score_home_ht": score_home_ht,
        "score_away_ht": score_away_ht,
        "score_home_2h": score_home_2h,
        "score_away_2h": score_away_2h,
        "partials":     partials,    # raw parciais: "2-0 3-0" (futebol) ou "6-7 7-6 6-2" (tenis)
        "status":        status,
    }
