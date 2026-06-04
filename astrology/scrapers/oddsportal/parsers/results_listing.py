"""
Parse da pagina /results/ do oddsagora.com.br.

Estrutura HTML (2026-04):
  <div data-testid="game-row">              <- OUTER (tem <a href> direto)
    <a href="/basketball/h2h/team1/team2/#matchID">
      <div data-testid="game-row">          <- INNER (ignorar)
        <div data-testid="time-item">22:30</div>
        <div data-testid="event-participants">
          <a title="Team1"><p class="participant-name">Team1</p></a>
          <a title="Team2"><p class="participant-name">Team2</p></a>
        </div>
      </div>
    </a>
  </div>

Data de cada grupo: <div data-testid="date-header"> ou <div data-testid="secondary-header">
precedendo os game-rows.

Retorna lista de dicts: match_url, event_datetime_utc, home, away, score_home, score_away, status.
"""
import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from ..normalizer import normalize_team_name, parse_pt_datetime

BASE_URL = "https://www.oddsagora.com.br"
ALLOWED_HOSTS = ("oddsagora.com.br", "oddsportal.com")


def _is_safe_url(url: str) -> bool:
    """Garante que so seguimos URLs dos hosts permitidos (evita SSRF via HTML malicioso)."""
    try:
        host = urlparse(url).netloc.lower()
        if not host:
            return False
        return any(host == a or host.endswith("." + a) for a in ALLOWED_HOSTS)
    except Exception:
        return False


def parse(html: str, base_url: str = BASE_URL) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    out = []

    # Itera todos os elementos em ordem para rastrear date-header + game-rows
    last_date_str = ""
    for el in soup.find_all(True):
        tid = el.get("data-testid", "")

        if tid in ("date-header", "secondary-header"):
            raw = el.get_text(" ", strip=True)
            # Remove info de round: "Hoje, 19 Abr  - Playoffs" -> "Hoje, 19 Abr"
            last_date_str = re.split(r'\s*-\s*', raw)[0].strip()
            continue

        if tid != "game-row":
            continue

        # So processa outer game-row (tem <a href> como filho direto)
        link = el.find("a", href=True, recursive=False)
        if not link:
            continue

        href = link["href"]
        if not href or href.startswith("http") is False and not href.startswith("/"):
            continue
        match_url = href if href.startswith("http") else base_url.rstrip("/") + href
        if not _is_safe_url(match_url):
            continue  # URL fora dos hosts permitidos — ignora (mitigacao SSRF)

        # Times
        ep = el.select_one('[data-testid="event-participants"]')
        teams = []
        if ep:
            for p in ep.select(".participant-name"):
                t = normalize_team_name(p.get_text())
                if t:
                    teams.append(t)
        if len(teams) < 2:
            # Fallback: titulos dos <a> dentro de event-participants
            if ep:
                for a in ep.find_all("a", title=True):
                    t = normalize_team_name(a["title"])
                    if t and t not in teams:
                        teams.append(t)
        if len(teams) < 2:
            continue
        home, away = teams[0], teams[1]

        # Horario: combina data do header com hora do game-row
        ti = el.select_one('[data-testid="time-item"]')
        time_str = ti.get_text(strip=True) if ti else "00:00"
        # game-row pode ter formato "22:30" ou "Finalizado"
        is_finished = not re.match(r'^\d{1,2}:\d{2}$', time_str)
        status = "finished" if is_finished else "scheduled"
        if is_finished:
            time_str = "00:00"

        # Concatena para parse: "Hoje, 19 Abr 22:30" ou "17 Abr 2026 22:30"
        combined = f"{last_date_str} {time_str}" if last_date_str else time_str
        iso = parse_pt_datetime(combined) or ""

        # Tenta extrair placar da linha (oddsagora mostra scores em resultados)
        score_home, score_away = "", ""
        if is_finished:
            score_el = el.select_one('[data-testid="result-score"], [data-testid="score"]')
            if score_el:
                sm = re.match(r'^(\d+)\s*[:\-]\s*(\d+)$', score_el.get_text(strip=True))
                if sm:
                    score_home, score_away = sm.group(1), sm.group(2)
            if not score_home:
                row_text = el.get_text(" ", strip=True)
                sm2 = re.search(r'(?<![:\d])(\d{1,2})\s*:\s*(\d{1,2})(?![:\d])', row_text)
                if sm2 and not re.match(r'^\d{2}:\d{2}$', sm2.group(0)):
                    score_home, score_away = sm2.group(1), sm2.group(2)

        out.append({
            "match_url": match_url,
            "event_datetime_utc": iso,
            "home": home,
            "away": away,
            "score_home": score_home,
            "score_away": score_away,
            "status": status,
        })

    # Deduplica por match_url (podem aparecer duplicatas na iteracao)
    seen, deduped = set(), []
    for r in out:
        if r["match_url"] not in seen:
            seen.add(r["match_url"])
            deduped.append(r)
    return deduped


def detect_pagination(html: str) -> int:
    """Retorna o numero total de paginas (1 se nao houver paginacao)."""
    # Procura apenas links de paginacao #/page/N/
    # Os secondary-headers tem "1 2" como cabecalhos de coluna (nao paginacao)
    # Os season archive links nao sao paginacao da listagem atual
    pages = [int(m) for m in re.findall(r'#/page/(\d+)/', html)]
    return max(pages) if pages else 1
