# Script: historico completo de tenis do oddsagora.com.br
# Rodar: C:\Users\Dell\anaconda3\python.exe astrology\scrapers\oddsportal\scrape_football_history.py
# Saida:  astrology/scrapers/oddsportal/data/raw/football_history.db
#
# Fase 1 (discovery): busca /football/results/ paginado e extrai todos os slugs de torneio ja disputados
# Fase 2 (scraping):  para cada torneio, raspa temporada atual + sufixos YYYY-YYYY / YYYY
# Fase 3 (paralelo):  torneios rodam em paralelo (PARALLEL_LEAGUES ao mesmo tempo)

import asyncio
import json
import os
import re
import sys
import time as _time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from astrology.scrapers.oddsportal.browser import OddsPortalBrowser
from astrology.scrapers.oddsportal import cache as cache_mod
from astrology.scrapers.oddsportal import url_builder
from astrology.scrapers.oddsportal.normalizer import stable_event_id
from astrology.scrapers.oddsportal.output_writer import SQLiteWriter
from astrology.scrapers.oddsportal.parsers import (
    results_listing, match_header, match_tabs,
)
from astrology.scrapers.oddsportal.market_catalog import get_market
import importlib
import sqlite3
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Sharding (GitHub Actions matrix): SHARD_ID/TOTAL_SHARDS dividem a lista de torneios.
SHARD_ID      = int(os.environ.get("SHARD_ID", "0"))
TOTAL_SHARDS  = int(os.environ.get("TOTAL_SHARDS", "1"))
DISCOVER_ONLY    = os.environ.get("DISCOVER_ONLY", "0") == "1"
PRIORITY_INVERT = os.environ.get("PRIORITY_INVERT", "0") == "1"  # shards 7-9: ITF-first
DEBUG_LEAGUES   = os.environ.get("DEBUG_LEAGUES", "0") == "1"   # roda apenas 5 ligas tier 0/1 localmente
DB_PATH       = os.environ.get("DB_PATH_OVERRIDE") or str(Path(__file__).parent / "data" / "raw" / "football_history.db")
BASE_URL      = "https://www.oddsagora.com.br"
SPORT_KEY     = "soccer"
SPORT_SLUG    = "football"
MARKETS       = [
    "1x2",               # Resultado final (1, X, 2)
    "over_under",        # Acima/Abaixo -- total de gols
    "btts",              # Ambas marcam (Sim/Nao)
    "double_chance",     # Dupla chance (1X, 12, X2)
    "draw_no_bet",       # Empate anula (1, 2)
    "european_handicap", # Handicap europeu
    "asian_handicap",    # Handicap asiatico
    "correct_score",     # Placar exato
    "ht_ft",             # Intervalo/Final
    "odd_even",          # Par/Impar
]

# Anos para tentar com sufixo de season - ordem REVERSA (mais recente primeiro)
# Combinado com early-stop: torneios discontinuados nao gastam 16 fetches.
SEASON_YEARS  = list(range(2026, 1997, -1))   # 2026, 2025, ..., 1998 (early-stop automatico)
SEASON_SUFFIXES = [None] + [str(y) for y in SEASON_YEARS]

_CUR_YEAR = datetime.now(timezone.utc).year


def _coalesce_score(primary, fallback):
    """Mantem o valor do header preferindo o primario, MAS preserva 0.
    BUG anterior: `header.get('score_home') or m.get('score_home')` descartava
    o 0 (set count do perdedor em sets diretos = falsy) -> placar virava NULL e
    o evento ficava 'finished' sem score. Atingia ~20k jogos."""
    return primary if primary not in (None, "") else fallback


def _season_is_final(suffix) -> bool:
    """True so para seasons PASSADAS (ano < ano atual): finalizadas, nao mudam mais.
    A season atual (None = feed ao vivo) e o ano corrente NUNCA viram cache."""
    if suffix is None:
        return False
    try:
        return int(suffix) < _CUR_YEAR
    except (TypeError, ValueError):
        return False

PARALLEL_LEAGUES  = 1   # 1 torneio por vez (evita travar a maquina)
# 12->24: onda 26950452680 confirmou zero OOM com 12 (todos cancelled, nenhum failed).
# 24x~150MB=3.6GB browser + 1GB overhead = ~4.6GB de 7GB. IO-bound: +paginas = +throughput.
# Se OOM (conclusion=failed), voltar p/ 12.
PARALLEL_MATCHES  = 28  # matches em paralelo (usa todas as paginas do pool)
BROWSER_POOL      = 28  # paginas Chromium no pool
USE_CACHE         = True
PAGE_FULL         = 40  # se a pg1 trouxe >= isso, provavelmente ha mais paginas
MAX_RESULT_PAGES  = 25  # teto de paginas de resultado por season
DISCOVERY_MAX_PAGES = 150       # paginas maximas a varrer na fase de discovery
DISCOVERY_CACHE_FILE = str(Path(__file__).parent / "data" / "raw" / "discovered_football_leagues.json")
DISCOVERY_CACHE_TTL  = 30 * 24 * 3600  # 30 dias

# ---------------------------------------------------------------------------
# Venue derivation (country/city from league slug — site nao fornece diretamente)
# ---------------------------------------------------------------------------

LEAGUE_CITY_OVERRIDES: dict[str, str] = {
    # Futebol: venue derivado automaticamente do slug (country/city)
    # Adicionar overrides especificos se necessario
    "england/campeonato-ingles":        "London",
    "spain/laliga":                     "Madrid",
    "italy/serie-a":                    "Rome",
    "germany/bundesliga":               "Munich",
    "france/ligue-1":                   "Paris",
    "europe/champions-league":          "Europe",
    "europe/liga-europa":               "Europe",
    "europe/liga-conferencia-europa":   "Europe",
    "brazil/brasileirao-betano":        "Brazil",
    "south-america/libertadores":       "South America",
    "south-america/sul-americana":      "South America",
}

_COUNTRY_NAMES: dict[str, str] = {
    "usa":                  "USA",
    "uae":                  "UAE",
    "united-arab-emirates": "UAE",
    "united-kingdom":       "UK",
    "south-korea":          "South Korea",
    "south-africa":         "South Africa",
    "czech-republic":       "Czech Republic",
    "new-zealand":          "New Zealand",
    "saudi-arabia":         "Saudi Arabia",
    "ivory-coast":          "Ivory Coast",
    "trinidad-tobago":      "Trinidad and Tobago",
}


def _derive_venue(league_path: str) -> tuple[str, str]:
    """Retorna (country, city) derivados do slug da liga."""
    parts = league_path.split("/", 1)
    country_slug = parts[0]
    tournament_slug = parts[1] if len(parts) > 1 else ""
    country = _COUNTRY_NAMES.get(country_slug, country_slug.replace("-", " ").title())
    if league_path in LEAGUE_CITY_OVERRIDES:
        return country, LEAGUE_CITY_OVERRIDES[league_path]
    slug = tournament_slug
    for pfx in ("div-", "liga-", "serie-", "cup-", "championship-",
                 "itf-m25-", "itf-w25-", "itf-m50-", "itf-w50-",
                 "itf-m100-", "itf-w100-"):
        if slug.startswith(pfx):
            slug = slug[len(pfx):]
            break
    for sfx in ("-open", "-masters", "-challenger", "-homens",
                "-mulheres", "-duplas", "-1", "-2", "-3", "-4"):
        if slug.endswith(sfx):
            slug = slug[:-len(sfx)]
            break
    return country, slug.replace("-", " ").title()


# Torneios conhecidos — usados como COMPLEMENTO ao discovery dinamico.
# O discovery raspa /football/results/ e descobre slugs reais automaticamente.
# Esta lista serve de fallback para torneios historicos que nao aparecem nas paginas recentes.
# Nomenclatura: {pais}/{atp|wta}-{cidade-ou-nome-pt}
# Torneios sem historico no site sao ignorados silenciosamente (0 matches = pula)
KNOWN_LEAGUES = [
    # --- Europa (Top 5) ---
    "england/campeonato-ingles",
    "spain/laliga",
    "italy/serie-a",
    "germany/bundesliga",
    "france/ligue-1",
    # --- Competicoes europeias ---
    "europe/champions-league",
    "europe/liga-europa",
    "europe/liga-conferencia-europa",
    # --- Brasil ---
    "brazil/brasileirao-betano",
    "brazil/copa-do-brasil",
    # --- America do Sul ---
    "south-america/libertadores",
    "south-america/sul-americana",
    # --- Outras ligas principais ---
    "netherlands/holanda",
    "portugal/primeira-liga",
    "turkey/super-lig",
    "russia/premier-league",
    "usa/mls",
    "argentina/primera-division",
]

# ---------------------------------------------------------------------------
# Prioridade por tier: garante que Grand Slams/Masters são processados PRIMEIRO
# dentro de cada shard, mesmo que o shard expire antes de terminar todos os torneios.
# Tier 0 = mais importante; tier 5 = ITF M15 etc.
# ---------------------------------------------------------------------------

_TIER_TOP = {
    "europe/champions-league", "europe/liga-europa", "europe/liga-conferencia-europa",
    "england/campeonato-ingles", "spain/laliga", "italy/serie-a",
    "germany/bundesliga", "france/ligue-1",
}

_TIER_MAJOR = {
    "brazil/brasileirao-betano", "south-america/libertadores", "south-america/sul-americana",
    "netherlands/holanda", "portugal/primeira-liga", "turkey/super-lig",
    "russia/premier-league", "usa/mls", "argentina/primera-division",
}


def _league_tier(path: str) -> int:
    """Retorna tier de prioridade (0=mais importante, 5=regional/amador)."""
    if path in _TIER_TOP:
        return 0
    if path in _TIER_MAJOR:
        return 1
    seg = path.split("/", 1)[-1] if "/" in path else path
    if any(k in seg for k in ("champions", "europa", "libertadores", "copa", "world-cup")):
        return 2
    if any(k in seg for k in ("serie-b", "championship", "segunda", "ligue-2", "2-bundesliga")):
        return 3
    if any(k in seg for k in ("serie-c", "tercera", "third", "3-")):
        return 4
    return 5  # ligas regionais/amador



# ---------------------------------------------------------------------------
# Discovery dinamico de torneios
# ---------------------------------------------------------------------------

def _extract_league_slugs(html: str) -> set[str]:
    """Extrai slugs {pais}/{torneio} de uma pagina de resultados de futebol."""
    found = set()
    # Match URL pattern: /football/{country}/{tournament}/{match-id}/
    for m in re.finditer(r'/football/([a-z0-9][a-z0-9-]*/[a-z0-9][a-z0-9-]*)/[a-z0-9#-]', html):
        slug = m.group(1)
        # Exclui slugs que parecem ser path de match ou segmentos invalidos
        parts = slug.split("/")
        if len(parts) == 2 and parts[0] and parts[1]:
            found.add(slug)
    return found


def load_discovered_leagues() -> list[str] | None:
    """Carrega slugs do cache JSON. Retorna None se inexistente, expirado ou vazio."""
    try:
        p = Path(DISCOVERY_CACHE_FILE)
        if not p.exists():
            return None
        data = json.loads(p.read_text(encoding="utf-8"))
        if _time.time() - data.get("timestamp", 0) > DISCOVERY_CACHE_TTL:
            print("[discovery] Cache expirado, re-descobrindo...")
            return None
        leagues = data.get("leagues", [])
        if not leagues:
            print("[discovery] Cache vazio, re-descobrindo...")
            return None
        print(f"[discovery] Cache carregado: {len(leagues)} torneios ({p})")
        return leagues
    except Exception as e:
        print(f"[discovery] Erro ao carregar cache: {e}")
        return None


def save_discovered_leagues(leagues: list[str]) -> None:
    try:
        p = Path(DISCOVERY_CACHE_FILE)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(
            json.dumps({"timestamp": _time.time(), "leagues": sorted(leagues)},
                       ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"[discovery] {len(leagues)} slugs salvos em {p}")
    except Exception as e:
        print(f"[discovery] Erro ao salvar cache: {e}")


DISCOVERY_WAIT_SELECTOR = '[data-testid="game-row"], a[href*="/football/"]'


async def _fetch_fresh(br: OddsPortalBrowser, url: str, wait_selector: str | None = None) -> str:
    """Fetch sem usar cache local (sempre hit na rede)."""
    return await br.fetch(url, wait_selector=wait_selector)


async def discover_leagues(br: OddsPortalBrowser) -> list[str]:
    """
    Raspa paginas do oddsagora.com.br para descobrir slugs reais de torneios.
    Tenta multiplas URLs-fonte (resultados globais + pagina principal do esporte).
    """
    slugs: set[str] = set()
    discovery_urls = [
        f"{BASE_URL}/football/results/",
        f"{BASE_URL}/football/",
        f"{BASE_URL}/matches/football/",
    ]

    print(f"\n[discovery] Buscando slugs reais em oddsagora.com.br ...")

    for base in discovery_urls:
        print(f"\n[discovery] Fonte: {base}")
        consecutive_no_new = 0
        for page in range(1, DISCOVERY_MAX_PAGES + 1):
            url = base if page == 1 else f"{base}#/page/{page}/"
            try:
                html = await _fetch_fresh(br, url, wait_selector=DISCOVERY_WAIT_SELECTOR)
            except Exception as e:
                print(f"  [discovery] Erro pagina {page}: {e}")
                break

            before = len(slugs)
            new = _extract_league_slugs(html)
            slugs.update(new)
            after = len(slugs)

            # Verifica se ha algum conteudo util na pagina (game-rows OU links de tenis)
            matches = results_listing.parse(html)
            has_content = bool(matches) or bool(new)
            if not has_content:
                print(f"  [discovery] Pagina {page}: vazia — pulando fonte")
                break

            print(f"  [discovery] Pagina {page}: {len(matches)} matches, +{after-before} slugs novos (total global: {after})")

            if after == before:
                consecutive_no_new += 1
                if consecutive_no_new >= 5:
                    print(f"  [discovery] 5 paginas consecutivas sem slugs novos — proxima fonte")
                    break
            else:
                consecutive_no_new = 0

        if slugs:
            # Se ja achou algo com essa fonte, nao precisa tentar as outras (economiza tempo)
            # Mas continue se quiser maximo de cobertura (deixa comentado como opcional)
            pass

    print(f"\n[discovery] Total: {len(slugs)} torneios descobertos")
    return sorted(slugs)


async def _fetch_cached(br: OddsPortalBrowser, url: str, wait_selector=None, force=False) -> str:
    """Busca HTML do cache local se disponivel; caso contrario faz request.
    force=True ignora o cache (usado para CONFIRMAR um listing vazio antes de cachear)."""
    if USE_CACHE and not force:
        cached = cache_mod.get(url)
        if cached is not None:
            return cached
    html = await br.fetch(url, wait_selector=wait_selector)
    if USE_CACHE:
        cache_mod.put(url, html)
    return html


# ---------------------------------------------------------------------------
# Checkpoint e Backfill
# ---------------------------------------------------------------------------

def _scraped_urls(db_path: str, league_path: str, season: str) -> set:
    """Retorna set de source_url de eventos ja completos: tem score E home_away odds."""
    try:
        con = sqlite3.connect(db_path)
        rows = con.execute("""
            SELECT DISTINCT e.source_url FROM events e
            JOIN leagues l ON l.id = e.league_id
            WHERE l.path = ? AND e.season = ?
              AND e.score_home IS NOT NULL AND e.score_home != ''
              AND (
                e.status != 'finished'
                OR (e.partials IS NOT NULL AND e.partials != '')
              )
              AND EXISTS (
                SELECT 1 FROM odds o JOIN markets m ON m.id = o.market_id
                WHERE o.event_id = e.id AND m.name = 'home_away'
              )
        """, (league_path, season)).fetchall()
        con.close()
        return {r[0] for r in rows}
    except Exception:
        return set()


def _scraped_urls_count(db_path: str, league_path: str, season: str) -> int:
    """Conta eventos existentes para liga+season (Achado 2: guard para false-empty)."""
    try:
        con = sqlite3.connect(db_path)
        row = con.execute(
            "SELECT COUNT(*) FROM events e JOIN leagues l ON e.league_id=l.id WHERE l.path=? AND e.season=?",
            (league_path, season)
        ).fetchone()
        con.close()
        return row[0] if row else 0
    except Exception:
        return 0


def _league_incomplete_events(db_path: str, league_path: str) -> list[dict]:
    """
    Retorna eventos da liga que estao no DB sem score OU sem home_away odds.
    Cobre: fetch da pagina principal falhou, score nao parseado, odds ausentes.
    """
    try:
        con = sqlite3.connect(db_path)
        rows = con.execute("""
            SELECT e.id, e.source_url, e.season,
                   th.name AS home, ta.name AS away, e.dt_utc, e.status
            FROM events e
            JOIN leagues l  ON l.id = e.league_id
            JOIN teams   th ON th.id = e.home_id
            JOIN teams   ta ON ta.id = e.away_id
            WHERE l.path = ? AND e.source_url != ''
              AND (
                e.score_home IS NULL OR e.score_home = ''
                OR NOT EXISTS (
                  SELECT 1 FROM odds o JOIN markets m ON m.id = o.market_id
                  WHERE o.event_id = e.id AND m.name = 'home_away'
                )
                OR (e.status = 'finished'
                    AND (e.partials IS NULL OR e.partials = ''))
              )
        """, (league_path,)).fetchall()
        con.close()
        return [
            {"event_id": r[0], "match_url": r[1], "season": r[2],
             "home": r[3], "away": r[4], "event_datetime_utc": r[5], "status": r[6]}
            for r in rows
        ]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Processar um match
# ---------------------------------------------------------------------------

def _load_parser(module_name: str):
    return importlib.import_module(
        f"astrology.scrapers.oddsportal.parsers.{module_name}"
    )


async def _process_match(
    br: OddsPortalBrowser, m: dict,
    league_path: str, season: str,
    writer: SQLiteWriter, sem: asyncio.Semaphore,
    idx: int, total: int,
) -> int:
    async with sem:
        match_url = m["match_url"]
        print(f"  [match {idx}/{total}] {m.get('home','?')} vs {m.get('away','?')}")
        try:
            from astrology.scrapers.oddsportal.browser import ODDS_WAIT_SELECTOR
            h_main = await _fetch_cached(br, match_url, wait_selector=ODDS_WAIT_SELECTOR)
            header = match_header.parse(h_main)
            home = header.get("home") or m.get("home", "")
            away = header.get("away") or m.get("away", "")
            iso  = header.get("event_datetime_utc") or m.get("event_datetime_utc", "")
            event_id = stable_event_id(SPORT_KEY, league_path, home, away, iso)

            ctx = {
                "sport": SPORT_KEY, "league": league_path, "season": season or "",
                "event_id": event_id, "event_datetime_utc": iso,
                "event_datetime_local": "",
                "home": home, "away": away,
                "score_home": _coalesce_score(header.get("score_home"), m.get("score_home", "")),
                "score_away": _coalesce_score(header.get("score_away"), m.get("score_away", "")),
                "partials": header.get("partials", ""),
                "score_home_ht": header.get("score_home_ht", ""),
                "score_away_ht": header.get("score_away_ht", ""),
                "score_home_2h": header.get("score_home_2h", ""),
                "score_away_2h": header.get("score_away_2h", ""),
                "status": header.get("status", "scheduled"),
                "venue": header.get("venue", ""), "venue_city": header.get("venue_city", ""),
                "venue_country": header.get("venue_country", ""),
                "venue_lat": "", "venue_lon": "",
                "payout_pct": "", "source_url": match_url,
                "scraped_at_utc": datetime.now(timezone.utc).isoformat(),
            }

            _country, _city = _derive_venue(league_path)
            ctx["venue_country"] = ctx["venue_country"] or _country
            ctx["venue_city"]    = ctx["venue_city"]    or _city

            available_tabs = set(match_tabs.parse(h_main))
            wanted = []
            for mk in MARKETS:
                try:
                    meta = get_market(mk)
                except KeyError:
                    continue
                label    = meta["tab_label"]
                tab_slug = meta.get("tab_slug", "")
                # Pula so se requer navegacao (tab_slug nao vazio) E tab nao esta na pagina
                if tab_slug and available_tabs and label not in available_tabs:
                    continue
                wanted.append((mk, label, meta["parser_module"]))

            needed_labels = list({lbl for _, lbl, _ in wanted if lbl})
            tab_htmls = await br.fetch_all_tabs(match_url, needed_labels) if needed_labels else {}
            htmls = {"": h_main}
            htmls.update(tab_htmls)

            rows_written = 0
            for mk, label, pmod_name in wanted:
                try:
                    pmod = _load_parser(pmod_name)
                    rows = pmod.parse(htmls.get(label, h_main), dict(ctx))
                except Exception as e:
                    print(f"    [ERRO parser {pmod_name}]: {e}")
                    continue
                for r in rows:
                    writer.write(r)
                rows_written += len(rows)

            # Garante que o evento fica no DB com score mesmo sem odds historicas
            if rows_written == 0 and ctx.get("event_id"):
                writer.ensure_event(ctx)

            print(f"  -> {rows_written} linhas (score: {ctx.get('score_home', '')}:{ctx.get('score_away', '')})")
            return rows_written

        except Exception as e:
            print(f"  [ERRO match]: {e}")
            return 0


# ---------------------------------------------------------------------------
# Processar um torneio (todas as seasons)
# ---------------------------------------------------------------------------

async def scrape_league(
    br: OddsPortalBrowser, league_path: str, writer: SQLiteWriter,
    league_sem: asyncio.Semaphore, idx: int = 0, total: int = 0,
):
    async with league_sem:
        match_sem = asyncio.Semaphore(PARALLEL_MATCHES)
        tier = _league_tier(league_path)
        _matches_total = 0
        _empty_seasons = 0
        _skipped_cache = 0
        _skipped_complete = 0
        prefix = f"[{idx}/{total}] " if idx else ""
        print(f"\n{'='*55}")
        print(f"{prefix}Torneio: {league_path}  [tier={tier}]")
        print(f"{'='*55}")

        for suffix in SEASON_SUFFIXES:
            season_str = suffix or ""

            # Pre-listing skip (correcao B): seasons passadas ja raspadas por completo
            # numa onda anterior sao puladas ANTES de paginar a listagem. Sem isso o
            # shard re-rastejava a listagem (ate 25 pgs) toda onda e nunca avancava
            # pela fila para alcancar os tiers profundos (ITF).
            if _season_is_final(suffix) and writer.is_season_complete(league_path, season_str):
                print(f"  [skip-cache] {league_path} season={season_str} ja completa (onda anterior)")
                _skipped_cache += 1
                continue

            results_url = url_builder.build_results_url(SPORT_SLUG, league_path, suffix)
            print(f"  [listing] season={season_str or 'atual'}")

            try:
                html = await _fetch_cached(br, results_url, wait_selector=DISCOVERY_WAIT_SELECTOR)
            except Exception as e:
                print(f"  [ERRO listing {results_url}]: {e}")
                continue

            all_matches = results_listing.parse(html)

            # Confirma vazio com 1 retry (bypass cache) antes de seguir: uma carga lenta
            # ou transiente nao pode virar um false-empty CACHEADO permanente (protege a
            # completude — mapeamos todos os anos que realmente existem).
            if not all_matches:
                try:
                    html = await _fetch_cached(br, results_url, wait_selector=DISCOVERY_WAIT_SELECTOR, force=True)
                    all_matches = results_listing.parse(html)
                except Exception:
                    pass

            # Paginacao por esgotamento. detect_pagination NAO funciona: os links
            # #/page/N/ sao hash-routed via JS e nao aparecem no HTML estatico
            # (sempre retorna 1) -> so pegava a 1a pagina (~50 de ~153 no AO).
            # Avanca paginas ate 3 seguidas sem match novo. So pagina se a pg1
            # veio "cheia" (>= PAGE_FULL): torneios pequenos cabem numa pagina.
            seen_urls = {m["match_url"] for m in all_matches}
            _pag_ok = True
            if len(all_matches) >= PAGE_FULL:
                _pag_ok = False
                no_new = 0
                pg = 2
                while pg <= MAX_RESULT_PAGES:
                    page_url = f"{results_url}#/page/{pg}/"
                    try:
                        h = await _fetch_cached(br, page_url, wait_selector=DISCOVERY_WAIT_SELECTOR)
                        pg_matches = results_listing.parse(h)
                    except Exception:
                        break
                    new = [m for m in pg_matches if m["match_url"] not in seen_urls]
                    for m in new:
                        seen_urls.add(m["match_url"])
                        all_matches.append(m)
                    if new:
                        no_new = 0
                    else:
                        no_new += 1
                        if no_new >= 3:
                            _pag_ok = True
                            break
                    pg += 1
                else:
                    _pag_ok = True

            if not all_matches:
                print(f"  [vazio] sem matches em {results_url}")
                _empty_seasons += 1
                # SEM early-stop: mapeamos TODOS os anos possiveis (completude).
                # A eficiencia vem do CACHE: marca a season passada vazia como completa
                # para nenhuma onda futura re-buscar esse ano. Assim a 1a onda mapeia o
                # torneio inteiro (cheio + vazio) e as proximas pulam tudo (so feed atual).
                if _season_is_final(suffix):
                    # Achado 2 guard: so marca como vazio se nao houver eventos ja gravados para esse ano.
                    # Evita cachear como "nunca existiu" uma season que foi coletada via feed atual.
                    existing_count = _scraped_urls_count(str(writer.path), league_path, season_str)
                    if existing_count == 0:
                        writer.mark_season_complete(league_path, season_str, 0)
                    else:
                        print(f"  [skip-empty-guard] {league_path}/{season_str} tem {existing_count} eventos no DB — nao cacheia como vazio")
                continue

            # Checkpoint por jogo: pula apenas os ja completos (score + home_away odds)
            done_urls = _scraped_urls(str(writer.path), league_path, season_str)
            matches_to_process = [m for m in all_matches if m["match_url"] not in done_urls]
            if not matches_to_process:
                print(f"  [skip] {league_path} season={season_str or 'atual'} — todos {len(all_matches)} ja completos")
                _skipped_complete += 1
                # Season passada totalmente coberta -> grava no cache p/ pular antes da listagem na proxima onda
                if _season_is_final(suffix) and _pag_ok:
                    writer.mark_season_complete(league_path, season_str, len(all_matches))
                continue

            print(f"  {len(matches_to_process)}/{len(all_matches)} jogos a raspar (season '{season_str or 'atual'}')")
            _matches_total += len(matches_to_process)
            tasks = [
                _process_match(br, m, league_path, season_str, writer, match_sem, i, len(matches_to_process))
                for i, m in enumerate(matches_to_process, 1)
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
            # Pass completa numa season PASSADA: o conjunto de jogos do torneio finalizado
            # nao muda mais; marca no cache para nao re-rastejar a listagem nas proximas ondas.
            # (jogos sem odds no site permanecem sem odds — re-tentar e desperdicio que trava o avanco)
            if _season_is_final(suffix) and _pag_ok:
                writer.mark_season_complete(league_path, season_str, len(all_matches))

        # Backfill: re-raspa eventos sem score OU sem home_away odds.
        # Exclui seasons passadas ja marcadas completas (cache B): re-tentar jogos
        # cronicamente sem odds (challenger/ITF antigos) impediria o shard de avancar.
        incomplete = [
            ev for ev in _league_incomplete_events(str(writer.path), league_path)
            if not (_season_is_final(ev.get("season")) and writer.is_season_complete(league_path, ev.get("season", "")))
        ]
        if incomplete:
            print(f"  [backfill] {len(incomplete)} eventos incompletos (sem score ou odds) em {league_path}")
            bf_tasks = [
                _process_match(br, ev, league_path, ev["season"], writer, match_sem, i, len(incomplete))
                for i, ev in enumerate(incomplete, 1)
            ]
            await asyncio.gather(*bf_tasks, return_exceptions=True)
        return {
            "league": league_path,
            "matches_total": _matches_total,
            "empty_seasons": _empty_seasons,
            "skipped_cache": _skipped_cache,
            "skipped_complete": _skipped_complete,
        }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    print("=" * 60)
    print("Tennis Full History Scraper")
    print(f"Output: {DB_PATH}")
    print(f"Paralelo: {PARALLEL_LEAGUES} torneios x {PARALLEL_MATCHES} matches")
    print("=" * 60)

    writer = SQLiteWriter(DB_PATH)

    async with OddsPortalBrowser(headful=False, concurrency=BROWSER_POOL) as br:

        # --- Fase 1: discovery de torneios reais ---
        discovered = load_discovered_leagues()
        if discovered is None:
            discovered = await discover_leagues(br)
            save_discovered_leagues(discovered)

        # Modo discover-only (usado pelo GitHub Actions para gerar slugs antes do matrix)
        if DISCOVER_ONLY:
            print(f"\n[discover-only] {len(discovered)} slugs salvos. Saindo.")
            writer.close()
            return

        # Merge: torneios descobertos + KNOWN_LEAGUES (complemento historico)
        all_leagues = sorted(set(discovered) | set(KNOWN_LEAGUES))
        print(f"\n[info] {len(discovered)} descobertos + {len(KNOWN_LEAGUES)} conhecidos = {len(all_leagues)} torneios unicos")

        # Sharding (GitHub Actions matrix): divide a lista global entre shards
        if TOTAL_SHARDS > 1:
            all_leagues = [s for i, s in enumerate(all_leagues) if i % TOTAL_SHARDS == SHARD_ID]
            print(f"[shard {SHARD_ID}/{TOTAL_SHARDS}] {len(all_leagues)} torneios neste shard (antes de reordenar)")

        # PRIORITY-FIRST: reordena dentro do shard por tier (Grand Slams primeiro).
        # Garante que mesmo se o shard expirar no timeout, os torneios mais importantes
        # ja foram processados. Dentro de cada tier, ordena alfabeticamente.
        all_leagues.sort(key=lambda p: (_league_tier(p), p), reverse=PRIORITY_INVERT)
        if DEBUG_LEAGUES:
            all_leagues = [lg for lg in all_leagues if _league_tier(lg) <= 1][:5]
            print(f"[DEBUG] Modo local — apenas {len(all_leagues)} ligas tier 0/1: {all_leagues}")
        tier_counts = {}
        for lg in all_leagues:
            t = _league_tier(lg)
            tier_counts[t] = tier_counts.get(t, 0) + 1
        print(f"[priority] Ordem: {dict(sorted(tier_counts.items()))} (tier 0=Grand Slam, 5=ITF)")

        # Filtro por manifest de incompletos (retry run)
        manifest_path = os.environ.get("INCOMPLETE_MANIFEST")
        if manifest_path and Path(manifest_path).exists():
            import json as _json
            incomplete_manifest = _json.loads(Path(manifest_path).read_text(encoding="utf-8"))
            slugs = {e["league"] for e in incomplete_manifest}
            all_leagues = [l for l in all_leagues if l in slugs]
            print(f"[manifest] Retry restrito a {len(all_leagues)} ligas incompletas")

        # --- Fase 2: scraping de todos os torneios ---
        league_sem = asyncio.Semaphore(PARALLEL_LEAGUES)
        tasks = [
            scrape_league(br, lg, writer, league_sem, idx, len(all_leagues))
            for idx, lg in enumerate(all_leagues, 1)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        leagues_with_data     = sum(1 for r in results if isinstance(r, dict) and r.get("matches_total", 0) > 0)
        leagues_empty         = sum(1 for r in results if isinstance(r, dict) and r.get("matches_total", 0) == 0)
        seasons_skipped_cache = sum(r.get("skipped_cache", 0) for r in results if isinstance(r, dict))
        print(f"\n[resumo] Ligas com dados: {leagues_with_data} | Ligas vazias: {leagues_empty} | Seasons puladas por cache: {seasons_skipped_cache}")

    stats = writer.stats()
    writer.close()
    print("\n\nConcluido!")
    print(f"  DB: {DB_PATH}")
    print(f"  Eventos: {stats.get('events', 0)}")
    print(f"  Odds: {stats.get('odds', 0)}")
    print(f"  Torneios: {stats.get('leagues', 0)}")


if __name__ == "__main__":
    asyncio.run(main())
