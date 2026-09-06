"""
Recupera placar/resultado dos eventos "fantasma": status='scheduled' mas
dt_utc corrompido ('...T00:00:00') E data ja passada -- o jogo com certeza
ja aconteceu, mas o scraper nunca capturou o resultado (nem pela listagem
nem pela pagina individual, ambas quebradas pela remocao dos data-testid
em 2026-09).

Estrategia: agrupa por (liga, temporada) -- MUITO mais eficiente que 1
fetch por evento, ja que 1 pagina de listagem cobre dezenas/centenas de
jogos. Para cada grupo:
  1. Constroi a URL de resultados (/{sport}/{league_path}-{season}/results/)
  2. Pagina via fetch_listing_pages (browser.py corrigido em 2026-09-06:
     data-testid sumiu -> href de h2h e o marcador; botao de paginacao
     precisa de espera pos-hidratacao antes do clique)
  3. Parseia cada linha (parse_results_scores.py: nome + placar por posicao
     estrutural no DOM, nao por classe CSS)
  4. Casa pelo #hash da source_url de cada evento fantasma do grupo

Validado ao vivo: ATP Indian Wells 2017, 74/77 (96%) casados.

Saida incremental: recovered_scores_football.json
  {event_id: {"score_home": int, "score_away": int} | null}
null = nao encontrado no grupo (fora do alcance da paginacao, torneio/season
com league/season mal atribuido no DB -- caso raro, ver nota historica sobre
Ruud/Tsonga -- ou jogo cancelado/walkover sem linha na listagem).
"""
import argparse
import asyncio
import json
import re
import sqlite3
import sys
from pathlib import Path

sys.stdout.reconfigure(errors="replace")
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
# parse_results_scores.py mora no meu-repositorio (nao neste checkout do
# scraper) -- caminho absoluto, mesmo padrao do --recovered em
# enriquecer_astro_tennis.py.
sys.path.insert(0, r"C:\Users\Dell\CursoPRD\meu-repositorio\astrology\astrologia_futebol")

from astrology.scrapers.oddsportal.browser import OddsPortalBrowser  # noqa: E402
from astrology.scrapers.oddsportal.url_builder import build_results_url  # noqa: E402
from parse_results_scores import parse_results_page  # noqa: E402

DEFAULT_DB  = str(Path(__file__).parent / "data" / "raw" / "football_history.db")
DEFAULT_OUT = str(Path(__file__).parent / "data" / "processed" / "recovered_scores_football.json")
SPORT = "football"

CHECKPOINT_EVERY_GROUPS = 20


def load_out(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def save_out(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=1, ensure_ascii=False), encoding="utf-8")


async def process_group(browser: OddsPortalBrowser, league_path: str, season: str,
                         events: list[tuple[str, str]]) -> dict:
    """events: [(event_id, source_url), ...] do mesmo (liga, temporada).
    Retorna {event_id: {score_home, score_away} | None}."""
    url = build_results_url(SPORT, league_path, season)
    try:
        htmls, status = await browser.fetch_listing_pages(url, max_pages=30)
    except Exception:
        htmls, status = [], None

    rows = []
    for h in htmls:
        try:
            rows.extend(parse_results_page(h))
        except Exception:
            continue
    by_hash = {r["hash"]: r for r in rows if r["hash"]}

    out = {}
    for event_id, src in events:
        frag = src.split("#", 1)[1] if "#" in src else ""
        r = by_hash.get(frag)
        if r:
            try:
                out[event_id] = {"score_home": int(r["score_home"]), "score_away": int(r["score_away"])}
            except ValueError:
                out[event_id] = None
        else:
            out[event_id] = None
    return out


async def main_async(db: str, out: str, concurrency: int, limit_groups: int | None):
    con = sqlite3.connect(db)
    rows = con.execute("""
        SELECT e.id, e.source_url, l.path, e.season FROM events e
        JOIN leagues l ON l.id = e.league_id
        WHERE e.dt_utc LIKE '%T00:00:00%' AND e.status = 'scheduled'
          AND e.source_url IS NOT NULL AND e.source_url != ''
    """).fetchall()
    con.close()

    groups: dict[tuple[str, str], list[tuple[str, str]]] = {}
    for event_id, src, league_path, season in rows:
        groups.setdefault((league_path, season or ""), []).append((event_id, src))

    group_list = list(groups.items())
    if limit_groups:
        group_list = group_list[:limit_groups]

    out_path = Path(out)
    done = load_out(out_path)

    # Pula grupos cujos eventos JA estao todos no cache
    todo_groups = []
    for key, evs in group_list:
        if any(eid not in done for eid, _ in evs):
            todo_groups.append((key, evs))

    n_total_events = sum(len(evs) for _, evs in group_list)
    print(f"Grupos (liga+temporada): {len(group_list)}  Faltam processar: {len(todo_groups)}")
    print(f"Eventos fantasma totais: {n_total_events}  Ja no cache: {len(done)}")

    if not todo_groups:
        print("Nada a fazer.")
        return

    sem = asyncio.Semaphore(concurrency)
    lock = asyncio.Lock()
    ok = fail = 0
    groups_done = 0

    async def worker(key, evs):
        nonlocal ok, fail, groups_done
        league_path, season = key
        async with sem:
            result = await process_group(browser, league_path, season, evs)
        async with lock:
            for eid, v in result.items():
                done[eid] = v
                if v:
                    ok += 1
                else:
                    fail += 1
            groups_done += 1
            if groups_done % CHECKPOINT_EVERY_GROUPS == 0:
                save_out(out_path, done)
                print(f"  grupos {groups_done}/{len(todo_groups)}  ok={ok}  falhou={fail}", flush=True)

    async with OddsPortalBrowser(headful=False, concurrency=concurrency) as browser:
        await asyncio.gather(*(worker(key, evs) for key, evs in todo_groups))

    save_out(out_path, done)
    print(f"\nConcluido. ok={ok}  falhou={fail}  total_cache={len(done)}")
    print(f"Saida: {out_path}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument("--limit-groups", type=int, default=None)
    args = ap.parse_args()
    asyncio.run(main_async(args.db, args.out, args.concurrency, args.limit_groups))


if __name__ == "__main__":
    main()
