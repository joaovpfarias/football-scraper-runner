"""
Merge de DBs de shards (gerados pelo GitHub Actions matrix) num DB final.

Uso:
    python merge_shards.py <shards_dir> <output_db>

Cada shard tem seus proprios IDs auto-incrementados nas tabelas de lookup
(sports, teams, etc.). O merge:
  1. Cria DB final com schema vazio
  2. Para cada shard, copia lookups via INSERT OR IGNORE (dedupe por name)
  3. Constroi mapping {src_id: dest_id} por tabela
  4. Insere events e odds remapeando os FKs
"""
import sqlite3
import sys
from pathlib import Path

from output_writer import DDL  # mesmo schema usado pelo SQLiteWriter

LOOKUP_TABLES = ("sports", "teams", "bookmakers", "markets", "submkts", "outcomes")


def _build_mapping(main: sqlite3.Connection, src: sqlite3.Connection, table: str) -> dict[int, int]:
    """Copia entradas do src.{table} para main.{table} e devolve {src_id: main_id}."""
    mapping: dict[int, int] = {}
    for src_id, name in src.execute(f"SELECT id, name FROM {table}"):
        main.execute(f"INSERT OR IGNORE INTO {table}(name) VALUES (?)", (name,))
        row = main.execute(f"SELECT id FROM {table} WHERE name = ?", (name,)).fetchone()
        mapping[src_id] = row[0]
    return mapping


def _build_leagues_mapping(main: sqlite3.Connection, src: sqlite3.Connection,
                           sports_map: dict[int, int]) -> dict[int, int]:
    mapping: dict[int, int] = {}
    for src_id, src_sport_id, path in src.execute("SELECT id, sport_id, path FROM leagues"):
        new_sport = sports_map[src_sport_id]
        main.execute("INSERT OR IGNORE INTO leagues(sport_id, path) VALUES (?, ?)", (new_sport, path))
        row = main.execute("SELECT id FROM leagues WHERE path = ?", (path,)).fetchone()
        mapping[src_id] = row[0]
    return mapping


def _has_column(con: sqlite3.Connection, table: str, column: str) -> bool:
    cols = [r[1] for r in con.execute(f"PRAGMA table_info({table})")]
    return column in cols


def _has_table(con: sqlite3.Connection, table: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def merge_shard(main: sqlite3.Connection, shard_path: str) -> dict:
    """Merge de um DB shard no DB principal. Retorna stats."""
    src = sqlite3.connect(shard_path)
    src.execute("PRAGMA query_only = ON")  # safety

    # 1) Mapeia lookup tables simples
    maps = {t: _build_mapping(main, src, t) for t in LOOKUP_TABLES}

    # 2) Leagues (FK pra sports)
    leagues_map = _build_leagues_mapping(main, src, maps["sports"])

    # 3) Events — suporta DBs antigos sem sets_detail
    # Upsert: evento com score vence sobre evento sem score (seed com historico ganha)
    has_sets = _has_column(src, "events", "sets_detail")
    sets_col = ", sets_detail" if has_sets else ""
    ev_inserted = 0
    for row in src.execute(f"""
        SELECT id, league_id, season, home_id, away_id, dt_utc, dt_local,
               score_home, score_away{sets_col}, status, venue, venue_city, venue_country,
               venue_lat, venue_lon, source_url, scraped_at
        FROM events
    """):
        new_league = leagues_map.get(row[1])
        new_home = maps["teams"].get(row[3])
        new_away = maps["teams"].get(row[4])
        if new_league is None or new_home is None or new_away is None:
            continue
        if has_sets:
            vals = (row[0], new_league, row[2], new_home, new_away,
                    row[5], row[6], row[7], row[8], row[9], row[10],
                    row[11], row[12], row[13], row[14], row[15], row[16], row[17])
        else:
            vals = (row[0], new_league, row[2], new_home, new_away,
                    row[5], row[6], row[7], row[8], "", row[9],
                    row[10], row[11], row[12], row[13], row[14], row[15], row[16])
        cur = main.execute("""
            INSERT INTO events(
                id, league_id, season, home_id, away_id, dt_utc, dt_local,
                score_home, score_away, sets_detail, status,
                venue, venue_city, venue_country, venue_lat, venue_lon,
                source_url, scraped_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                score_home  = CASE WHEN events.score_home IS NULL OR events.score_home = ''
                                        AND (excluded.score_home IS NOT NULL AND excluded.score_home != '')
                                   THEN excluded.score_home ELSE events.score_home END,
                score_away  = CASE WHEN events.score_home IS NULL OR events.score_home = ''
                                        AND (excluded.score_away IS NOT NULL AND excluded.score_away != '')
                                   THEN excluded.score_away ELSE events.score_away END,
                sets_detail = CASE WHEN (events.score_home IS NULL OR events.score_home = '')
                                        AND excluded.sets_detail != ''
                                   THEN excluded.sets_detail ELSE events.sets_detail END,
                status      = CASE WHEN (events.score_home IS NULL OR events.score_home = '')
                                        AND excluded.status = 'finished'
                                   THEN excluded.status ELSE events.status END
        """, vals)
        ev_inserted += cur.rowcount

    # 4) Odds (FKs: market, submkt, outcome, bookmaker)
    odds_inserted = 0
    for row in src.execute("""
        SELECT event_id, market_id, submarket_id, outcome_id, bookmaker_id,
               odds_opening, odds_closing, payout_pct
        FROM odds
    """):
        new_market = maps["markets"].get(row[1])
        new_sub = maps["submkts"].get(row[2])
        new_outcome = maps["outcomes"].get(row[3])
        new_bm = maps["bookmakers"].get(row[4])
        if None in (new_market, new_sub, new_outcome, new_bm):
            continue
        cur = main.execute("""
            INSERT OR IGNORE INTO odds(
                event_id, market_id, submarket_id, outcome_id, bookmaker_id,
                odds_opening, odds_closing, payout_pct
            ) VALUES (?,?,?,?,?,?,?,?)
        """, (row[0], new_market, new_sub, new_outcome, new_bm, *row[5:]))
        odds_inserted += cur.rowcount

    # 5) season_state (cache B): season completa em qualquer shard vale globalmente.
    # DBs antigos (pre-B) nao tem a tabela -> ignora.
    if _has_table(src, "season_state"):
        for row in src.execute(
            "SELECT league_path, season, n_matches, completed_at FROM season_state"
        ):
            main.execute(
                """INSERT OR IGNORE INTO season_state(league_path, season, n_matches, completed_at)
                   VALUES (?,?,?,?)""",
                row,
            )

    main.commit()
    src.close()
    return {"events": ev_inserted, "odds": odds_inserted}


def main():
    if len(sys.argv) < 3:
        print("Uso: python merge_shards.py <shards_dir> <output_db>")
        sys.exit(1)

    shards_dir = Path(sys.argv[1])
    output_db = Path(sys.argv[2])

    output_db.parent.mkdir(parents=True, exist_ok=True)
    if output_db.exists():
        output_db.unlink()

    main_con = sqlite3.connect(str(output_db))
    main_con.executescript(DDL)
    main_con.commit()

    # Shards primeiro, seed por ultimo — upsert garante que seed (com scores) vence
    shard_dbs = (
        sorted(shards_dir.rglob("shard-*.db")) +
        [p for p in sorted(shards_dir.rglob("*.db")) if "shard-" not in p.name]
    )
    if not shard_dbs:
        print(f"[ERRO] Nenhum .db encontrado em {shards_dir}")
        sys.exit(2)

    print(f"[merge] {len(shard_dbs)} DBs para mergear:")
    totals = {"events": 0, "odds": 0}
    skipped = 0
    for db_path in shard_dbs:
        # Pula DBs vazios ou sem schema (shard que nao produziu dados)
        try:
            chk = sqlite3.connect(str(db_path))
            tables = {r[0] for r in chk.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            chk.close()
            if "sports" not in tables:
                print(f"  -> {db_path} [PULADO — sem schema]")
                skipped += 1
                continue
        except Exception as e:
            print(f"  -> {db_path} [PULADO — {e}]")
            skipped += 1
            continue

        print(f"  -> {db_path}")
        try:
            stats = merge_shard(main_con, str(db_path))
            totals["events"] += stats["events"]
            totals["odds"] += stats["odds"]
            print(f"     +{stats['events']} eventos, +{stats['odds']} odds")
        except Exception as e:
            print(f"     [ERRO ao mergear: {e}]")
            skipped += 1

    if skipped:
        print(f"[merge] {skipped} shard(s) pulados (vazios ou corrompidos)")

    # Auto-cura do bug falsy-0: evento 'finished' com EXATAMENTE um lado NULL teve
    # o 0 do perdedor (sets diretos) apagado pelo `or`. O unico int falsy e 0, entao
    # o lado faltante e definitivamente 0. Conserta resíduo de DBs/shards antigos.
    r1 = main_con.execute(
        "UPDATE events SET score_home=0 "
        "WHERE status='finished' AND score_home IS NULL AND score_away IS NOT NULL"
    ).rowcount
    r2 = main_con.execute(
        "UPDATE events SET score_away=0 "
        "WHERE status='finished' AND score_away IS NULL AND score_home IS NOT NULL"
    ).rowcount
    main_con.commit()
    if r1 or r2:
        print(f"[fix falsy-0] placar do perdedor (0) restaurado: {r1 + r2} eventos")

    # Stats finais
    final = {}
    for t in ("events", "odds", "leagues", "teams", "bookmakers"):
        final[t] = main_con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    main_con.execute("PRAGMA optimize")
    main_con.close()

    print("\n[merge] Concluido!")
    print(f"  DB final: {output_db}")
    print(f"  Eventos:    {final['events']}")
    print(f"  Odds:       {final['odds']}")
    print(f"  Torneios:   {final['leagues']}")
    print(f"  Bookmakers: {final['bookmakers']}")


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parent))
    main()
