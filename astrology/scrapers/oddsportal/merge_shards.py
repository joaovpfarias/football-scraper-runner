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

    # 3) Events — futebol: partials + scores por tempo
    # Upsert: evento com score vence sobre evento sem score (seed com historico ganha)
    has_partials = _has_column(src, "events", "partials")
    partials_col = ", partials, score_home_ht, score_away_ht, score_home_2h, score_away_2h" if has_partials else ""
    ev_inserted = 0
    for row in src.execute(f"""
        SELECT id, league_id, season, home_id, away_id, dt_utc, dt_local,
               score_home, score_away{partials_col}, status, venue, venue_city, venue_country,
               venue_lat, venue_lon, source_url, scraped_at
        FROM events
    """):
        new_league = leagues_map.get(row[1])
        new_home = maps["teams"].get(row[3])
        new_away = maps["teams"].get(row[4])
        if new_league is None or new_home is None or new_away is None:
            continue
        if has_partials:
            # SELECT returns 22 cols:
            # 0=id,1=league_id,2=season,3=home_id,4=away_id,5=dt_utc,6=dt_local,
            # 7=score_home,8=score_away,9=partials,10=sh_ht,11=sa_ht,12=sh_2h,13=sa_2h,
            # 14=status,15=venue,16=venue_city,17=venue_country,18=lat,19=lon,20=source_url,21=scraped_at
            vals = (row[0], new_league, row[2], new_home, new_away,
                    row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13],
                    row[14], row[15], row[16], row[17], row[18], row[19], row[20], row[21])
        else:
            # SELECT returns 17 cols (sem partials):
            # 0=id,1=league_id,2=season,3=home_id,4=away_id,5=dt_utc,6=dt_local,
            # 7=score_home,8=score_away,9=status,10=venue,11=venue_city,12=venue_country,
            # 13=lat,14=lon,15=source_url,16=scraped_at
            vals = (row[0], new_league, row[2], new_home, new_away,
                    row[5], row[6], row[7], row[8], "", None, None, None, None,
                    row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16])
        cur = main.execute("""
            INSERT INTO events(
                id, league_id, season, home_id, away_id, dt_utc, dt_local,
                score_home, score_away, partials,
                score_home_ht, score_away_ht, score_home_2h, score_away_2h,
                status, venue, venue_city, venue_country, venue_lat, venue_lon,
                source_url, scraped_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                score_home    = CASE WHEN events.score_home IS NULL AND excluded.score_home IS NOT NULL
                                     THEN excluded.score_home ELSE events.score_home END,
                score_away    = CASE WHEN events.score_home IS NULL AND excluded.score_away IS NOT NULL
                                     THEN excluded.score_away ELSE events.score_away END,
                partials      = CASE WHEN events.partials = '' AND excluded.partials != ''
                                     THEN excluded.partials ELSE events.partials END,
                score_home_ht = CASE WHEN events.score_home_ht IS NULL AND excluded.score_home_ht IS NOT NULL
                                     THEN excluded.score_home_ht ELSE events.score_home_ht END,
                score_away_ht = CASE WHEN events.score_away_ht IS NULL AND excluded.score_away_ht IS NOT NULL
                                     THEN excluded.score_away_ht ELSE events.score_away_ht END,
                score_home_2h = CASE WHEN events.score_home_2h IS NULL AND excluded.score_home_2h IS NOT NULL
                                     THEN excluded.score_home_2h ELSE events.score_home_2h END,
                score_away_2h = CASE WHEN events.score_away_2h IS NULL AND excluded.score_away_2h IS NOT NULL
                                     THEN excluded.score_away_2h ELSE events.score_away_2h END,
                status        = CASE WHEN events.score_home IS NULL AND excluded.status = 'finished'
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
                """INSERT INTO season_state(league_path, season, n_matches, completed_at)
                   VALUES (?,?,?,?)
                   ON CONFLICT(league_path, season) DO UPDATE SET
                     n_matches    = MAX(season_state.n_matches, excluded.n_matches),
                     completed_at = excluded.completed_at""",
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
    # o 0 do perdedor (scores diretos) apagado pelo `or`. O unico int falsy e 0, entao
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

    # Achado 4: infere season do dt_utc para eventos coletados via feed atual (season='')
    r_season = main_con.execute(
        "UPDATE events SET season = substr(dt_utc, 1, 4) "
        "WHERE season = '' AND dt_utc != '' AND length(dt_utc) >= 4"
    ).rowcount
    main_con.commit()
    if r_season:
        print(f"[fix season] {r_season} eventos tiveram season inferido do dt_utc")

    # Achado 2: Targeted DELETE remove false-empty entries onde events existem
    r_del = main_con.execute("""
        DELETE FROM season_state
        WHERE n_matches = 0
        AND EXISTS (
            SELECT 1 FROM events e
            JOIN leagues l ON e.league_id = l.id
            WHERE l.path = season_state.league_path
            AND e.season = season_state.season
        )
    """).rowcount
    main_con.commit()
    if r_del:
        print(f"[fix season_state] {r_del} entradas false-empty removidas (eventos existem para essa liga+season)")

    # Stats finais
    final = {}
    for t in ("events", "odds", "leagues", "teams", "bookmakers"):
        final[t] = main_con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    if _has_table(main_con, "season_state"):
        final["seasons_cached"] = main_con.execute("SELECT COUNT(*) FROM season_state").fetchone()[0]
        final["seasons_empty"]  = main_con.execute("SELECT COUNT(*) FROM season_state WHERE n_matches=0").fetchone()[0]
    else:
        final["seasons_cached"] = 0
        final["seasons_empty"]  = 0
    main_con.execute("PRAGMA optimize")
    # Checkpoint WAL->main antes de fechar: sem isso, o .db publicado no release
    # pode ficar sem as ultimas escritas (o publish so sobe o .db, nao o -wal/-shm
    # que o SQLite mantem em modo WAL). Mesmo checkpoint que ja existe por-shard,
    # faltava para o arquivo FINAL mergeado (causa suspeita da corrupcao
    # 'database disk image is malformed' vista no release em 2026-08-06).
    main_con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    main_con.close()

    print("\n[merge] Concluido!")
    print(f"  DB final: {output_db}")
    print(f"  Eventos:    {final['events']}")
    print(f"  Odds:       {final['odds']}")
    print(f"  Torneios:   {final['leagues']}")
    print(f"  Bookmakers: {final['bookmakers']}")
    print(f"  Seasons cacheadas: {final['seasons_cached']} ({final['seasons_empty']} vazias)")


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parent))
    main()
