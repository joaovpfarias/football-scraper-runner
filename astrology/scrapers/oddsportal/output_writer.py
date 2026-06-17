"""
Escrita incremental em SQLite normalizado.

Schema (5 tabelas de lookup + 2 tabelas de dados):
  sports, leagues, teams, bookmakers, markets, submkts, outcomes  <- lookup
  events  <- 1 linha por jogo
  odds    <- N linhas por jogo (market x submarket x outcome x bookmaker)

Reducao vs CSV longo: ~40x menor (strings repetidas viram integers de 1-4 bytes).
Idempotencia: INSERT OR IGNORE em todas as tabelas.
"""
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

# Cutoff de deploy do fix de cobertura (selector/paginacao/false-empty).
# Entradas de season_state com n_matches=0 marcadas ANTES deste instante sao
# suspeitas (cache envenenado pelo bug antigo) e NAO devem ser confiadas como
# "season vazia" -> a season e re-listada pelo codigo corrigido. Vazios marcados
# DEPOIS do cutoff (codigo novo, selector+paginacao corretos) sao confiaveis.
# Sobrescrevivel por env CACHE_EPOCH (ISO UTC) para futuros redeploys.
CACHE_EPOCH = os.environ.get("CACHE_EPOCH", "2026-06-16T18:00:00Z")

# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------

DDL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous  = NORMAL;
PRAGMA cache_size   = -65536;

CREATE TABLE IF NOT EXISTS sports (
    id   INTEGER PRIMARY KEY,
    name TEXT    UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS leagues (
    id       INTEGER PRIMARY KEY,
    sport_id INTEGER NOT NULL REFERENCES sports(id),
    path     TEXT    UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS teams (
    id   INTEGER PRIMARY KEY,
    name TEXT    UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS bookmakers (
    id   INTEGER PRIMARY KEY,
    name TEXT    UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS markets (
    id   INTEGER PRIMARY KEY,
    name TEXT    UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS submkts (
    id   INTEGER PRIMARY KEY,
    name TEXT    UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS outcomes (
    id   INTEGER PRIMARY KEY,
    name TEXT    UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id            TEXT    PRIMARY KEY,
    league_id     INTEGER NOT NULL REFERENCES leagues(id),
    season        TEXT    DEFAULT '',
    home_id       INTEGER NOT NULL REFERENCES teams(id),
    away_id       INTEGER NOT NULL REFERENCES teams(id),
    dt_utc        TEXT    DEFAULT '',
    dt_local      TEXT    DEFAULT '',
    score_home    INTEGER,
    score_away    INTEGER,
    sets_detail   TEXT    DEFAULT '',
    status        TEXT    DEFAULT 'scheduled',
    venue         TEXT    DEFAULT '',
    venue_city    TEXT    DEFAULT '',
    venue_country TEXT    DEFAULT '',
    venue_lat     REAL,
    venue_lon     REAL,
    source_url    TEXT    DEFAULT '',
    scraped_at    TEXT    DEFAULT ''
);

CREATE TABLE IF NOT EXISTS odds (
    event_id     TEXT    NOT NULL REFERENCES events(id),
    market_id    INTEGER NOT NULL REFERENCES markets(id),
    submarket_id INTEGER NOT NULL REFERENCES submkts(id),
    outcome_id   INTEGER NOT NULL REFERENCES outcomes(id),
    bookmaker_id INTEGER NOT NULL REFERENCES bookmakers(id),
    odds_opening REAL,
    odds_closing REAL,
    payout_pct   TEXT    DEFAULT '',
    PRIMARY KEY (event_id, market_id, submarket_id, outcome_id, bookmaker_id)
);

-- Estado de raspagem por liga-season. Uma season PASSADA (ano < atual) de um
-- torneio FINALIZADO nao muda mais; uma vez raspada por completo, marcamos aqui
-- para PULAR antes de paginar a listagem nas proximas ondas (o skip por-jogo
-- antigo so evitava o fetch de detalhe DEPOIS de rastejar a listagem inteira).
-- E isso que permite o shard AVANCAR pela fila ate os tiers profundos (ITF).
CREATE TABLE IF NOT EXISTS season_state (
    league_path  TEXT    NOT NULL,
    season       TEXT    NOT NULL,
    n_matches    INTEGER DEFAULT 0,
    completed_at TEXT    DEFAULT '',
    PRIMARY KEY (league_path, season)
);

CREATE INDEX IF NOT EXISTS idx_odds_event      ON odds(event_id);
CREATE INDEX IF NOT EXISTS idx_odds_market     ON odds(market_id);
CREATE INDEX IF NOT EXISTS idx_events_league   ON events(league_id);
CREATE INDEX IF NOT EXISTS idx_events_dt       ON events(dt_utc);
CREATE INDEX IF NOT EXISTS idx_events_home     ON events(home_id);
CREATE INDEX IF NOT EXISTS idx_events_away     ON events(away_id);
CREATE INDEX IF NOT EXISTS idx_events_src_url  ON events(source_url);
CREATE INDEX IF NOT EXISTS idx_events_score    ON events(score_home);
"""

# ---------------------------------------------------------------------------
# SQLiteWriter
# ---------------------------------------------------------------------------

def _load_cache(con: sqlite3.Connection, table: str, key_col: str = "name") -> dict:
    rows = con.execute(f"SELECT {key_col}, id FROM {table}").fetchall()
    return {k: i for k, i in rows}


class SQLiteWriter:
    """
    Escreve odds no banco SQLite normalizado.

    Uso:
        writer = SQLiteWriter("data/odds.db")
        writer.write(row_dict)   # retorna True se inseriu, False se duplicata
        writer.close()           # flush final + fecha conexao
    """

    def __init__(self, path: str, flush_every: int = 500):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.flush_every = flush_every
        self._buf: list[tuple] = []
        self._new_count = 0

        self._con = sqlite3.connect(str(self.path), check_same_thread=False)
        self._con.executescript(DDL)
        self._con.commit()

        # Caches em memoria para IDs dos lookups (evita SELECT a cada linha)
        self._sports:     dict[str, int] = _load_cache(self._con, "sports")
        self._leagues:    dict[str, int] = _load_cache(self._con, "leagues", "path")
        self._teams:      dict[str, int] = _load_cache(self._con, "teams")
        self._bookmakers: dict[str, int] = _load_cache(self._con, "bookmakers")
        self._markets:    dict[str, int] = _load_cache(self._con, "markets")
        self._submkts:    dict[str, int] = _load_cache(self._con, "submkts")
        self._outcomes:   dict[str, int] = _load_cache(self._con, "outcomes")

    # ---------------------------------------------------------------- lookups

    def _lookup(self, cache: dict, table: str, name: str) -> int:
        if name not in cache:
            self._con.execute(
                f"INSERT OR IGNORE INTO {table}(name) VALUES (?)", (name,)
            )
            row = self._con.execute(
                f"SELECT id FROM {table} WHERE name = ?", (name,)
            ).fetchone()
            cache[name] = row[0]
        return cache[name]

    def _league_id(self, sport: str, path: str) -> int:
        if path not in self._leagues:
            sport_id = self._lookup(self._sports, "sports", sport)
            self._con.execute(
                "INSERT OR IGNORE INTO leagues(sport_id, path) VALUES (?, ?)",
                (sport_id, path),
            )
            row = self._con.execute(
                "SELECT id FROM leagues WHERE path = ?", (path,)
            ).fetchone()
            self._leagues[path] = row[0]
        return self._leagues[path]

    # ----------------------------------------------------------------- event

    def _upsert_event(self, row: dict, league_id: int,
                      home_id: int, away_id: int) -> None:
        sh = row.get("score_home")
        sa = row.get("score_away")
        sh_val = int(sh) if sh not in ("", None) else None
        sa_val = int(sa) if sa not in ("", None) else None
        status = row.get("status", "scheduled")

        sets_detail = row.get("sets_detail", "") or ""
        self._con.execute(
            """INSERT OR IGNORE INTO events
               (id, league_id, season, home_id, away_id,
                dt_utc, dt_local, score_home, score_away, sets_detail, status,
                venue, venue_city, venue_country, venue_lat, venue_lon,
                source_url, scraped_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                row["event_id"], league_id,
                row.get("season", ""),
                home_id, away_id,
                row.get("event_datetime_utc", ""),
                row.get("event_datetime_local", ""),
                sh_val, sa_val, sets_detail, status,
                row.get("venue", ""),
                row.get("venue_city", ""),
                row.get("venue_country", ""),
                float(row["venue_lat"]) if row.get("venue_lat") else None,
                float(row["venue_lon"]) if row.get("venue_lon") else None,
                row.get("source_url", ""),
                row.get("scraped_at_utc", ""),
            ),
        )
        # Se o evento ja existia sem placar, atualiza score/sets/status
        if sh_val is not None:
            self._con.execute(
                """UPDATE events SET score_home=?, score_away=?, sets_detail=?, status=?, scraped_at=?
                   WHERE id=? AND score_home IS NULL""",
                (sh_val, sa_val, sets_detail, status, row.get("scraped_at_utc", ""), row["event_id"]),
            )
        # Preenche a season quando o evento foi criado pela feed atual (season="")
        # e agora chega da passada com sufixo de ano. Sem isso ~28% ficava sem ano.
        season_val = row.get("season", "")
        if season_val:
            self._con.execute(
                "UPDATE events SET season=? WHERE id=? AND (season='' OR season IS NULL)",
                (season_val, row["event_id"]),
            )

    # ------------------------------------------------------------------ write

    def ensure_event(self, row: dict) -> None:
        """Persiste o evento com score/status mesmo sem odds. Util quando a pagina nao tem odds historicas."""
        league_id    = self._league_id(row["sport"], row["league"])
        home_id      = self._lookup(self._teams, "teams", row.get("home", ""))
        away_id      = self._lookup(self._teams, "teams", row.get("away", ""))
        self._upsert_event(row, league_id, home_id, away_id)
        self._con.commit()

    def write(self, row: dict) -> bool:
        """
        Enfileira uma linha de odds. Retorna True (a deduplicacao real e
        feita pelo PRIMARY KEY do SQLite no flush via INSERT OR IGNORE).
        """
        league_id    = self._league_id(row["sport"], row["league"])
        home_id      = self._lookup(self._teams,      "teams",      row.get("home", ""))
        away_id      = self._lookup(self._teams,      "teams",      row.get("away", ""))
        market_id    = self._lookup(self._markets,    "markets",    row.get("market", ""))
        submarket_id = self._lookup(self._submkts,    "submkts",    str(row.get("submarket", "")))
        outcome_id   = self._lookup(self._outcomes,   "outcomes",   row.get("outcome", ""))
        bookmaker_id = self._lookup(self._bookmakers, "bookmakers", row.get("bookmaker", ""))

        self._upsert_event(row, league_id, home_id, away_id)

        oc = row.get("odds_closing")
        op = row.get("odds_opening")
        self._buf.append((
            row["event_id"],
            market_id, submarket_id, outcome_id, bookmaker_id,
            float(op) if op not in ("", None) else None,
            float(oc) if oc not in ("", None) else None,
            row.get("payout_pct", ""),
        ))

        if len(self._buf) >= self.flush_every:
            self._flush()
        return True

    def _flush(self) -> None:
        if not self._buf:
            return
        cur = self._con.executemany(
            """INSERT OR IGNORE INTO odds
               (event_id, market_id, submarket_id, outcome_id, bookmaker_id,
                odds_opening, odds_closing, payout_pct)
               VALUES (?,?,?,?,?,?,?,?)""",
            self._buf,
        )
        self._con.commit()
        self._new_count += cur.rowcount
        self._buf.clear()

    def close(self) -> None:
        self._flush()
        self._con.execute("PRAGMA optimize;")
        self._con.close()

    @property
    def new_rows(self) -> int:
        return self._new_count

    # ----------------------------------------------------- season checkpoint

    def is_season_complete(self, league_path: str, season: str) -> bool:
        """True se esta liga-season ja foi raspada por completo numa onda anterior.

        Guard anti-poison: um cache n_matches=0 (season vazia) so e confiavel se
        foi gravado pelo codigo CORRIGIDO (completed_at >= CACHE_EPOCH). Vazios
        antigos podem ser false-empties do bug de selector/paginacao -> ignorados
        para que a season seja re-listada. Completes reais (n_matches>0) sempre valem.
        """
        row = self._con.execute(
            "SELECT n_matches, completed_at FROM season_state WHERE league_path=? AND season=?",
            (league_path, season),
        ).fetchone()
        if row is None:
            return False
        n_matches, completed_at = (row[0] or 0), (row[1] or "")
        if n_matches != 0:
            # n>0 = tem dados; n<0 = slug quebrado/404 conhecido (nao re-tentar — so
            # muda com correcao de slug, que e outra chave). Auditoria trata n<0 como gap.
            return True
        return completed_at >= CACHE_EPOCH

    def mark_season_complete(self, league_path: str, season: str, n_matches: int) -> None:
        """Marca a liga-season como totalmente raspada (so usar em seasons PASSADAS)."""
        self._con.execute(
            """INSERT INTO season_state(league_path, season, n_matches, completed_at)
               VALUES (?,?,?,?)
               ON CONFLICT(league_path, season) DO UPDATE SET
                 n_matches    = MAX(season_state.n_matches, excluded.n_matches),
                 completed_at = excluded.completed_at""",
            (league_path, season, int(n_matches),
             datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")),
        )
        self._con.commit()

    # ------------------------------------------------------------------ stats

    def stats(self) -> dict:
        """Retorna contagens das tabelas principais."""
        result = {}
        for t in ("events", "odds", "sports", "leagues", "teams", "bookmakers"):
            row = self._con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()
            result[t] = row[0]
        return result
