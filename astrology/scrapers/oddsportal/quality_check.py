"""
quality_check.py — verifica completude do DB mergeado.

Uso:
    python quality_check.py <db_path> [output_json]

Saida: incomplete_leagues.json com lista de {league, season, total, scored}.
Exit code 0 = tudo completo, 1 = ha dados incompletos.
"""
import json
import sqlite3
import sys
from pathlib import Path


def check(db_path: str, output_json: str = "incomplete_leagues.json") -> list[dict]:
    con = sqlite3.connect(db_path)
    rows = con.execute("""
        SELECT l.path, e.season,
               COUNT(*)  AS total,
               SUM(CASE WHEN e.score_home IS NOT NULL AND e.score_home != '' THEN 1 ELSE 0 END) AS scored,
               SUM(CASE WHEN EXISTS (
                     SELECT 1 FROM odds o JOIN markets m ON m.id = o.market_id
                     WHERE o.event_id = e.id AND m.name = 'home_away'
                   ) THEN 1 ELSE 0 END) AS with_odds
        FROM events e
        JOIN leagues l ON l.id = e.league_id
        -- So eventos passados ha 2+ dias contam: jogos agendados/ao vivo nunca
        -- teriam score e inflavam a metrica para sempre (1.4k -> 2.3k "incompletas").
        -- dt_utc e ISO 'YYYY-MM-DDT...' — comparacao lexical com datetime() funciona
        -- no nivel do dia, suficiente para um corte de -2 dias.
        WHERE e.dt_utc IS NOT NULL AND e.dt_utc != ''
          AND e.dt_utc < datetime('now', '-2 days')
        GROUP BY l.path, e.season
        HAVING scored < total OR with_odds < total
        ORDER BY (total - scored) DESC
    """).fetchall()
    con.close()

    incomplete = [
        {"league": r[0], "season": r[1], "total": r[2], "scored": r[3], "with_odds": r[4]}
        for r in rows
    ]

    Path(output_json).write_text(
        json.dumps(incomplete, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    total_missing = sum(r["total"] - r["scored"] for r in incomplete)
    print(f"[quality] {len(incomplete)} (liga, season) incompletas | {total_missing} jogos sem score")
    if incomplete:
        print(f"[quality] Top 5 mais incompletas:")
        for r in incomplete[:5]:
            pct = round(r['scored'] / r['total'] * 100) if r['total'] else 0
            print(f"  {r['league']} ({r['season'] or 'atual'}): {r['scored']}/{r['total']} scored ({pct}%)")
    return incomplete


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python quality_check.py <db_path> [output_json]")
        sys.exit(1)
    db = sys.argv[1]
    out = sys.argv[2] if len(sys.argv) > 2 else "incomplete_leagues.json"
    result = check(db, out)
    sys.exit(1 if result else 0)
