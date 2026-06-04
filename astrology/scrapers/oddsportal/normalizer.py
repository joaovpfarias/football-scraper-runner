"""
Helpers de normalizacao de dados.
"""
import re
import hashlib
from datetime import datetime, timezone
from dateutil import parser as dtparser


def parse_odd(s) -> float | None:
    """
    Converte '1.87', '+150', '-110', '2/5', '-' em float decimal.
    Retorna None se nao parseavel.
    """
    if s is None:
        return None
    s = str(s).strip()
    if s in ("", "-", "-", "N/A", "nan", "NaN"):
        return None
    m = re.match(r"^([+-])(\d+)$", s)
    if m:
        sign, num = m.group(1), int(m.group(2))
        if sign == "+":
            return round(1 + num / 100, 4)
        if num > 0:
            return round(1 + 100 / num, 4)
    try:
        v = float(s.replace(",", "."))
        if v > 1.0:
            return v
    except ValueError:
        pass
    m = re.match(r"^(\d+)/(\d+)$", s)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        if b > 0:
            return round(1 + a / b, 4)
    return None


def normalize_team_name(s: str) -> str:
    """Remove espacos duplos, caracteres unicode estranhos."""
    if not s:
        return ""
    return " ".join(str(s).strip().split())


def unix_to_iso_utc(unix_ts) -> str:
    return datetime.fromtimestamp(int(unix_ts), tz=timezone.utc).isoformat()


def parse_datetime_any(s: str) -> str | None:
    """Tenta parsear qualquer string de data/hora, retorna ISO 8601 UTC ou None."""
    if not s:
        return None
    try:
        dt = dtparser.parse(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return None


_MONTHS_PT = {
    'jan': 1, 'fev': 2, 'mar': 3, 'abr': 4, 'mai': 5, 'jun': 6,
    'jul': 7, 'ago': 8, 'set': 9, 'out': 10, 'nov': 11, 'dez': 12,
}


def parse_pt_datetime(text: str) -> str | None:
    """
    Parse Portuguese datetime string -> ISO 8601 UTC.
    Handles:
      "Hoje, 19 Abr 2026, 00:30"
      "Ontem, 18 Abr 2026, 00:30"
      "17 Abr 2026  - Playoffs de Acesso"
      "17 Abr 2026"
      "Hoje, 19 Abr  - Playoffs" (date-only, returns midnight UTC)
    """
    from datetime import datetime, timezone, date as dt_date, timedelta
    if not text:
        return None
    text = text.strip()
    # Extract time HH:MM
    tm = re.search(r'(\d{1,2}):(\d{2})(?!\d)', text)
    hour, minute = (int(tm.group(1)), int(tm.group(2))) if tm else (0, 0)
    # Full date DD Mmm YYYY
    dm = re.search(r'(\d{1,2})\s+(\w{3})\s+(\d{4})', text, re.IGNORECASE)
    if dm:
        day, mon, year = int(dm.group(1)), _MONTHS_PT.get(dm.group(2).lower(), 0), int(dm.group(3))
        if mon:
            try:
                return datetime(year, mon, day, hour, minute, tzinfo=timezone.utc).isoformat()
            except ValueError:
                pass
    # Relative: Hoje / Ontem
    today = dt_date.today()
    tl = text.lower()
    if 'hoje' in tl:
        d = today
    elif 'ontem' in tl:
        d = today - timedelta(days=1)
    else:
        # DD Mmm without year
        dm2 = re.search(r'(\d{1,2})\s+(\w{3})', text, re.IGNORECASE)
        if dm2:
            day, mon = int(dm2.group(1)), _MONTHS_PT.get(dm2.group(2).lower(), 0)
            if mon:
                try:
                    return datetime(today.year, mon, day, hour, minute, tzinfo=timezone.utc).isoformat()
                except ValueError:
                    pass
        return None
    return datetime(d.year, d.month, d.day, hour, minute, tzinfo=timezone.utc).isoformat()


def stable_event_id(sport: str, league: str, home: str, away: str, iso_utc: str) -> str:
    """ID reprodutivel de um evento - usado como chave de idempotencia no CSV."""
    base = f"{sport}|{league}|{normalize_team_name(home)}|{normalize_team_name(away)}|{(iso_utc or '')[:10]}"
    return hashlib.sha1(base.encode()).hexdigest()[:16]
