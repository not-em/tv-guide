#!/usr/bin/env python3
"""
strictly/scraper.py

Scrapes per-week scores from Wikipedia for Strictly Come Dancing series 10–22
and writes a single tidy CSV: strictly_scores.csv

Series–year mapping
-------------------
Series 10 → 2012   Series 14 → 2016   Series 18 → 2020   Series 22 → 2024
Series 11 → 2013   Series 15 → 2017   Series 19 → 2021
Series 12 → 2014   Series 16 → 2018   Series 20 → 2022
Series 13 → 2015   Series 17 → 2019   Series 21 → 2023

Output columns
--------------
series          int     e.g. 10
celebrity       str     e.g. "Louis Smith"
professional    str     e.g. "Flavia Cacace"
week            int     1-based week number
dance           str     e.g. "Waltz"
song            str     e.g. "The Way You Look Tonight" (if available)
artist          str     e.g. "Michael Bublé" (if available)
total_score     int     total out of 40 (or higher in the final)
craig           int/NaN always present
darcey          int/NaN series 10–17
len             int/NaN series 10–15
bruno           int/NaN series 10–21
shirley         int/NaN series 16+
motsi           int/NaN series 17+
anton           int/NaN series 19+
verdict         str     "safe", "bottom-two", "eliminated", "runner-up", "winner",
                        or "" for continuation rows (multi-dance weeks)

Usage
-----
    pip install requests beautifulsoup4 pandas
    python scraper.py

Re-run behaviour
----------------
Raw HTML is cached in data/ so Wikipedia is not re-fetched unless you delete
the cache files.  The CSV is overwritten on every run.

Notes
-----
- Weeks with no score (bye, withdrawal) are omitted.
- The final has multiple dances per couple; each gets its own row.
- Judge columns are NaN when that judge was not on the panel that series.
"""

import pathlib
import re
import time
import traceback
from typing import Any, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Add series 23 here once its data is complete on Wikipedia.
SERIES: list[int] = list(range(10, 23))  # series 10–22 inclusive

WIKI_URL = "https://en.wikipedia.org/wiki/Strictly_Come_Dancing_series_{}"

# ---- Judge panels --------------------------------------------------------
# Only regular (non-guest) judges are listed.  Craig Revel Horwood has been
# present for every series; all other judges joined or left at various points.
_JUDGES_EARLY = ["craig", "darcey", "len", "bruno"]  # series 10–15 panel

JUDGES_BY_SERIES: dict[int, list[str]] = {
    **{s: list(_JUDGES_EARLY) for s in range(10, 16)},
    16: ["craig", "darcey", "shirley", "bruno"],
    17: ["craig", "darcey", "shirley", "motsi", "bruno"],
    18: ["craig", "shirley", "motsi", "bruno"],
    19: ["craig", "shirley", "motsi", "anton", "bruno"],
    **{s: ["craig", "shirley", "motsi", "anton"] for s in range(20, 23)},
}

# All judges who have ever sat on the panel — defines extra CSV columns.
ALL_JUDGES: list[str] = ["craig", "darcey", "len", "bruno", "shirley", "motsi", "anton"]

# ---- Output schema -------------------------------------------------------
# Single source of truth for CSV column order.
COLUMNS: list[str] = [
    "series",
    "celebrity",
    "professional",
    "week",
    "dance",
    "song",
    "artist",
    "total_score",
    *ALL_JUDGES,
    "verdict",
]

# ---- Paths ---------------------------------------------------------------
# Resolved relative to this file so the script works regardless of cwd.
_HERE = pathlib.Path(__file__).parent
CACHE_DIR = _HERE / "data"  # cached Wikipedia HTML files
OUT_FILE = _HERE / "strictly_scores.csv"

# ---- Network -------------------------------------------------------------
HEADERS = {"User-Agent": "StrictlyDataProject/1.0 (educational; contact via github)"}
DELAY = 1.5  # seconds between live Wikipedia requests — be polite

# ---------------------------------------------------------------------------
# Compiled regular expressions
# ---------------------------------------------------------------------------

# Wikipedia footnote references: "[1]", "[note 2]", etc.
_FOOTNOTE_RE = re.compile(r"\[.*?]")
# Wikipedia annotation symbols used as footnote markers in table cells.
_ANNOTATION_RE = re.compile(r"[†‡★☆♦]")
# Separator between song title and artist: em-dash (—), en-dash (–), or hyphen.
_SONG_SPLIT_RE = re.compile(r"\s*[—–-]\s*")

# ---------------------------------------------------------------------------
# Verdict mapping
# ---------------------------------------------------------------------------

# Maps raw result-cell text (lower-cased) to a normalised verdict string.
# Keys not present in this map are left as "" — this handles cases where the
# last cell contains a song title rather than a result (e.g. weeks with no
# Result column).
_VERDICT_MAP: dict[str, str] = {
    "safe": "safe",
    "bottom two": "bottom-two",
    "eliminated": "eliminated",
    "withdrew": "eliminated",  # treated the same as elimination
    "winners": "winner",
    "runners-up": "runner-up",
    "fourth place": "runner-up",  # used in some series finals
}

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def fetch_html(series_num: int) -> str:
    """Return the Wikipedia HTML for *series_num*, using a local cache.

    On the first call for a given series the page is fetched from Wikipedia
    and written to ``CACHE_DIR/series_<n>.html``.  Subsequent calls read from
    that file so Wikipedia is not hit again.

    Args:
        series_num: The series number to fetch (e.g. 10).

    Returns:
        The full HTML source of the Wikipedia page as a string.

    Raises:
        requests.HTTPError: If the HTTP response indicates an error status.
    """
    CACHE_DIR.mkdir(exist_ok=True)
    cache_file = CACHE_DIR / f"series_{series_num}.html"

    if cache_file.exists():
        print(f"  [cache] series {series_num}")
        return cache_file.read_text(encoding="utf-8")

    url = WIKI_URL.format(series_num)
    print(f"  [fetch] series {series_num} — {url}")
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    cache_file.write_text(resp.text, encoding="utf-8")
    time.sleep(DELAY)
    return resp.text


def clean(text: str) -> str:
    """Normalise a Wikipedia table cell string.

    Removes:
    - Footnote references such as ``[1]`` or ``[note 2]``.
    - Annotation symbols (†, ‡, ★, ☆, ♦) used as in-table markers.
    - Leading and trailing whitespace.

    Args:
        text: Raw text extracted from a BeautifulSoup element.

    Returns:
        The cleaned string, or ``""`` if *text* is falsy.
    """
    if not text:
        return ""
    text = _FOOTNOTE_RE.sub("", text)
    text = _ANNOTATION_RE.sub("", text)
    return text.strip()


def parse_int(text: str) -> Optional[int]:
    """Return the leading integer found in *text*, or ``None``.

    Handles compound score strings such as ``"28 (6, 7, 8, 7)"`` by taking
    only the first number.

    Args:
        text: A string that may start with a digit.

    Returns:
        The integer value of the leading digit sequence, or ``None`` if no
        digits are found.
    """
    m = re.match(r"(\d+)", clean(text))
    return int(m.group(1)) if m else None


def split_couple(couple_str: str) -> tuple[str, str]:
    """Split a paired name string into ``(celebrity, professional)``.

    Expects the format ``"FirstName & FirstName"`` as used in Wikipedia's
    weekly scoring tables.  If no ``&`` separator is found, the full string
    is returned as the celebrity name and the professional is ``""``.

    Args:
        couple_str: Raw couple cell text, e.g. ``"Louis & Flavia"``.

    Returns:
        A ``(celebrity_first, professional_first)`` tuple.
    """
    parts = re.split(r"\s*&\s*", clean(couple_str), maxsplit=1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return couple_str.strip(), ""


def _split_song_artist(text: str) -> tuple[str, str]:
    """Split a ``"Song – Artist"`` cell into ``(song, artist)``.

    Wikipedia uses em-dash (—), en-dash (–), or a plain hyphen (-) as the
    separator, often inconsistently.  Surrounding whitespace and leading/
    trailing quotation marks on the song title are stripped.

    Args:
        text: The raw music-cell text.

    Returns:
        A ``(song, artist)`` tuple.  *artist* is ``""`` when no separator is
        found.
    """
    m = _SONG_SPLIT_RE.search(text)
    if m:
        song = text[: m.start()].strip().strip('"')
        artist = text[m.end() :].strip()
        return song, artist
    return text.strip(), ""


def _make_empty_row(
    series_num: int,
    celebrity: str,
    professional: str,
    week_num: int,
) -> dict:
    """Return a row dict pre-populated with all column defaults.

    String fields default to ``""``; numeric fields (``total_score`` and all
    judge columns) default to ``None``, which becomes ``NaN`` in the DataFrame.

    Args:
        series_num:   Series number (e.g. 10).
        celebrity:    Full celebrity name.
        professional: Full professional dancer name.
        week_num:     1-based week number.

    Returns:
        A dict with every key from ``COLUMNS`` set to its default value.
    """
    row: dict = {
        "series": series_num,
        "celebrity": celebrity,
        "professional": professional,
        "week": week_num,
        "dance": "",
        "song": "",
        "artist": "",
        "total_score": None,
        "verdict": "",
    }
    for judge in ALL_JUDGES:
        row[judge] = None
    return row


# ---------------------------------------------------------------------------
# Couples section parser
# ---------------------------------------------------------------------------


def _parse_couples_section(
    soup: BeautifulSoup,
) -> tuple[dict[str, str], dict[str, str]]:
    """Build name-lookup dicts from the series' ``Couples`` table.

    The weekly scoring tables identify participants by first name only (or
    ``"Dr. <name>"`` for medical-title celebrities), so this function creates
    mappings from those short keys to full names.

    Handles mid-series professional substitutions such as
    ``"Amy Dowden (Weeks 1–6)Lauren Oakley (Weeks 7–13)"`` by registering
    both professionals under their own first-name keys.

    Args:
        soup: Parsed BeautifulSoup object for the full series page.

    Returns:
        A ``(celeb_names, pro_names)`` tuple where each dict maps
        ``{short_key -> full_name}``.
    """
    celeb_names: dict[str, str] = {}
    pro_names: dict[str, str] = {}

    for tag in soup.find_all(["h2", "h3", "h4"]):
        if clean(tag.get_text()) != "Couples":
            continue

        table = tag.find_next("table")
        if not table:
            break

        for tr in table.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            if len(cells) < 3:
                continue

            celeb_full = clean(cells[0].get_text())
            pro_raw = clean(cells[2].get_text())

            # Skip header rows.
            if not celeb_full or celeb_full.lower() in ("celebrity", "couple"):
                continue

            # Build the celebrity key.
            # Weekly tables show "Dr. Punam" (two tokens) for Dr. celebrities
            # and a single first name for everyone else.
            words = celeb_full.split()
            celeb_key = f"{words[0]} {words[1]}" if words[0] == "Dr." else words[0]
            celeb_names[celeb_key] = celeb_full

            # Build professional key(s).
            # Split on parenthetical segments to handle substitution strings.
            for segment in re.split(r"\s*\([^)]*\)\s*", pro_raw):
                pro_full = segment.strip()
                if not pro_full:
                    continue
                pro_key = pro_full.split()[0]
                pro_names[pro_key] = pro_full

        break  # only one Couples section per page

    return celeb_names, pro_names


# ---------------------------------------------------------------------------
# Per-series and per-week parsers
# ---------------------------------------------------------------------------


def parse_series(series_num: int, html: str) -> list[dict]:
    """Parse all scoring data from a series Wikipedia page.

    Locates each ``Week N`` heading, finds the table that follows it, and
    delegates to :func:`parse_week_table`.  Falls back to
    :func:`parse_series_summary_only` if no week headings are found.

    Args:
        series_num: The series number (e.g. 10).
        html:       Full HTML source of the Wikipedia page.

    Returns:
        A list of row dicts, one per ``(couple, week, dance)``, sorted by
        week number.
    """
    soup = BeautifulSoup(html, "html.parser")
    judges = JUDGES_BY_SERIES.get(series_num, list(_JUDGES_EARLY))
    celeb_names, pro_names = _parse_couples_section(soup)

    # Collect {week_number: <table element>} for each "Week N" heading.
    week_sections: dict[int, Any] = {}
    for tag in soup.find_all(["h2", "h3", "h4"]):
        m = re.match(r"Week\s+(\d+)", clean(tag.get_text()), re.I)
        if m:
            sibling = tag.find_next("table")
            if sibling:
                week_sections[int(m.group(1))] = sibling

    if not week_sections:
        print(f"  [warn] series {series_num}: no week sections found, trying fallback")
        return parse_series_summary_only(series_num, soup)

    return [
        row
        for week_num, table in sorted(week_sections.items())
        for row in parse_week_table(
            series_num, week_num, table, judges, celeb_names, pro_names
        )
    ]


def parse_week_table(
    series_num: int,
    week_num: int,
    table,
    judges: list[str],
    celeb_names: dict[str, str],
    pro_names: dict[str, str],
) -> list[dict]:
    """Parse one week's scoring table into a list of row dicts.

    Each ``<tr>`` is classified as one of:

    - **Named row** — contains ``"&"`` in the first cell, identifying a couple.
      Updates the running celebrity/professional and (optionally) verdict.
    - **Continuation row** — first cell is a score integer.  Represents a
      second or third dance for the same couple in a multi-dance week.  Inherits
      the couple and verdict from the most recent named row.
    - **Header / empty row** — skipped.

    Individual judge scores are extracted from a parenthetical in the score
    cell, e.g. ``"28 (6, 7, 8, 7)"``.

    Args:
        series_num:  Series number (written into every output row).
        week_num:    1-based week number (written into every output row).
        table:       BeautifulSoup ``<table>`` element for this week.
        judges:      Ordered list of judge name keys for this series.
        celeb_names: ``{short_key -> full_name}`` for celebrities.
        pro_names:   ``{short_key -> full_name}`` for professionals.

    Returns:
        A list of row dicts for this week.
    """
    rows: list[dict] = []

    # Running state — updated as we encounter named rows.
    last_celebrity = ""
    last_professional = ""
    last_verdict = ""

    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 2:
            continue

        texts = [clean(c.get_text()) for c in cells]

        # Skip header rows (first cell is "Couple" or blank).
        if texts[0].lower() in ("couple", ""):
            continue

        # ------------------------------------------------------------------
        # Classify the row and extract score / dance / song fields.
        # ------------------------------------------------------------------
        if "&" in texts[0]:
            # Named row: first cell identifies the couple.
            celeb_first, pro_first = split_couple(texts[0])
            last_celebrity = celeb_names.get(celeb_first, celeb_first)
            last_professional = pro_names.get(pro_first, pro_first)
            last_verdict = ""  # reset until a result cell confirms a verdict

            score_text = texts[1]
            dance = texts[2] if len(texts) > 2 else ""
            song_text = texts[3] if len(texts) > 3 else ""

            # Only update the verdict when a recognised key is present.
            # On weeks without a Result column (e.g. week 1) texts[-1] is a
            # song title — mapping it would silently corrupt last_verdict.
            mapped = _VERDICT_MAP.get(texts[-1].lower())
            if mapped is not None:
                last_verdict = mapped

        elif parse_int(texts[0]) is not None and last_celebrity:
            # Continuation row: no couple name, score is in cell 0.
            # Inherits couple identity and verdict from the preceding named row.
            score_text = texts[0]
            dance = texts[1] if len(texts) > 1 else ""
            song_text = texts[2] if len(texts) > 2 else ""

        else:
            continue  # unrecognised row shape — skip

        total = parse_int(score_text)
        if total is None:
            continue  # row has no usable score

        # Extract per-judge scores from "(6, 7, 8, 7)" parenthetical.
        judge_scores: dict[str, Optional[int]] = {}
        paren = re.search(r"\(([^)]+)\)", score_text)
        if paren:
            score_parts = [p.strip() for p in paren.group(1).split(",")]
            for i, judge in enumerate(judges):
                if i < len(score_parts):
                    judge_scores[judge] = parse_int(score_parts[i])

        song, artist = _split_song_artist(song_text)

        row = _make_empty_row(series_num, last_celebrity, last_professional, week_num)
        row.update(
            {
                "dance": dance,
                "song": song,
                "artist": artist,
                "total_score": total,
                "verdict": last_verdict,
                **{judge: judge_scores.get(judge) for judge in ALL_JUDGES},
            }
        )
        rows.append(row)

    return rows


def parse_series_summary_only(series_num: int, soup: BeautifulSoup) -> list[dict]:
    """Fallback parser using the summary scoring chart (weeks as columns).

    Used when no per-week headed sections are found.  Produces rows with
    ``total_score`` only — dance type and individual judge scores are absent.

    Args:
        series_num: Series number (used for logging and column values).
        soup:       Parsed BeautifulSoup object for the full series page.

    Returns:
        A list of row dicts, one per ``(couple, week)`` with a non-null score.
    """
    print(f"  [fallback] series {series_num}: using summary scoring chart")
    rows: list[dict] = []

    # The summary chart has "Couple" in the first column, then a column per week.
    for table in soup.find_all("table", class_="wikitable"):
        headers = [clean(th.get_text()) for th in table.find_all("th")]
        if not headers or "Couple" not in headers[0]:
            continue

        week_cols = [
            int(m.group(1)) for h in headers[1:] if (m := re.match(r"(\d+)", h))
        ]
        if not week_cols:
            continue

        for tr in table.find_all("tr")[1:]:
            cells = [clean(td.get_text()) for td in tr.find_all("td")]
            if not cells or "&" not in cells[0]:
                continue

            celebrity, professional = split_couple(cells[0])
            for i, week_num in enumerate(week_cols):
                if i + 1 >= len(cells):
                    break
                total = parse_int(cells[i + 1])
                if total is None:
                    continue
                row = _make_empty_row(series_num, celebrity, professional, week_num)
                row["total_score"] = total
                rows.append(row)

        break  # use only the first matching table

    return rows


# ---------------------------------------------------------------------------
# DataFrame post-processing
# ---------------------------------------------------------------------------


def _build_dataframe(all_rows: list[dict]) -> pd.DataFrame:
    """Convert raw row dicts into a typed, normalised DataFrame.

    Steps applied:
    - Cast ``series`` and ``week`` to ``int``.
    - Cast ``total_score`` and all judge columns to numeric (invalid → NaN).
    - Title-case ``dance`` to normalise variants such as ``"Cha-cha"`` → ``"Cha-Cha"``.

    Args:
        all_rows: List of row dicts produced by the parsers.

    Returns:
        A DataFrame with columns in the order defined by ``COLUMNS``.
    """
    df = pd.DataFrame(all_rows, columns=COLUMNS)

    df["series"] = df["series"].astype(int)
    df["week"] = df["week"].astype(int)
    df["total_score"] = pd.to_numeric(df["total_score"], errors="coerce")
    for judge in ALL_JUDGES:
        df[judge] = pd.to_numeric(df[judge], errors="coerce")

    df["dance"] = df["dance"].str.title()

    return df


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Scrape all configured series and write the combined CSV."""
    all_rows: list[dict] = []

    for series_num in SERIES:
        print(f"\nSeries {series_num}:")
        try:
            html = fetch_html(series_num)
            rows = parse_series(series_num, html)
            print(f"  {len(rows)} rows parsed")
            all_rows.extend(rows)
        except Exception as e:
            print(f"  [error] series {series_num}: {e}")
            traceback.print_exc()

    if not all_rows:
        print("\nNo data scraped — check errors above.")
        return

    df = _build_dataframe(all_rows)
    df.to_csv(OUT_FILE, index=False)

    print(f"\nDone — {len(df)} rows written to {OUT_FILE}")
    print(
        f"\nSeries coverage:\n{df.groupby('series')['celebrity'].nunique().to_string()}"
    )
    print(f"\nSample:\n{df.tail(5).to_string()}")


if __name__ == "__main__":
    main()
