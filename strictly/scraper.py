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
celeb_dob       str     celebrity date of birth (YYYY-MM-DD), or "" if unavailable
celeb_age       float   celebrity's age in whole years at the series première, or NaN
celeb_gender    str     "M", "F", or "" if undetermined
pro_gender      str     "M", "F", or "" if undetermined
same_sex        bool    True when both genders are known and match

Usage
-----
    pip install requests beautifulsoup4 pandas
    python scraper.py

Re-run behaviour
----------------
Raw HTML is cached in data/ so Wikipedia is not re-fetched unless you delete
the cache files.  The CSV is overwritten on every run.

Person data (DOB + gender) is fetched in a single batched SPARQL query to the
Wikidata Query Service and cached as data/people.json.  Delete that file to
force a refresh (e.g. after adding a new series).

Notes
-----
- Weeks with no score (bye, withdrawal) are omitted.
- The final has multiple dances per couple; each gets its own row.
- Judge columns are NaN when that judge was not on the panel that series.
"""

import json
import pathlib
import re
import textwrap
import time
import traceback
from datetime import date
from typing import Any, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SERIES: list[int] = list(range(10, 23))  # series 10–22 inclusive

WIKI_URL = "https://en.wikipedia.org/wiki/Strictly_Come_Dancing_series_{}"

# ---- Judge panels --------------------------------------------------------
_JUDGES_EARLY = ["craig", "darcey", "len", "bruno"]

JUDGES_BY_SERIES: dict[int, list[str]] = {
    **{s: list(_JUDGES_EARLY) for s in range(10, 16)},
    16: ["craig", "darcey", "shirley", "bruno"],
    17: ["craig", "darcey", "shirley", "motsi", "bruno"],
    18: ["craig", "shirley", "motsi", "bruno"],
    19: ["craig", "shirley", "motsi", "anton", "bruno"],
    **{s: ["craig", "shirley", "motsi", "anton"] for s in range(20, 23)},
}

ALL_JUDGES: list[str] = ["craig", "darcey", "len", "bruno", "shirley", "motsi", "anton"]

# ---- Output schema -------------------------------------------------------
COLUMNS: list[str] = [
    "series",
    "celebrity",
    "professional",
    "week",
    "dance",
    "dance_style",
    "song",
    "artist",
    "total_score",
    *ALL_JUDGES,
    "verdict",
    "celeb_dob",
    "celeb_age",
    "celeb_gender",
    "pro_gender",
    "same_sex",
]

# ---- Paths ---------------------------------------------------------------
_HERE = pathlib.Path(__file__).parent
CACHE_DIR = _HERE / "data"
PEOPLE_CACHE_FILE = _HERE / "data" / "people.json"
OUT_FILE = _HERE / "strictly_scores.csv"

# ---- Series première dates (for age calculation) -------------------------
SERIES_START_DATES: dict[int, date] = {
    10: date(2012, 9, 15),
    11: date(2013, 9, 7),
    12: date(2014, 9, 6),
    13: date(2015, 9, 5),
    14: date(2016, 9, 3),
    15: date(2017, 9, 9),
    16: date(2018, 9, 8),
    17: date(2019, 9, 7),
    18: date(2020, 10, 17),
    19: date(2021, 9, 18),
    20: date(2022, 9, 17),
    21: date(2023, 9, 16),
    22: date(2024, 9, 14),
}

# ---- Network -------------------------------------------------------------
HEADERS = {"User-Agent": "StrictlyDataProject/1.0 (educational; contact via github)"}
DELAY = 1.5

# ---- Wikidata ------------------------------------------------------------
WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"
WIKIDATA_BATCH_SIZE = 50   # slugs per SPARQL request — keeps queries fast
# P21 (sex or gender) Wikidata QIDs we map to M/F.
_GENDER_MAP: dict[str, str] = {"Q6581072": "F", "Q6581097": "M"}

# ---------------------------------------------------------------------------
# Compiled regular expressions
# ---------------------------------------------------------------------------

_FOOTNOTE_RE = re.compile(r"\[.*?]")
_ANNOTATION_RE = re.compile(r"[†‡★☆♦]")
_SONG_SPLIT_RE = re.compile(r"\s*[—–-]\s*")

# ---------------------------------------------------------------------------
# Verdict mapping
# ---------------------------------------------------------------------------

_VERDICT_MAP: dict[str, str] = {
    "safe": "safe",
    "bottom two": "bottom-two",
    "eliminated": "eliminated",
    "withdrew": "eliminated",
    "winners": "winner",
    "runners-up": "runner-up",
    "fourth place": "runner-up",
}

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def fetch_html(series_num: int) -> str:
    """Return cached or freshly-fetched Wikipedia HTML for *series_num*."""
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
    """Strip footnote refs, annotation symbols, and whitespace from *text*."""
    if not text:
        return ""
    text = _FOOTNOTE_RE.sub("", text)
    text = _ANNOTATION_RE.sub("", text)
    return text.strip()


def parse_int(text: str) -> Optional[int]:
    """Return the leading integer in *text*, or ``None``."""
    m = re.match(r"(\d+)", clean(text))
    return int(m.group(1)) if m else None


def split_couple(couple_str: str) -> tuple[str, str]:
    """Split ``"First & First"`` into ``(celebrity_first, pro_first)``."""
    parts = re.split(r"\s*&\s*", clean(couple_str), maxsplit=1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return couple_str.strip(), ""


def _split_song_artist(text: str) -> tuple[str, str]:
    """Split ``"Song – Artist"`` into ``(song, artist)``."""
    m = _SONG_SPLIT_RE.search(text)
    if m:
        song = text[: m.start()].strip().strip('"')
        artist = text[m.end():].strip()
        return song, artist
    return text.strip(), ""


def _make_empty_row(series_num: int, celebrity: str, professional: str, week_num: int) -> dict:
    """Return a row dict with all column defaults (numerics as None)."""
    row: dict = {
        "series": series_num,
        "celebrity": celebrity,
        "professional": professional,
        "week": week_num,
        "dance": "",
        "dance_style": "",
        "song": "",
        "artist": "",
        "total_score": None,
        "verdict": "",
        "celeb_dob": "",
        "celeb_age": None,
        "celeb_gender": "",
        "pro_gender": "",
        "same_sex": False,
    }
    for judge in ALL_JUDGES:
        row[judge] = None
    return row


# ---------------------------------------------------------------------------
# Wikidata person lookup
# ---------------------------------------------------------------------------


def query_wikidata(slugs: list[str]) -> dict[str, tuple[str, str]]:
    """Query Wikidata for DOB and gender for a list of Wikipedia slugs.

    Sends requests in batches of ``WIKIDATA_BATCH_SIZE`` to stay well under
    the Wikidata SPARQL endpoint's complexity/timeout limits.  Each batch uses
    the confirmed-working pattern::

        VALUES ?article { <https://en.wikipedia.org/wiki/Slug> … }
        ?article schema:about ?item .

    Args:
        slugs: Wikipedia article slugs, e.g. ``["Amy_Dowden", "Johannes_Radebe"]``.

    Returns:
        ``{slug: (dob, gender)}`` where *dob* is ``"YYYY-MM-DD"`` or ``""``
        and *gender* is ``"M"``, ``"F"``, or ``""``.
    """
    results: dict[str, tuple[str, str]] = {}

    for batch_start in range(0, len(slugs), WIKIDATA_BATCH_SIZE):
        batch = slugs[batch_start: batch_start + WIKIDATA_BATCH_SIZE]
        values = " ".join(
            f"<https://en.wikipedia.org/wiki/{s}>" for s in batch
        )
        query = textwrap.dedent(f"""
            SELECT ?article ?dob ?genderQid WHERE {{
              VALUES ?article {{ {values} }}
              ?article schema:about ?item .
              OPTIONAL {{ ?item wdt:P569 ?dob . }}
              OPTIONAL {{
                ?item wdt:P21 ?genderNode .
                BIND(STRAFTER(STR(?genderNode), "entity/") AS ?genderQid)
              }}
            }}
        """)
        try:
            resp = requests.get(
                WIKIDATA_SPARQL_URL,
                params={"query": query, "format": "json"},
                headers={**HEADERS, "Accept": "application/sparql-results+json"},
                timeout=30,
            )
            resp.raise_for_status()
            bindings = resp.json()["results"]["bindings"]
        except Exception as exc:
            print(f"  [warn] Wikidata batch {batch_start // WIKIDATA_BATCH_SIZE + 1} failed: {exc}")
            continue

        for b in bindings:
            slug = b["article"]["value"].split("/wiki/")[-1]
            raw_dob = b.get("dob", {}).get("value", "")
            dob = raw_dob.lstrip("+").split("T")[0] if raw_dob else ""
            gender_qid = b.get("genderQid", {}).get("value", "")
            gender = _GENDER_MAP.get(gender_qid, "")
            results[slug] = (dob, gender)

    return results


def _build_person_lookup(
    celeb_slugs: dict[str, str],
    pro_slugs: dict[str, str],
) -> tuple[dict[str, tuple[str, str]], dict[str, tuple[str, str]]]:
    """Return ``{full_name: (dob, gender)}`` dicts for celebs and pros.

    Results are cached to ``data/people.json``.  Delete that file to refresh.
    """
    # Merge all unique slugs into one list for a single batched query.
    all_slug_to_names: dict[str, list[str]] = {}
    for full_name, slug in {**celeb_slugs, **pro_slugs}.items():
        all_slug_to_names.setdefault(slug, []).append(full_name)

    all_slugs = list(all_slug_to_names.keys())

    if PEOPLE_CACHE_FILE.exists():
        print(f"  [cache] people.json ({len(all_slugs)} people)")
        slug_data: dict[str, list] = json.loads(PEOPLE_CACHE_FILE.read_text(encoding="utf-8"))
    else:
        n_batches = (len(all_slugs) + WIKIDATA_BATCH_SIZE - 1) // WIKIDATA_BATCH_SIZE
        print(f"  [wikidata] fetching {len(all_slugs)} people in {n_batches} batch(es)…")
        raw = query_wikidata(all_slugs)
        slug_data = {slug: list(pair) for slug, pair in raw.items()}
        CACHE_DIR.mkdir(exist_ok=True)
        PEOPLE_CACHE_FILE.write_text(
            json.dumps(slug_data, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"  [wikidata] got data for {len(slug_data)}/{len(all_slugs)} people")

    def _to_info(slug_map: dict[str, str]) -> dict[str, tuple[str, str]]:
        info: dict[str, tuple[str, str]] = {}
        for full_name, slug in slug_map.items():
            pair = slug_data.get(slug, ["", ""])
            info[full_name] = (str(pair[0]), str(pair[1]))
        return info

    return _to_info(celeb_slugs), _to_info(pro_slugs)


def _age_at(dob: str, on_date: date) -> Optional[float]:
    """Whole-year age on *on_date* for someone born on *dob* (YYYY-MM-DD)."""
    if not dob:
        return None
    try:
        birth = date.fromisoformat(dob)
        age = on_date.year - birth.year
        if (on_date.month, on_date.day) < (birth.month, birth.day):
            age -= 1
        return float(age)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Couples section parser
# ---------------------------------------------------------------------------


def _parse_couples_section(
    soup: BeautifulSoup,
) -> tuple[dict[str, str], dict[str, str], dict[str, str], dict[str, str]]:
    """Parse the Couples table, returning name maps and Wikipedia slug maps.

    Returns:
        ``(celeb_names, pro_names, celeb_slugs, pro_slugs)`` where:

        - ``celeb_names``  ``{short_key -> full_name}``
        - ``pro_names``    ``{short_key -> full_name}``
        - ``celeb_slugs``  ``{full_name -> wikipedia_slug}``
        - ``pro_slugs``    ``{full_name -> wikipedia_slug}``
    """
    celeb_names: dict[str, str] = {}
    pro_names: dict[str, str] = {}
    celeb_slugs: dict[str, str] = {}
    pro_slugs: dict[str, str] = {}

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

            if not celeb_full or celeb_full.lower() in ("celebrity", "couple"):
                continue

            # Celebrity name key + slug
            words = celeb_full.split()
            celeb_key = f"{words[0]} {words[1]}" if words[0] == "Dr." else words[0]
            celeb_names[celeb_key] = celeb_full
            celeb_link = cells[0].find("a", href=True)
            if celeb_link:
                href = celeb_link["href"]
                if href.startswith("/wiki/") and ":" not in href:
                    celeb_slugs[celeb_full] = href[len("/wiki/"):]

            # Professional name key(s) + slug(s)
            # Build a map of link text -> slug from the pro cell's <a> tags.
            pro_link_map: dict[str, str] = {}
            for a in cells[2].find_all("a", href=True):
                href = a["href"]
                if href.startswith("/wiki/") and ":" not in href:
                    pro_link_map[a.get_text().strip()] = href[len("/wiki/"):]

            for segment in re.split(r"\s*\([^)]*\)\s*", pro_raw):
                pro_full = segment.strip()
                if not pro_full:
                    continue
                pro_key = pro_full.split()[0]
                pro_names[pro_key] = pro_full
                if pro_full in pro_link_map:
                    pro_slugs[pro_full] = pro_link_map[pro_full]

        break

    return celeb_names, pro_names, celeb_slugs, pro_slugs


# ---------------------------------------------------------------------------
# Per-series and per-week parsers
# ---------------------------------------------------------------------------


def parse_series(
    series_num: int, html: str
) -> tuple[list[dict], dict[str, str], dict[str, str]]:
    """Parse scores from a series page; also return celeb/pro slug maps."""
    soup = BeautifulSoup(html, "html.parser")
    judges = JUDGES_BY_SERIES.get(series_num, list(_JUDGES_EARLY))
    celeb_names, pro_names, celeb_slugs, pro_slugs = _parse_couples_section(soup)

    week_sections: dict[int, Any] = {}
    for tag in soup.find_all(["h2", "h3", "h4"]):
        m = re.match(r"Week\s+(\d+)", clean(tag.get_text()), re.I)
        if m:
            sibling = tag.find_next("table")
            if sibling:
                week_sections[int(m.group(1))] = sibling

    if not week_sections:
        print(f"  [warn] series {series_num}: no week sections found, trying fallback")
        rows = parse_series_summary_only(series_num, soup)
        return rows, celeb_slugs, pro_slugs

    rows = [
        row
        for week_num, table in sorted(week_sections.items())
        for row in parse_week_table(
            series_num, week_num, table, judges, celeb_names, pro_names
        )
    ]
    return rows, celeb_slugs, pro_slugs


def parse_week_table(
    series_num: int,
    week_num: int,
    table,
    judges: list[str],
    celeb_names: dict[str, str],
    pro_names: dict[str, str],
) -> list[dict]:
    """Parse one week's scoring table into row dicts."""
    rows: list[dict] = []
    last_celebrity = ""
    last_professional = ""
    last_verdict = ""

    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 2:
            continue

        texts = [clean(c.get_text()) for c in cells]

        if texts[0].lower() in ("couple", ""):
            continue

        if "&" in texts[0]:
            celeb_first, pro_first = split_couple(texts[0])
            last_celebrity = celeb_names.get(celeb_first, celeb_first)
            last_professional = pro_names.get(pro_first, pro_first)
            last_verdict = ""
            score_text = texts[1]
            dance = texts[2] if len(texts) > 2 else ""
            song_text = texts[3] if len(texts) > 3 else ""
            mapped = _VERDICT_MAP.get(texts[-1].lower())
            if mapped is not None:
                last_verdict = mapped

        elif parse_int(texts[0]) is not None and last_celebrity:
            score_text = texts[0]
            dance = texts[1] if len(texts) > 1 else ""
            song_text = texts[2] if len(texts) > 2 else ""

        else:
            continue

        total = parse_int(score_text)
        if total is None:
            continue

        judge_scores: dict[str, Optional[int]] = {}
        paren = re.search(r"\(([^)]+)\)", score_text)
        if paren:
            score_parts = [p.strip() for p in paren.group(1).split(",")]
            for i, judge in enumerate(judges):
                if i < len(score_parts):
                    judge_scores[judge] = parse_int(score_parts[i])

        song, artist = _split_song_artist(song_text)

        row = _make_empty_row(series_num, last_celebrity, last_professional, week_num)
        row.update({
            "dance": dance,
            "song": song,
            "artist": artist,
            "total_score": total,
            "verdict": last_verdict,
            **{judge: judge_scores.get(judge) for judge in ALL_JUDGES},
        })
        rows.append(row)

    return rows


def parse_series_summary_only(series_num: int, soup: BeautifulSoup) -> list[dict]:
    """Fallback: parse the summary scoring chart when no weekly sections exist."""
    print(f"  [fallback] series {series_num}: using summary scoring chart")
    rows: list[dict] = []

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

        break

    return rows


# ---------------------------------------------------------------------------
# Dance style classification
# ---------------------------------------------------------------------------

_LATIN_DANCES: frozenset[str] = frozenset([
    "Cha-Cha-Cha", "Jive", "Paso Doble", "Rumba", "Samba", "Salsa", "Argentine Tango",
])

_BALLROOM_DANCES: frozenset[str] = frozenset([
    "Waltz", "Viennese Waltz", "Foxtrot", "Quickstep", "Tango", "American Smooth",
])


def _classify_dance_style(dance: str) -> str:
    """Return ``"Latin"``, ``"Ballroom"``, or ``"Other"``."""
    if not dance:
        return "Other"
    if "marathon" in dance.lower():
        return "Other"
    base = dance.split("&")[0].strip()
    if base in _LATIN_DANCES:
        return "Latin"
    if base in _BALLROOM_DANCES:
        return "Ballroom"
    return "Other"


# ---------------------------------------------------------------------------
# DataFrame post-processing
# ---------------------------------------------------------------------------


def _enrich_dataframe(
    df: pd.DataFrame,
    celeb_info: dict[str, tuple[str, str]],
    pro_info: dict[str, tuple[str, str]],
) -> pd.DataFrame:
    """Fill celeb_dob, celeb_age, celeb_gender, pro_gender, same_sex columns."""
    # Pre-compute per unique (celebrity, series) to avoid repeated work.
    for idx, row in df.iterrows():
        celeb = row["celebrity"]
        pro = row["professional"]
        series_num = int(row["series"])

        dob, cgender = celeb_info.get(celeb, ("", ""))
        _, pgender = pro_info.get(pro, ("", ""))
        start = SERIES_START_DATES.get(series_num)

        df.at[idx, "celeb_dob"] = dob
        df.at[idx, "celeb_age"] = _age_at(dob, start) if start else None
        df.at[idx, "celeb_gender"] = cgender
        df.at[idx, "pro_gender"] = pgender
        df.at[idx, "same_sex"] = bool(cgender and pgender and cgender == pgender)

    return df


def _build_dataframe(
    all_rows: list[dict],
    celeb_info: dict[str, tuple[str, str]],
    pro_info: dict[str, tuple[str, str]],
) -> pd.DataFrame:
    """Build, type-cast, and enrich the scores DataFrame."""
    df = pd.DataFrame(all_rows, columns=COLUMNS)

    df["series"] = df["series"].astype(int)
    df["week"] = df["week"].astype(int)
    df["total_score"] = pd.to_numeric(df["total_score"], errors="coerce")
    for judge in ALL_JUDGES:
        df[judge] = pd.to_numeric(df[judge], errors="coerce")

    df["dance"] = df["dance"].str.title()
    df["dance_style"] = df["dance"].apply(_classify_dance_style)
    df["celeb_age"] = pd.to_numeric(df["celeb_age"], errors="coerce")

    df = _enrich_dataframe(df, celeb_info, pro_info)

    return df


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Scrape all configured series and write the combined CSV."""
    all_rows: list[dict] = []
    all_celeb_slugs: dict[str, str] = {}
    all_pro_slugs: dict[str, str] = {}

    for series_num in SERIES:
        print(f"\nSeries {series_num}:")
        try:
            html = fetch_html(series_num)
            rows, celeb_slugs, pro_slugs = parse_series(series_num, html)
            print(f"  {len(rows)} rows parsed")
            all_rows.extend(rows)
            all_celeb_slugs.update(celeb_slugs)
            all_pro_slugs.update(pro_slugs)
        except Exception as e:
            print(f"  [error] series {series_num}: {e}")
            traceback.print_exc()

    if not all_rows:
        print("\nNo data scraped — check errors above.")
        return

    print("\nLooking up person data:")
    celeb_info, pro_info = _build_person_lookup(all_celeb_slugs, all_pro_slugs)

    df = _build_dataframe(all_rows, celeb_info, pro_info)
    df.to_csv(OUT_FILE, index=False)

    print(f"\nDone — {len(df)} rows written to {OUT_FILE}")
    print(f"\nSeries coverage:\n{df.groupby('series')['celebrity'].nunique().to_string()}")
    # Show a few enriched columns to confirm they populated.
    sample_cols = ["series", "celebrity", "professional", "celeb_age", "celeb_gender", "pro_gender", "same_sex"]
    print(f"\nPerson data sample:\n{df[sample_cols].drop_duplicates('celebrity').head(10).to_string()}")


if __name__ == "__main__":
    main()
