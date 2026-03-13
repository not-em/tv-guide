# reality-data

Historical data scraped from Wikipedia for British reality and competition TV shows.

Built for analysis and visualisation.

---

## shows

### strictly/
Strictly Come Dancing series 10–22 (2012–2024).

**Columns:** `series`, `celebrity`, `professional`, `week`, `dance`, `song`, `artist`, `total_score`, `craig`, `darcey`, `len`, `bruno`, `shirley`, `motsi`, `anton`, `verdict`.

Judge columns are `NaN` when that judge wasn't on the panel that series.

Pre-2012 data (series 1–9) can be added manually — the format changed enough that automated scraping isn't reliable for those years.

---

## setup

```bash
pip install -r requirements.txt
```

## scraping

```bash
cd strictly
python scraper.py
```

Raw HTML is cached in `strictly/data/` (gitignored) so Wikipedia isn't re-fetched on repeat runs. Delete a cache file to force a refresh for that series.

---

## data sources

Wikipedia pages for each Strictly Come Dancing series, e.g.:
- https://en.wikipedia.org/wiki/Strictly_Come_Dancing_series_10

Data is used for personal analysis and visualisation only.

---

## attribution

Data sourced from Wikipedia, available under [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/).
