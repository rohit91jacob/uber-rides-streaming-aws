# Verification against independently published figures

`uber-rides verify --month YYYY-MM` compares figures computed from the raw TLC trip file with
two reports the TLC publishes separately, and writes the result to
`<data>/ops/verification/YYYY-MM.json` (also rendered on the results site). Nothing is adjusted
to make numbers agree. Statuses: **match** (within tolerance), **close** (within 3x
tolerance), **mismatch**, **not published**.

Sources:

* **FHV Base Aggregate Report**, NYC Open Data [`2v9c-2k7f`](https://data.cityofnewyork.us/Transportation/FHV-Base-Aggregate-Report/2v9c-2k7f):
  dispatched trips and shared trips per base per month. Uber and Lyft appear as one row each
  (`base_license_number` = `UBER` / `LYFT`). Published about two months after the month ends,
  one month after the trip file.
* **TLC monthly data reports** ([aggregated reports page](https://www.nyc.gov/site/tlc/about/aggregated-reports.page),
  file `data_reports_monthly.csv`): one row per month and licence class. The `FHV - High
  Volume` class is every HVFHS licensee together (Uber + Lyft), so it is compared with all
  licensees in the raw file. Values are rounded (trips per day to a whole trip, minutes per trip
  to a whole minute).

## Results (run on 2026-10-09)

### May 2026 (`fhvhv_tripdata_2026-05.parquet`, profiled only; not loaded into the lake)

| Check | Ours (raw file) | Published | Diff | Status |
|---|---:|---:|---:|---|
| Uber dispatched trips (HV0003 rows vs base `UBER`) | 15,354,816 | 15,354,816 | 0.00% | match |
| Uber shared trips (`shared_match_flag = 'Y'`) | 220,904 | 220,904 | 0.00% | match |
| Uber shared *requests* (`shared_request_flag = 'Y'`) | 388,750 | 220,904 | +75.98% | mismatch (expected: other definition) |
| Lyft dispatched trips (HV0005 rows vs base `LYFT`) | 6,770,928 | 6,770,928 | 0.00% | match |
| Lyft shared trips (`shared_match_flag = 'Y'`) | 1,245 | 2,022 | -38.43% | **mismatch** |
| Lyft shared *requests* (`shared_request_flag = 'Y'`) | 2,022 | 2,022 | 0.00% | match |
| HVFHS trips per day (all licensees / 31) | 713,733.7 | 713,730 | +0.001% | match |
| HVFHS shared trips per day | 7,166.1 | 7,191 | -0.35% | match |
| HVFHS average minutes per trip | 20.50 | 20 | +2.5% | match (report rounds to whole minutes) |

### June 2026 (the month loaded end to end)

| Check | Ours | Published | Diff | Status |
|---|---:|---:|---:|---|
| Uber / Lyft dispatched and shared trips | 15,257,740 / 256,422 (Uber); 5,518,128 / 1,482 (Lyft) | not yet in the base aggregate report | - | not published |
| HVFHS trips per day (all licensees / 30) | 692,528.9 | 707,670 | **-2.14%** | close |
| HVFHS shared trips per day | 8,596.8 | 8,619 | -0.26% | match |
| HVFHS average minutes per trip | 20.47 | 20 | +2.35% | match (rounding) |

Internal reconciliation for June 2026 (lake vs raw file): Iceberg `trips` + `trips_quarantine`
= 15,257,740 = HV0003 rows in the raw file, and dbt `int_trips_enriched` holds the same
15,257,740 rows. All additive marts sum back to that number (`marts` gate and the dbt singular
test `assert_marts_reconcile_to_trips`).

## What the mismatches mean

* **Shared trips: the two companies report different things.** Each base's
  `total_dispatched_shared_trips` is compared with both trip-file flags. For Uber it equals the
  rows with `shared_match_flag = 'Y'` exactly (220,904); for Lyft it equals the rows with
  `shared_request_flag = 'Y'` exactly (2,022), while Lyft's matched rows are 1,245. So the
  base aggregate counts matched shared rides for Uber and shared *requests* for Lyft. The
  monthly report's "Trips Per Day Shared" (7,191 for May) is the sum of those two base figures
  divided by 31 ((220,904 + 2,022) / 31 = 7,191.2), which is why our all-matched figure
  (7,166.1) is 0.35% lower. Both mismatching rows are kept in the output because they are the
  evidence for this finding. The marts use `shared_match_flag` for "matched".
* **June HVFHS trips per day (-2.14%).** For May the monthly report agrees with the trip file
  to within 4 trips over the whole month (22,125,744 vs 713,730 x 31 = 22,125,630), but for June
  the report implies 21,230,100 trips while the published June file has 20,775,868 (454,232
  fewer). Every one of the 30 days is present in the file and no day is anomalously low, so the
  gap is not an obvious missing day. Possible causes: the monthly report was computed from a
  later or earlier submission than the file on the CDN, or it counts trips the trip file does
  not carry. We cannot tell which from the public data. The June base aggregate (expected
  around November 2026) will show whether the Uber/Lyft totals agree with the file, as they did
  exactly for May; the monthly `refresh` workflow re-runs this verification automatically.

## Profiled facts the pipeline reproduces

These are computed by the pipeline from the June 2026 file and match an independent DuckDB
profile of the same file taken before the pipeline was written:

| Fact | Value |
|---|---:|
| HV0003 (Uber) trips / HV0005 (Lyft) trips | 15,257,740 / 5,518,128 |
| Uber avg trip miles / avg base fare / avg driver pay | 5.08 / $29.40 / $22.60 |
| Uber trips with zone id > 263 (unknown zones 264/265) | 724,322 |
| Uber fares <= 0 | 7,920 |
| Uber shared-ride requests / matched | 412,276 / 256,422 |
| Uber trips charged the CBD congestion fee | 4,912,000 (fee $1.50 each, $7,368,000 total) |
| `request_datetime` / `on_scene_datetime` nulls | 0 / 0 |
