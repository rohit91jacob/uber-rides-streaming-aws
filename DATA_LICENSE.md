# Data licence and attribution

The code in this repository is MIT licensed ([LICENSE](LICENSE)). The data it processes is not
ours. This file lists every source, its terms, and exactly what is committed.

## Sources

| Source | Publisher | Terms | Attribution |
|---|---|---|---|
| [High Volume For-Hire Vehicle trip records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) (`fhvhv_tripdata_YYYY-MM.parquet`) and the [taxi zone lookup](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv) | NYC Taxi & Limousine Commission (TLC) | Published as NYC Open Data under the [NYC Open Data Terms of Use](https://opendata.cityofnewyork.us/overview/#termsofuse): free to use and redistribute, provided "as is", no endorsement implied | "Contains data from the NYC Taxi & Limousine Commission (TLC)." |
| [TLC monthly data reports](https://www.nyc.gov/site/tlc/about/aggregated-reports.page) (`data_reports_monthly.csv`) | NYC TLC | Same NYC Open Data terms | as above |
| [FHV Base Aggregate Report](https://data.cityofnewyork.us/Transportation/FHV-Base-Aggregate-Report/2v9c-2k7f) (Socrata `2v9c-2k7f`) | NYC TLC via NYC Open Data | Same NYC Open Data terms | as above |
| [Open-Meteo historical weather API](https://open-meteo.com/en/docs/historical-weather-api) (hourly, central NYC) | Open-Meteo; underlying reanalysis by ECMWF / Copernicus (ERA5) | [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/); free non-commercial API use, no key | "Weather data by [Open-Meteo.com](https://open-meteo.com/) (CC BY 4.0), based on ERA5 from the Copernicus Climate Change Service." |

The TLC states that it cannot guarantee the accuracy of the records submitted by the bases.
"Uber" and "Lyft" are trademarks of their owners; this project has no affiliation with Uber,
Lyft, the TLC or Open-Meteo. The licence numbers `HV0003` (Uber) and `HV0005` (Lyft) are the
TLC's own identifiers.

## What is committed

All committed files are unmodified extracts of the sources above, each under 300 KB, built by
[`scripts/build_fixtures.py`](scripts/build_fixtures.py):

| File | Content | Size |
|---|---|---|
| `tests/fixtures/tlc/fhvhv_tripdata_2026-06.parquet` | 3,203 real June 2026 rows (all 25 columns): every Uber trip requested 2026-06-12 18:00-18:02 plus every 10,000th Uber and Lyft row of the file | ~123 KB |
| `tests/fixtures/tlc/taxi_zone_lookup.csv` | TLC zone lookup, unchanged | 12 KB |
| `tests/fixtures/weather/open_meteo_hourly_2026-06.json` | Open-Meteo hourly response for June 2026 | ~41 KB |
| `tests/fixtures/published/data_reports_monthly.csv` | The `FHV - High Volume` rows of the TLC monthly report, 2025-06 onwards | ~2 KB |
| `tests/fixtures/published/fhv_base_aggregate_2026-0{5,6}.json` | The `UBER` and `LYFT` rows of the base aggregate report (June was not yet published, so that file is an empty list) | < 1 KB |

Full monthly files (about 500 MB each) and everything the pipeline derives from them are
**not committed**; `uber-rides ingest` downloads them at runtime.

## Simulated data

Nothing in the trip, zone, weather or report data is simulated. The only simulated parts are
labelled as such in code, docs and the results site:

* the *delivery* of ride events as a live feed (the replay producer sends real, timestamped
  events at a chosen speed), and
* the fault-injection drills: duplicated records and corrupted records injected by the producer
  with a fixed seed (`--duplicate-every`, `--malformed-every`) to exercise dedupe and the DLQ.
