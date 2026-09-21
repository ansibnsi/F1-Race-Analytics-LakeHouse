# F1 Race Analytics Lakehouse

A Microsoft Fabric Lakehouse project that ingests, transforms, and models
Formula 1 racing data (races, drivers, constructors, results) through a
**Bronze → Silver → Gold (medallion)** architecture, with automated data
quality checks and a Power BI semantic layer on top.

---

## Stack

| Layer | Tool |
|---|---|
| Ingestion & orchestration | Microsoft Fabric **Data Factory** (pipelines) + **Notebooks** |
| Transformation | **PySpark** / **Spark SQL** |
| Storage | **Delta tables** in a Fabric **Lakehouse**, partitioned for query performance |
| Data quality | PySpark-based completeness/consistency/duplicate checks, run as part of the pipeline |
| Analytics & reporting | **Power BI** dashboards on the Gold layer |

## Architecture

```
        ┌───────────────┐      ┌────────────────────┐      ┌───────────────────────────┐
        │ Source files   │ Data │  Lakehouse          │      │  PySpark Notebooks         │
        │ (CSV: races,   ├─────►│  Files/raw/         ├─────►│  Bronze → Silver → Gold     │
        │ drivers, etc.) │Factory│ (landed as-is)      │Jobs  │  + Data Quality checks      │
        └───────────────┘      └────────────────────┘      └────────────┬────────────────┘
                                                                          │
                                                                          ▼
                                                            ┌───────────────────────────┐
                                                            │  Lakehouse Tables/          │
                                                            │  gold_* Delta tables        │
                                                            └────────────┬────────────────┘
                                                                         │
                                                                         ▼
                                                            ┌───────────────────────────┐
                                                            │  Power BI dashboards        │
                                                            │  (race/driver/constructor    │
                                                            │   performance)               │
                                                            └───────────────────────────┘
```

See [`architecture/architecture.md`](architecture/architecture.md) for the
full diagram, medallion layer definitions, and partitioning strategy.

## What's in this repo

```
f1-race-analytics-lakehouse/
├── README.md
├── architecture/
│   └── architecture.md           <- diagram, layer definitions, partitioning & design notes
├── data/
│   ├── races.csv                  <- sample source data to test the pipeline end-to-end
│   ├── drivers.csv
│   ├── constructors.csv
│   └── results.csv
├── notebooks/
│   ├── 01_bronze_ingestion.py      <- raw files -> Bronze Delta tables
│   ├── 02_silver_transformation.py  <- clean/dedupe/type-cast/join -> Silver
│   ├── 03_gold_aggregation.py        <- driver/constructor/race performance marts -> Gold
│   └── 04_data_quality_checks.py      <- completeness, consistency, duplicate checks
├── fabric/
│   └── pipeline_definition.md          <- Data Factory pipeline design (activities, dependencies, schedule)
├── powerbi/
│   └── dashboard_notes.md               <- report pages, visuals, key DAX measures
├── docs/
│   └── data_quality_checks.md            <- what's checked, thresholds, and what happens on failure
├── requirements.txt
├── .gitignore
└── LICENSE
```

## The data

Small sample CSVs modeling a simplified version of the well-known
["Ergast" F1 dataset](http://ergast.com/mrd/) shape — `races`, `drivers`,
`constructors`, and `results` — enough rows to exercise every
transformation and quality check without needing a real Fabric capacity to
try the notebooks' logic locally (e.g. with `pyspark` installed standalone).

| File | Grain | Key columns |
|---|---|---|
| `races.csv` | one row per race | `race_id`, `season`, `round`, `race_name`, `circuit`, `race_date` |
| `drivers.csv` | one row per driver | `driver_id`, `driver_name`, `nationality`, `date_of_birth` |
| `constructors.csv` | one row per team | `constructor_id`, `constructor_name`, `nationality` |
| `results.csv` | one row per driver per race | `race_id`, `driver_id`, `constructor_id`, `grid`, `position`, `points`, `status` |

## Medallion layers

| Layer | What happens |
|---|---|
| **Bronze** | Raw CSVs landed as Delta tables as-is, with ingestion metadata (`_ingested_at`, `_source_file`, `_batch_id`). Append-only, no business logic. |
| **Silver** | Cleaned/typed/deduplicated tables per entity (`silver_races`, `silver_drivers`, `silver_constructors`, `silver_results`), with results joined to enrich with driver/constructor/race names. |
| **Gold** | Business-facing aggregates: `gold_driver_standings`, `gold_constructor_standings`, `gold_race_summary` — shaped for direct Power BI consumption. |

## Quickstart (running the notebooks in Fabric)

1. Create a **Lakehouse** in your Fabric workspace.
2. Upload the CSVs from `data/` into the Lakehouse's `Files/raw/` folder
   (or point Data Factory's Copy activity at your real source system —
   see [`fabric/pipeline_definition.md`](fabric/pipeline_definition.md)).
3. Import the four notebooks from `notebooks/` into your workspace and
   attach them to the Lakehouse.
4. Run `01_bronze_ingestion` → `02_silver_transformation` →
   `03_gold_aggregation` → `04_data_quality_checks`, in that order (or
   orchestrate them as pipeline activities — see `fabric/`).
5. Connect Power BI (Direct Lake or Import mode) to the `gold_*` tables
   and build/import the report described in
   [`powerbi/dashboard_notes.md`](powerbi/dashboard_notes.md).

## Quickstart (testing the transformation logic locally)

The notebooks are plain PySpark, so the transformation logic can be
exercised outside Fabric with a local Spark session — useful for fast
iteration before deploying to a workspace:

```bash
pip install -r requirements.txt
python notebooks/01_bronze_ingestion.py --local --data-dir data --output-dir ./lakehouse_local
python notebooks/02_silver_transformation.py --local --output-dir ./lakehouse_local
python notebooks/03_gold_aggregation.py --local --output-dir ./lakehouse_local
python notebooks/04_data_quality_checks.py --local --output-dir ./lakehouse_local
```
Each notebook detects the `--local` flag and reads/writes Delta tables
under `./lakehouse_local` instead of a Fabric Lakehouse's `Files`/`Tables`
paths — the transformation code itself is identical either way.

## Data quality checks

Implemented in `notebooks/04_data_quality_checks.py` and documented in
[`docs/data_quality_checks.md`](docs/data_quality_checks.md):

- **Completeness** — required fields (`driver_id`, `race_id`,
  `constructor_id`, `points`) are non-null above a configurable threshold
- **Consistency** — foreign keys in `results` resolve to real rows in
  `drivers` / `constructors` / `races`; `position`/`points` are within
  plausible ranges
- **Duplicates** — no duplicate `(race_id, driver_id)` pairs in `results`

Failing checks are logged with row-level detail and can optionally fail
the pipeline run (see the notebook's `fail_on_error` parameter).

## Extending this project

- Swap the sample CSVs for a real feed (e.g. the Ergast/Jolpica F1 API)
  via a Data Factory Copy activity.
- Add incremental loading (watermark on `race_date`) instead of full
  reloads once the dataset is large.
- Add lap-time / telemetry data as a new Bronze source and a
  `gold_lap_performance` mart.
- Add Great Expectations or Fabric's built-in data quality tooling as an
  alternative to the hand-rolled PySpark checks here.

## License

MIT — see [LICENSE](LICENSE).
