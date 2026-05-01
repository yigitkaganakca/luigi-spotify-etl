# Luigi (Spotify) — Top Tracks ETL Pipeline

> Individual Tool Presentation — YZV 322E *Applied Data Engineering* (Spring 2026)
> Tool: **Luigi** by Spotify · Category: *Airflow / Orchestration*

A Dockerised batch ETL pipeline orchestrated by **[Luigi](https://luigi.readthedocs.io/)** that fetches up to ~1,000 tracks from the public **iTunes Search API** (one parallel `FetchArtistTracks` task per artist), cleans and enriches the merged dataset with Pandas, loads the result into **PostgreSQL**, and indexes it into **Elasticsearch** for exploration in **Kibana**. The whole stack is brought up by a single `docker compose up`.

The project deliberately exercises **five of the seven core course tools** (Docker, PostgreSQL, pgAdmin, Elasticsearch, Kibana) so the grader can see Luigi's role inside a realistic data-engineering pipeline. The data source — iTunes Search — was chosen specifically because it requires **no API key, no signup, and no rate-limit headaches**, so anyone running the demo reproduces it end-to-end.

---

## 1. What is this tool?

Luigi is a Python framework, originally built at Spotify (2012, Apache 2.0), for stitching long-running batch jobs into a directed acyclic graph (DAG) of `Task` objects. Each task declares its inputs (`requires`), where it writes its output (`output`), and what it does (`run`). Luigi tracks task completion through **file-based targets**, gives you idempotent re-runs, automatic retries, parameterised tasks, and a built-in scheduler with a web UI. It is the spiritual ancestor of Airflow.

## 2. Prerequisites

| Software | Tested version | Notes |
|---|---|---|
| **Windows 10/11** | 10.0.26200 | Verified on this stack |
| macOS / Linux | macOS 14+ on Apple Silicon | Also works |
| Docker Desktop | 4.30+ | On Windows: enable the **WSL2 backend** (default). Verify: `docker --version`, `docker compose version` |
| RAM allocated to Docker | **>= 4 GB** | Docker Desktop -> Settings -> Resources. Elasticsearch alone wants ~1 GB heap |
| Free disk | ~3 GB for the images | One-time pull |
| PowerShell 5.1+ (Windows) | preinstalled | for the `./tasks.ps1` runner |
| GNU Make (optional) | any | only if you prefer `make` over `./tasks.ps1` (Git Bash + `choco install make` works) |
| Python 3.11 (host) | optional | only for IDE support outside Docker |

You **do not need** to install Luigi or any Python package on the host to grade this project — everything runs inside containers.

## 3. Installation

The repo ships with both a **PowerShell runner** (`tasks.ps1`, default for Windows) and a **Makefile** (default for macOS/Linux/Git Bash). Pick whichever your shell prefers — they expose the same verbs.

### Windows (PowerShell)

```powershell
# 1. Clone
git clone <THIS-REPO-URL> luigi-spotify-etl
cd luigi-spotify-etl

# 2. (one-time) PowerShell may need execution-policy relaxed for the .ps1 runner
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

# 3. Build the Luigi worker image and start the stack
#    tasks.ps1 creates the external 'luigi' docker network on first use.
./tasks.ps1 build
./tasks.ps1 up
```

### macOS / Linux / Git Bash

```bash
git clone <THIS-REPO-URL> luigi-spotify-etl
cd luigi-spotify-etl
docker network inspect luigi >/dev/null 2>&1 || docker network create luigi
make build
make up
```

`up` boots six containers and prints the URLs:

| Service | URL | Credentials |
|---|---|---|
| Luigi scheduler UI | http://localhost:8082 | — |
| pgAdmin | http://localhost:5050 | `admin@example.com` / `admin` |
| Elasticsearch | http://localhost:9200 | — |
| Kibana | http://localhost:5601 | — |

Wait ~30 seconds for Elasticsearch and Kibana to become healthy, then check status:

```powershell
./tasks.ps1 ps        # Windows
make ps               # Bash
```

All six rows should show `Up` and the stateful ones should be `(healthy)`.

## 4. Running the example

One command runs the entire DAG (parallel fan-out to 8 workers):

```powershell
./tasks.ps1 run       # Windows
make run              # Bash
```

Behind the scenes this is:

```bash
docker compose exec luigi-worker \
  python -m luigi --module pipeline.tasks RunAll --scheduler-host luigid --workers 8
```

Open the **Luigi UI at <http://localhost:8082>** while it runs — you will see one `FetchArtistTracks` task per seed artist running in parallel, then `MergeArtistTracks -> CleanData -> EnrichData -> LoadToPostgres / IndexToElasticsearch` light up green as the fan-in completes.

First run takes ~20–30 seconds (one HTTP call per artist with a half-second throttle); every subsequent run on the same UTC day is **instant** because Luigi sees the per-artist JSON targets already exist.

To force a clean re-run (useful for the live demo):

```powershell
./tasks.ps1 rerun     # Windows
make rerun            # Bash
```

## 5. Set up the Kibana dashboard (one-time, after first `run`)

The repo does **not** ship a saved-objects export, so the dashboard is built once by hand against the live index. Total time: ~3 minutes.

```powershell
./tasks.ps1 kibana-setup     # creates the 'Spotify Top Tracks' data view
```

Then in the browser:

1. Open <http://localhost:5601>.
2. **Stack Management -> Data Views** — confirm `Spotify Top Tracks` exists (re-run `kibana-setup` if not).
3. **Dashboard -> Create dashboard -> Create visualization**. Build these four panels using the `Spotify Top Tracks` data view:

   | Panel | Type | Config |
   |---|---|---|
   | Top 10 artists | Bar (vertical) | Y = `Count`, X = `artist.keyword`, top 10 desc |
   | Genre distribution | Pie | Slice by `genre.keyword`, top 10 |
   | Tracks by decade | Bar | Y = `Count`, X = `decade`, sort ascending |
   | Avg duration by decade | Bar | Y = `Average duration_min`, X = `decade` |

4. **Save** the dashboard as **`Spotify Top Tracks`**.
5. Take a screenshot into `docs/screenshots/kibana-dashboard.png` for the slide deck.

## 6. Expected output

After `run`, you should see (excerpt — exact counts vary slightly because iTunes results drift):

```
INFO: luigi-interface  FetchArtistTracks(The Beatles): 50 results
INFO: luigi-interface  FetchArtistTracks(Pink Floyd): 50 results
... (one line per seed artist, run in parallel) ...
INFO: luigi-interface  MergeArtistTracks: 873 rows merged from 20 artist files
INFO: luigi-interface  CleanData: 873 -> 851 rows
INFO: luigi-interface  EnrichData: 851 rows, 13 columns
INFO: luigi-interface  LoadToPostgres: 851 rows in top_tracks
INFO: luigi-interface  IndexToElasticsearch: 851 ok, 0 errors
===== Luigi Execution Summary =====
Scheduled 25 tasks of which:
* 25 ran successfully:
This progress looks :) because there were no failed tasks or missing dependencies
```

Verify the data landed in both stores:

```powershell
./tasks.ps1 pg-count     # Windows
./tasks.ps1 es-count
make pg-count            # Bash
make es-count
```

Both should report the same row count (somewhere between ~700 and ~1000).

Open **pgAdmin** (<http://localhost:5050>) -> connect to host `postgres`, user `luigi`, password `luigi`, db `charts` -> run:

```sql
SELECT artist, COUNT(*) AS tracks, ROUND(AVG(duration_min)::numeric, 2) AS avg_min
FROM top_tracks
GROUP BY artist
ORDER BY tracks DESC, avg_min DESC;
```

Open **Kibana** (<http://localhost:5601>) -> *Discover* or the **`Spotify Top Tracks`** dashboard you built in section 5. Useful filters: `decade: 1980`, `genre: "Rock"`, `length_category: long`.

Reference screenshots live in [`docs/screenshots/`](./docs/screenshots/).

## 7. Repository layout

```
luigi-spotify-etl/
|-- docker-compose.yml          # 6 services on the `luigi` network
|-- Dockerfile                  # python:3.11-slim + requirements.txt
|-- luigi.cfg                   # tells workers where the scheduler is
|-- Makefile                    # macOS/Linux/Git Bash entry points
|-- tasks.ps1                   # Windows PowerShell entry points
|-- requirements.txt
|-- pipeline/
|   |-- __init__.py
|   |-- config.py               # Pydantic v2 settings
|   `-- tasks.py                # the Luigi DAG (5 tasks + 1 wrapper)
|-- data/
|   |-- seed/artists.txt        # one artist per line; one parallel task each
|   `-- output/                 # Luigi LocalTarget files (gitignored)
|       `-- <run-date>/
|           |-- raw/<artist>.json    # cached iTunes API responses
|           |-- raw.csv              # MergeArtistTracks output
|           |-- clean.csv
|           |-- enriched.csv
|           |-- _loaded_postgres.marker
|           `-- _indexed_es.marker
|-- kibana/                     # placeholder for future saved-objects export
|-- docs/screenshots/           # demo screenshots for the slide deck
|-- AI_USAGE.md                 # required AI disclosure
|-- .env.example
`-- README.md
```

## 8. The DAG, at a glance

```
   FetchArtistTracks(Beatles)  FetchArtistTracks(Queen)  ...  FetchArtistTracks(Olivia Rodrigo)
              |                          |                                |
              `-------------+------------'------------- ... ---------------'
                            v
                   +--------------------+
                   | MergeArtistTracks  |   raw.csv
                   +---------+----------+
                             v
                   +--------------------+
                   |     CleanData      |   clean.csv
                   +---------+----------+
                             v
                   +--------------------+
                   |     EnrichData     |   enriched.csv
                   +---------+----------+
                             v
                +------------+------------+
                v                         v
     +----------------------+  +----------------------+
     |   LoadToPostgres     |  | IndexToElasticsearch |
     |  (top_tracks table)  |  | (spotify_top_tracks) |
     +----------+-----------+  +----------+-----------+
                 \                       /
                  \     +----------+    /
                   `--->|  RunAll  |<--'
                        +----------+
```

The DAG is a true **fan-out / fan-in** graph, not a straight chain. Every non-wrapper task is also parameterised by `run_date`:

- Re-running on the same UTC day is a no-op (Luigi sees every target file already exists — idempotency).
- Re-running on a different `--run-date` produces a fresh per-date directory under `data/output/`.
- The cached per-artist JSON means subsequent runs do **not** re-hit the iTunes API.

## 9. Connection to the course

| Course tool | Where it shows up here |
|---|---|
| **Docker** + Compose | The whole stack is one command |
| **PostgreSQL** | `LoadToPostgres` writes the enriched dataframe to a `top_tracks` table |
| **pgAdmin** | The same table is browsable at `localhost:5050` |
| **Elasticsearch** | `IndexToElasticsearch` bulk-indexes documents with a typed mapping |
| **Kibana** | A data view + dashboard built on top of that index |
| **Apache Airflow** | Discussed on slide 4 — Luigi is the older, file-target-based cousin of Airflow |

## 10. Tearing it down

```powershell
./tasks.ps1 down      # Windows: stop containers, keep volumes
./tasks.ps1 clean     # Windows: stop AND wipe volumes + data/output
make down             # Bash equivalents
make clean
```

## 11. Troubleshooting

| Symptom | Fix |
|---|---|
| `network luigi declared as external, but could not be found` | `docker network create luigi` (the runner does this for you on first use) |
| `pull access denied for luigi-spotify-etl` | Run `./tasks.ps1 build` (or `docker compose build`) first |
| `tasks.ps1 cannot be loaded because running scripts is disabled` | `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` in that PowerShell window |
| ES container restarting / OOM | Bump Docker Desktop RAM to >=4 GB |
| Kibana shows "no data view" | Run `./tasks.ps1 kibana-setup` |
| Line endings break the Dockerfile after `git clone` on Windows | `git config --global core.autocrlf input`, then re-clone |

## 12. AI usage disclosure

See [`AI_USAGE.md`](./AI_USAGE.md).

## 13. References

- [Luigi documentation](https://luigi.readthedocs.io/en/stable/)
- [Luigi on GitHub](https://github.com/spotify/luigi)
- [Spotify engineering blog — Luigi announcement](https://engineering.atspotify.com/2014/02/luigi-batch-data-processing/)
- [iTunes Search API](https://developer.apple.com/library/archive/documentation/AudioVideo/Conceptual/iTuneSearchAPI/)
- [Elasticsearch Python client compatibility](https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/index.html)
- [Kibana saved objects API](https://www.elastic.co/guide/en/kibana/8.15/saved-objects-api.html)
