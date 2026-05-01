"""Luigi DAG: fetch -> merge -> clean -> enrich -> load Postgres / index Elasticsearch.

Run from inside the worker container:

    python -m luigi --module pipeline.tasks RunAll --scheduler-host luigid

Implementation notes
--------------------
* Every task that produces a CSV uses ``self.output().temporary_path()`` instead
  of ``self.output().open("w")``.  Pandas' ``to_csv`` flushes through the file
  handle on close, and Luigi's text-mode ``AtomicLocalFile`` wrapper reports
  bytes-vs-str errors on Luigi 3.8 + pandas 3.0.  ``temporary_path`` keeps the
  atomic-rename guarantee while letting pandas write to a real filesystem path.
* The DAG fans out: one ``FetchArtistTracks`` per artist runs in parallel
  (great Luigi UI demo), then ``MergeArtistTracks`` fans them back in.
"""
from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path

import luigi
import pandas as pd
import requests
from elasticsearch import Elasticsearch, helpers
from sqlalchemy import create_engine, text

from pipeline.config import settings

log = logging.getLogger("luigi-interface")


def _slugify(value: str) -> str:
    """Filesystem-safe lowercase slug for an artist name."""
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_") or "artist"


def _read_artists() -> list[str]:
    """Read the seed artist list, skipping comments and blanks."""
    path = settings.artists_file
    if not path.exists():
        raise FileNotFoundError(f"Seed artists file missing at {path}")
    artists: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        artists.append(line)
    if not artists:
        raise ValueError(f"No artists found in {path}")
    return artists


class PipelineTask(luigi.Task):
    """Base class: every task is parameterised by a run_date so re-runs on a
    new date produce a fresh set of file targets, while re-runs on the same
    date are idempotent (Luigi sees the targets already exist)."""

    run_date = luigi.DateParameter(default=datetime.utcnow().date())

    @property
    def run_dir(self) -> Path:
        d = settings.output_dir / self.run_date.isoformat()
        d.mkdir(parents=True, exist_ok=True)
        return d


class FetchArtistTracks(luigi.Task):
    """Step 1 (leaf, parallelisable): fetch one artist's top tracks from the
    iTunes Search API and persist the raw JSON response.

    Why iTunes Search API and not Spotify? Zero auth, no API key, generous
    polite-use limit. Anyone running ``make run`` reproduces the demo without
    creating a developer account.
    """

    artist = luigi.Parameter()
    run_date = luigi.DateParameter(default=datetime.utcnow().date())
    limit = luigi.IntParameter(default=settings.itunes_per_artist_limit)

    @property
    def _raw_dir(self) -> Path:
        d = settings.output_dir / self.run_date.isoformat() / "raw"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(str(self._raw_dir / f"{_slugify(self.artist)}.json"))

    def run(self) -> None:
        params = {
            "term": self.artist,
            "entity": "song",
            "media": "music",
            "limit": int(self.limit),
        }
        log.info("FetchArtistTracks(%s): GET %s", self.artist, settings.itunes_search_url)
        resp = requests.get(settings.itunes_search_url, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        payload["_query_artist"] = self.artist  # remember what we asked for

        with self.output().temporary_path() as out_path:
            Path(out_path).write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

        # Polite throttle so 20 parallel artists don't hammer the endpoint.
        time.sleep(settings.itunes_request_sleep_s)
        log.info(
            "FetchArtistTracks(%s): %d results",
            self.artist,
            payload.get("resultCount", 0),
        )


class MergeArtistTracks(PipelineTask):
    """Step 2 (fan-in): combine every artist's JSON into a single raw CSV."""

    def requires(self):
        return [
            FetchArtistTracks(artist=a, run_date=self.run_date)
            for a in _read_artists()
        ]

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(str(self.run_dir / "raw.csv"))

    def run(self) -> None:
        rows: list[dict] = []
        for inp in self.input():
            payload = json.loads(Path(inp.path).read_text(encoding="utf-8"))
            query_artist = payload.get("_query_artist", "")
            for i, item in enumerate(payload.get("results", []), start=1):
                if item.get("kind") != "song":
                    continue
                release_date = item.get("releaseDate") or ""
                release_year = (
                    int(release_date[:4]) if release_date[:4].isdigit() else None
                )
                rows.append(
                    {
                        "track_id": str(item.get("trackId") or ""),
                        "track_name": item.get("trackName"),
                        "artist": item.get("artistName"),
                        "query_artist": query_artist,
                        "album": item.get("collectionName"),
                        "release_date": release_date[:10] if release_date else None,
                        "release_year": release_year,
                        "duration_ms": item.get("trackTimeMillis"),
                        "genre": item.get("primaryGenreName"),
                        "country": item.get("country"),
                        "rank_in_artist": i,
                    }
                )

        df = pd.DataFrame(rows)
        with self.output().temporary_path() as out_path:
            df.to_csv(out_path, index=False)
        log.info(
            "MergeArtistTracks: %d rows merged from %d artist files",
            len(df),
            len(self.input()),
        )


class CleanData(PipelineTask):
    """Step 3: drop nulls on critical fields, trim whitespace, dedupe by track_id."""

    def requires(self) -> luigi.Task:
        return MergeArtistTracks(run_date=self.run_date)

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(str(self.run_dir / "clean.csv"))

    def run(self) -> None:
        df = pd.read_csv(self.input().path)

        before = len(df)
        df = df.dropna(subset=["track_id", "track_name", "artist", "duration_ms"])
        df = df[df["track_id"].astype(str).str.len() > 0]
        df["track_name"] = df["track_name"].astype(str).str.strip()
        df["artist"] = df["artist"].astype(str).str.strip()
        df = df.drop_duplicates(subset=["track_id"])

        with self.output().temporary_path() as out_path:
            df.to_csv(out_path, index=False)
        log.info("CleanData: %d -> %d rows", before, len(df))


class EnrichData(PipelineTask):
    """Step 4: derive decade and length category from the cleaned dataframe."""

    def requires(self) -> luigi.Task:
        return CleanData(run_date=self.run_date)

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(str(self.run_dir / "enriched.csv"))

    def run(self) -> None:
        df = pd.read_csv(self.input().path)

        df["decade"] = (df["release_year"] // 10 * 10).astype("Int64")
        df["length_category"] = pd.cut(
            df["duration_ms"],
            bins=[0, 180_000, 240_000, 10_000_000],
            labels=["short", "standard", "long"],
        ).astype("string")
        df["duration_min"] = (df["duration_ms"] / 60_000).round(2)

        with self.output().temporary_path() as out_path:
            df.to_csv(out_path, index=False)
        log.info("EnrichData: %d rows, %d columns", len(df), len(df.columns))


class LoadToPostgres(PipelineTask):
    """Step 5a: write the enriched dataframe to Postgres table ``top_tracks``."""

    def requires(self) -> luigi.Task:
        return EnrichData(run_date=self.run_date)

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(str(self.run_dir / "_loaded_postgres.marker"))

    def run(self) -> None:
        df = pd.read_csv(self.input().path)

        engine = create_engine(settings.postgres_url, future=True)
        with engine.begin() as conn:
            df.to_sql("top_tracks", conn, if_exists="replace", index=False)
            count = conn.execute(text("SELECT COUNT(*) FROM top_tracks")).scalar()

        with self.output().temporary_path() as out_path:
            Path(out_path).write_text(
                f"loaded {count} rows into Postgres at {datetime.utcnow().isoformat()}Z\n"
            )
        log.info("LoadToPostgres: %d rows in top_tracks", count)


class IndexToElasticsearch(PipelineTask):
    """Step 5b: bulk-index the enriched dataframe into Elasticsearch."""

    def requires(self) -> luigi.Task:
        return EnrichData(run_date=self.run_date)

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(str(self.run_dir / "_indexed_es.marker"))

    def run(self) -> None:
        df = pd.read_csv(self.input().path)

        es = Elasticsearch(settings.es_url, request_timeout=30)
        index = settings.es_index

        if es.indices.exists(index=index):
            es.indices.delete(index=index)

        es.indices.create(
            index=index,
            mappings={
                "properties": {
                    "track_id": {"type": "keyword"},
                    "track_name": {"type": "text"},
                    "artist": {"type": "keyword"},
                    "query_artist": {"type": "keyword"},
                    "album": {"type": "text"},
                    "release_date": {"type": "date", "format": "yyyy-MM-dd"},
                    "release_year": {"type": "integer"},
                    "decade": {"type": "integer"},
                    "duration_ms": {"type": "integer"},
                    "duration_min": {"type": "float"},
                    "length_category": {"type": "keyword"},
                    "genre": {"type": "keyword"},
                    "country": {"type": "keyword"},
                    "rank_in_artist": {"type": "integer"},
                }
            },
        )

        def _actions():
            for _, row in df.iterrows():
                source = {k: (None if pd.isna(v) else v) for k, v in row.items()}
                source = {k: v for k, v in source.items() if v is not None}
                yield {"_index": index, "_id": str(row["track_id"]), "_source": source}

        ok, errors = helpers.bulk(es, _actions(), raise_on_error=False)
        with self.output().temporary_path() as out_path:
            Path(out_path).write_text(
                f"indexed {ok} docs into '{index}', errors={len(errors)}, "
                f"at {datetime.utcnow().isoformat()}Z\n"
            )
        log.info("IndexToElasticsearch: %d ok, %d errors", ok, len(errors))


class RunAll(luigi.WrapperTask):
    """Top-level entrypoint. Loads to Postgres and indexes to Elasticsearch in
    parallel; both share the same upstream EnrichData target, which itself
    fans in 1 JSON file per artist via FetchArtistTracks."""

    run_date = luigi.DateParameter(default=datetime.utcnow().date())

    def requires(self):
        yield LoadToPostgres(run_date=self.run_date)
        yield IndexToElasticsearch(run_date=self.run_date)
