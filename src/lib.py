# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

import asyncio
import csv
import itertools
import logging
from collections.abc import Iterator
from typing import Final

import aiohttp
import pyarrow as pa
import pyarrow.parquet as pq

LOGGER = logging.getLogger(__name__)

WQP_BASE_URL: Final[str] = "https://www.waterqualitydata.us"
STATE_CODES_URL: Final[str] = f"{WQP_BASE_URL}/Codes/statecode?mimeType=json"
STATION_SEARCH_URL: Final[str] = f"{WQP_BASE_URL}/data/Station/search"
PERIOD_OF_RECORD_URL: Final[str] = (
    f"{WQP_BASE_URL}/data/summary/monitoringLocation/search"
)

MAX_RETRIES: Final[int] = 5
RETRY_BACKOFF_SECONDS: Final[float] = 5.0
DOWNLOAD_CHUNK_BYTES: Final[int] = 1024 * 1024
# rows parsed and written to parquet at a time; bounds peak memory per state
CSV_BATCH_ROWS: Final[int] = 50_000


async def fetch_state_codes(session: aiohttp.ClientSession) -> list[str]:
    """
    Fetch the full list of valid FIPS statecode values (e.g. "US:01")
    that the WQP Station search accepts. Used to page through the
    entire nationwide dataset one jurisdiction at a time, since the
    Station search has no native pagination.
    """
    async with session.get(STATE_CODES_URL) as response:
        response.raise_for_status()
        data = await response.json()

    return [code["value"] for code in data["codes"]]


async def _fetch_csv_to_file(
    session: aiohttp.ClientSession,
    url: str,
    params: dict[str, str],
    dest_path: str,
    log_label: str,
) -> None:
    """
    Stream a CSV response to disk in chunks instead of buffering the whole
    body in memory. Large states return hundreds of MB, and holding several
    of those in memory at once can exhaust the GitHub Actions runner.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, params=params) as response:
                response.raise_for_status()
                # truncate on each attempt so a partial download from a
                # failed attempt is discarded; local 1MB chunk writes are
                # fast enough that blocking the event loop is negligible
                with open(dest_path, "wb") as f:  # noqa: ASYNC230
                    async for chunk in response.content.iter_chunked(
                        DOWNLOAD_CHUNK_BYTES
                    ):
                        f.write(chunk)
                return
        except (TimeoutError, aiohttp.ClientError) as e:
            if attempt == MAX_RETRIES:
                raise
            wait = RETRY_BACKOFF_SECONDS * attempt
            LOGGER.warning(
                f"[{log_label}] fetch failed (attempt {attempt}/{MAX_RETRIES}): {e}; "
                f"retrying in {wait}s"
            )
            await asyncio.sleep(wait)


def iter_csv_row_batches(
    path: str, batch_size: int = CSV_BATCH_ROWS
) -> Iterator[list[dict[str, str]]]:
    """
    Read a CSV file from disk as batches of row dicts so that only
    batch_size rows are held in memory at a time.
    """
    with open(path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        while batch := list(itertools.islice(reader, batch_size)):
            yield batch


async def fetch_stations_for_statecode(
    session: aiohttp.ClientSession, statecode: str, dest_path: str
) -> None:
    """
    Fetch every WQP monitoring location (station) within a single
    statecode and stream the CSV to dest_path.
    """
    params = {"statecode": statecode, "mimeType": "csv", "zip": "no"}
    await _fetch_csv_to_file(session, STATION_SEARCH_URL, params, dest_path, statecode)


async def fetch_period_of_record_for_statecode(
    session: aiohttp.ClientSession, statecode: str, dest_path: str
) -> None:
    """
    Fetch the periodOfRecord summary for a single statecode and stream the
    CSV to dest_path: one row per (monitoring location, characteristic,
    year) describing which variables have been measured at each location
    and how often, without fetching any of the underlying measurement values.
    """
    params = {
        "statecode": statecode,
        "mimeType": "csv",
        "zip": "no",
        "dataProfile": "periodOfRecord",
        "summaryYears": "all",
    }
    await _fetch_csv_to_file(
        session,
        PERIOD_OF_RECORD_URL,
        params,
        dest_path,
        f"{statecode} periodOfRecord",
    )


class ParquetFeatureWriter:
    def __init__(self, path: str, schema: pa.Schema):
        self.path = path
        self.schema = schema
        self.writer = pq.ParquetWriter(
            self.path,
            self.schema,
            compression="snappy",
        )

    def write(self, rows: list[dict]):
        if not rows:
            return

        table = pa.Table.from_pylist(rows, schema=self.schema)
        self.writer.write_table(table)

    def close(self):
        if self.writer:
            self.writer.close()
