# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

import asyncio
import csv
import io
import logging
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


async def _fetch_csv_rows(
    session: aiohttp.ClientSession,
    url: str,
    params: dict[str, str],
    log_label: str,
) -> list[dict[str, str]]:
    text: str | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, params=params) as response:
                response.raise_for_status()
                text = await response.text()
                break
        except (TimeoutError, aiohttp.ClientError) as e:
            if attempt == MAX_RETRIES:
                raise
            wait = RETRY_BACKOFF_SECONDS * attempt
            LOGGER.warning(
                f"[{log_label}] fetch failed (attempt {attempt}/{MAX_RETRIES}): {e}; "
                f"retrying in {wait}s"
            )
            await asyncio.sleep(wait)

    assert text is not None
    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


async def fetch_stations_for_statecode(
    session: aiohttp.ClientSession, statecode: str
) -> list[dict[str, str]]:
    """
    Fetch every WQP monitoring location (station) within a single
    statecode as a list of CSV row dicts.
    """
    params = {"statecode": statecode, "mimeType": "csv", "zip": "no"}
    return await _fetch_csv_rows(session, STATION_SEARCH_URL, params, statecode)


async def fetch_period_of_record_for_statecode(
    session: aiohttp.ClientSession, statecode: str
) -> list[dict[str, str]]:
    """
    Fetch the periodOfRecord summary for a single statecode: one row per
    (monitoring location, characteristic, year) describing which variables
    have been measured at each location and how often, without fetching
    any of the underlying measurement values.
    """
    params = {
        "statecode": statecode,
        "mimeType": "csv",
        "zip": "no",
        "dataProfile": "periodOfRecord",
        "summaryYears": "all",
    }
    return await _fetch_csv_rows(
        session, PERIOD_OF_RECORD_URL, params, f"{statecode} periodOfRecord"
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
