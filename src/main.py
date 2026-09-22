# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

import asyncio
import logging
import os
from typing import Final

import aiohttp
import geoparquet_io as gpio
import pyarrow as pa
import shapely

from lib import ParquetFeatureWriter, fetch_state_codes, fetch_stations_for_statecode
from schemas import MONITORING_LOCATION_COLUMNS, monitoring_locations_schema

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

# WQP has no per-request pagination for the Station search, so requests
# are split by jurisdiction (statecode) instead. Bound how many of those
# run concurrently so we don't hammer the WQP server.
CONCURRENCY: Final[int] = 6

OUTPUT_PARQUET_PATH: Final[str] = "wqp_monitoring_locations.parquet"

# The largest states (CA, TX) return hundreds of thousands of stations in a
# single request; the default aiohttp timeout (5 minutes) is too tight for
# that against the WQP server.
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=1800)


def _clean_str(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    return value or None


def _clean_float(value: str | None) -> float | None:
    value = _clean_str(value)
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def station_row_to_record(row: dict[str, str]) -> dict | None:
    """
    Convert a single row from the WQP Station search CSV into a record
    matching monitoring_locations_schema(). Returns None if the station
    has no usable point geometry, since the Geoconnex SHACL shape
    requires every Place to have one.
    """
    lat = _clean_float(row.get("LatitudeMeasure"))
    lon = _clean_float(row.get("LongitudeMeasure"))
    if lat is None or lon is None:
        return None

    record: dict = {}
    for col in MONITORING_LOCATION_COLUMNS:
        raw = row.get(col.csv_column)
        if pa.types.is_floating(col.arrow_type):
            record[col.field_name] = _clean_float(raw)
        else:
            record[col.field_name] = _clean_str(raw)

    record["geometry"] = shapely.Point(lon, lat).wkb
    return record


async def fetch_and_write_state(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    statecode: str,
    writer: ParquetFeatureWriter,
    write_lock: asyncio.Lock,
) -> None:
    async with semaphore:
        LOGGER.info(f"Fetching stations for statecode={statecode}")
        rows = await fetch_stations_for_statecode(session, statecode)

    records = [
        record for row in rows if (record := station_row_to_record(row)) is not None
    ]
    skipped = len(rows) - len(records)
    if skipped:
        LOGGER.warning(f"[{statecode}] skipped {skipped} stations with no geometry")

    async with write_lock:
        writer.write(records)

    LOGGER.info(f"[{statecode}] wrote {len(records)} stations")


async def main() -> None:
    writer = ParquetFeatureWriter(OUTPUT_PARQUET_PATH, monitoring_locations_schema())
    write_lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(CONCURRENCY)

    async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT) as session:
        statecodes = await fetch_state_codes(session)

        if os.environ.get("TEST_MODE"):
            LOGGER.warning("Fetching subset of data since env var TEST_MODE was set")
            statecodes = statecodes[:2]

        LOGGER.info(f"Fetching WQP stations across {len(statecodes)} jurisdictions")

        await asyncio.gather(
            *(
                fetch_and_write_state(session, semaphore, statecode, writer, write_lock)
                for statecode in statecodes
            )
        )

    writer.close()

    LOGGER.info("Adding geoparquet metadata and sorting by hilbert curve")
    gpio.read(OUTPUT_PARQUET_PATH).add_bbox().sort_hilbert().add_bbox_metadata().write(
        OUTPUT_PARQUET_PATH, overwrite=True
    )
    LOGGER.info("Done")


if __name__ == "__main__":
    asyncio.run(main())
