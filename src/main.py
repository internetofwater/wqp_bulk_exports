# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

import asyncio
import logging
import os
import tempfile
from typing import Final

import aiohttp
import duckdb
import geoparquet_io as gpio
import pyarrow as pa
import shapely

from lib import (
    ParquetFeatureWriter,
    fetch_period_of_record_for_statecode,
    fetch_state_codes,
    fetch_stations_for_statecode,
    iter_csv_row_batches,
)
from schemas import (
    MONITORING_LOCATION_COLUMNS,
    PERIOD_OF_RECORD_COLUMNS,
    monitoring_locations_schema,
    period_of_record_schema,
)

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

# WQP has no per-request pagination for the Station search, so requests
# are split by jurisdiction (statecode) instead. Bound how many of those
# run concurrently so we don't hammer the WQP server.
CONCURRENCY: Final[int] = 6

STATIONS_RAW_PATH: Final[str] = "_wqp_stations_raw.parquet"
PERIOD_OF_RECORD_RAW_PATH: Final[str] = "_wqp_period_of_record_raw.parquet"
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


def _clean_int(value: str | None) -> int | None:
    value = _clean_str(value)
    if value is None:
        return None
    try:
        return int(float(value))
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


def period_of_record_row_to_record(row: dict[str, str]) -> dict | None:
    """
    Convert a single row from the WQP periodOfRecord summary CSV (one row
    per monitoring location / characteristic / year) into a record matching
    period_of_record_schema(). Returns None if the row can't be tied back
    to a location and characteristic.
    """
    monitoring_location_identifier = _clean_str(row.get("MonitoringLocationIdentifier"))
    characteristic_name = _clean_str(row.get("CharacteristicName"))
    if not (monitoring_location_identifier and characteristic_name):
        return None

    record: dict = {}
    for col in PERIOD_OF_RECORD_COLUMNS:
        raw = row.get(col.csv_column)
        if pa.types.is_integer(col.arrow_type):
            record[col.field_name] = _clean_int(raw)
        else:
            record[col.field_name] = _clean_str(raw)

    return record


async def fetch_and_write_state(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    statecode: str,
    station_writer: ParquetFeatureWriter,
    period_of_record_writer: ParquetFeatureWriter,
    write_lock: asyncio.Lock,
) -> None:
    with tempfile.TemporaryDirectory(prefix=f"wqp_{statecode}_") as tmpdir:
        stations_csv = os.path.join(tmpdir, "stations.csv")
        period_of_record_csv = os.path.join(tmpdir, "period_of_record.csv")

        async with semaphore:
            LOGGER.info(f"Fetching stations for statecode={statecode}")
            await asyncio.gather(
                fetch_stations_for_statecode(session, statecode, stations_csv),
                fetch_period_of_record_for_statecode(
                    session, statecode, period_of_record_csv
                ),
            )

        station_count = 0
        skipped = 0
        period_of_record_count = 0
        async with write_lock:
            for batch in iter_csv_row_batches(stations_csv):
                station_records = [
                    record
                    for row in batch
                    if (record := station_row_to_record(row)) is not None
                ]
                skipped += len(batch) - len(station_records)
                station_count += len(station_records)
                station_writer.write(station_records)

            for batch in iter_csv_row_batches(period_of_record_csv):
                period_of_record_records = [
                    record
                    for row in batch
                    if (record := period_of_record_row_to_record(row)) is not None
                ]
                period_of_record_count += len(period_of_record_records)
                period_of_record_writer.write(period_of_record_records)

    if skipped:
        LOGGER.warning(f"[{statecode}] skipped {skipped} stations with no geometry")

    LOGGER.info(
        f"[{statecode}] wrote {station_count} stations, "
        f"{period_of_record_count} characteristic/year summary rows"
    )


def join_and_write_parquet(
    stations_path: str, period_of_record_path: str, output_parquet_path: str
) -> None:
    LOGGER.info(
        f"Joining stations and characteristic summaries and writing to {output_parquet_path}"
    )
    con = duckdb.connect()
    query = f"""
        COPY (
            WITH stations AS (
                SELECT * FROM read_parquet('{stations_path}')
            ),
            characteristics_by_location AS (
                SELECT
                    monitoring_location_identifier,
                    characteristic_type,
                    characteristic_name,
                    sum(activity_count) AS activity_count,
                    sum(result_count) AS result_count,
                    min(year_summarized) AS begin_year,
                    max(year_summarized) AS end_year,
                FROM read_parquet('{period_of_record_path}')
                GROUP BY monitoring_location_identifier, characteristic_type, characteristic_name
            ),
            characteristics_agg AS (
                SELECT
                    monitoring_location_identifier,
                    list(
                        struct_pack(
                            characteristic_type,
                            characteristic_name,
                            activity_count,
                            result_count,
                            begin_year,
                            end_year
                        )
                    ) AS characteristics
                FROM characteristics_by_location
                GROUP BY monitoring_location_identifier
            )

            SELECT
                stations.*,
                characteristics_agg.characteristics
            FROM stations
            LEFT JOIN characteristics_agg
            ON stations.monitoring_location_identifier = characteristics_agg.monitoring_location_identifier
        )
        TO '{output_parquet_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD);
    """
    con.execute(query)
    con.close()


async def main() -> None:
    station_writer = ParquetFeatureWriter(
        STATIONS_RAW_PATH, monitoring_locations_schema()
    )
    period_of_record_writer = ParquetFeatureWriter(
        PERIOD_OF_RECORD_RAW_PATH, period_of_record_schema()
    )
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
                fetch_and_write_state(
                    session,
                    semaphore,
                    statecode,
                    station_writer,
                    period_of_record_writer,
                    write_lock,
                )
                for statecode in statecodes
            )
        )

    station_writer.close()
    period_of_record_writer.close()

    join_and_write_parquet(
        STATIONS_RAW_PATH, PERIOD_OF_RECORD_RAW_PATH, OUTPUT_PARQUET_PATH
    )

    LOGGER.info("Adding geoparquet metadata and sorting by hilbert curve")
    gpio.read(OUTPUT_PARQUET_PATH).add_bbox().sort_hilbert().add_bbox_metadata().write(
        OUTPUT_PARQUET_PATH, overwrite=True
    )
    LOGGER.info("Done")


if __name__ == "__main__":
    asyncio.run(main())
