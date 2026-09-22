# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

import argparse
import json
import logging
import math
from multiprocessing import Pool, cpu_count
from pathlib import Path
from urllib.parse import quote

import pyarrow.parquet as pq
import requests
import shapely

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

WQP_BASE_URL = "https://www.waterqualitydata.us"
GEOCONNEX_WQP_NAMESPACE = "https://geoconnex.us/wqp"


def clean(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, str) and value.strip() == "":
        return None
    return value


def clean_str(value: object) -> str | None:
    cleaned = clean(value)
    return cleaned if isinstance(cleaned, str) else None


def row_to_jsonld(row: dict) -> dict | None:
    """
    Convert a single WQP monitoring location row into JSON-LD
    compliant with the Geoconnex LocationOrientedShape. Returns None
    if the row is missing a field needed to build a Geoconnex PID or
    geometry, in which case the row is skipped rather than failing
    the whole export.
    """
    wkb = row.get("geometry")
    if not isinstance(wkb, bytes):
        return None
    geometry_obj = shapely.from_wkb(wkb)
    if not isinstance(geometry_obj, shapely.Point):
        return None

    provider_name = clean_str(row.get("provider_name"))
    organization_identifier = clean_str(row.get("organization_identifier"))
    monitoring_location_identifier = clean_str(
        row.get("monitoring_location_identifier")
    )
    organization_formal_name = clean_str(row.get("organization_formal_name"))
    location_type = clean_str(row.get("monitoring_location_type_name"))

    if not (
        provider_name and organization_identifier and monitoring_location_identifier
    ):
        LOGGER.warning(f"Skipping row missing identifying fields: {row}")
        return None

    # matches the existing geoconnex.us/namespaces/wqp redirect pattern:
    # https://geoconnex.us/wqp/{ProviderName}/{OrganizationIdentifier}/{MonitoringLocationIdentifier}
    pid_path = "/".join(
        quote(part, safe="")
        for part in (
            provider_name,
            organization_identifier,
            monitoring_location_identifier,
        )
    )

    place = {
        "@context": {
            "@vocab": "https://schema.org/",
            "gsp": "http://www.opengis.net/ont/geosparql#",
            "hyf": "https://www.opengis.net/def/schema/hy_features/hyf/",
        },
        "@type": ["Place", "hyf:HY_HydrometricFeature", "hyf:HY_HydroLocation"],
        "@id": f"{GEOCONNEX_WQP_NAMESPACE}/{pid_path}",
        # schema:name is required by the LocationOrientedShape (minCount 1); a
        # handful of WQP stations have an empty MonitoringLocationName, so fall
        # back to the identifier rather than emitting a nameless Place.
        "name": clean_str(row.get("monitoring_location_name"))
        or monitoring_location_identifier,
        "description": clean(row.get("monitoring_location_description_text")),
        "hyf:HydroLocationType": location_type,
        "identifier": {
            "@type": "PropertyValue",
            "propertyID": "WQP monitoring location identifier",
            "value": monitoring_location_identifier,
        },
        "url": f"{WQP_BASE_URL}/provider/{pid_path}/",
        "provider": {
            # WQP aggregates data from a wide mix of contributors (federal/state
            # agencies, tribes, universities, nonprofits), so we can't assume
            # GovernmentOrganization the way a single-agency dataset could.
            "@type": "Organization",
            "name": organization_formal_name or provider_name,
        },
        "geo": {
            "@type": "GeoCoordinates",
            "latitude": geometry_obj.y,
            "longitude": geometry_obj.x,
        },
        "gsp:hasGeometry": {
            "@type": "http://www.opengis.net/ont/sf#Point",
            "gsp:asWKT": {"@type": "gsp:wktLiteral", "@value": geometry_obj.wkt},
            "gsp:crs": {"@id": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"},
        },
    }

    # remove nulls (SHACL cleanliness)
    return {k: v for k, v in place.items() if v is not None}


def get_parquet_file(file_location: str) -> Path:
    if Path(file_location).exists():
        LOGGER.info("Found parquet file locally")
        return Path(file_location)

    LOGGER.info(f"Downloading parquet file from {file_location}")
    download_path = Path(__file__).parent / "wqp_monitoring_locations.parquet"
    with requests.get(file_location, stream=True) as response:
        response.raise_for_status()
        with open(download_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    return download_path


def process_row(row: dict) -> str | None:
    jsonld = row_to_jsonld(row)
    if jsonld is None:
        return None
    return json.dumps(jsonld, allow_nan=False)


def main(file_location: str) -> None:
    parquet_file = get_parquet_file(file_location)
    pf = pq.ParquetFile(parquet_file)

    batch_size = 50000
    # Use most cores, but leave 1 free
    num_workers = max(cpu_count() - 1, 1)

    with Pool(processes=num_workers) as pool:
        for batch in pf.iter_batches(batch_size=batch_size):
            rows = batch.to_pylist()
            jsonld_records = pool.map(process_row, rows, chunksize=1000)
            lines = [record for record in jsonld_records if record is not None]
            if lines:
                print("\n".join(lines))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--parquet_file",
        type=str,
        default=(
            "https://github.com/internetofwater/wqp_bulk_exports/releases/"
            "latest/download/wqp_monitoring_locations.parquet"
        ),
    )
    args = parser.parse_args()
    main(args.parquet_file)
