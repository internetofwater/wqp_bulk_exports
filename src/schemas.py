# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

from typing import Final, NamedTuple

import pyarrow as pa
from geoarrow import pyarrow as ga


class ColumnMapping(NamedTuple):
    # column name as returned by the WQP Station search CSV
    csv_column: str
    # snake_case field name used in the output parquet / JSON-LD template
    field_name: str
    arrow_type: pa.DataType


# https://www.waterqualitydata.us/data/Station/search?mimeType=csv
# One row per WQP monitoring location (station).
MONITORING_LOCATION_COLUMNS: Final[list[ColumnMapping]] = [
    ColumnMapping("OrganizationIdentifier", "organization_identifier", pa.string()),
    ColumnMapping("OrganizationFormalName", "organization_formal_name", pa.string()),
    ColumnMapping(
        "MonitoringLocationIdentifier", "monitoring_location_identifier", pa.string()
    ),
    ColumnMapping("MonitoringLocationName", "monitoring_location_name", pa.string()),
    ColumnMapping(
        "MonitoringLocationTypeName", "monitoring_location_type_name", pa.string()
    ),
    ColumnMapping(
        "MonitoringLocationDescriptionText",
        "monitoring_location_description_text",
        pa.string(),
    ),
    ColumnMapping("HUCEightDigitCode", "huc_eight_digit_code", pa.string()),
    ColumnMapping(
        "DrainageAreaMeasure/MeasureValue", "drainage_area_measure_value", pa.float64()
    ),
    ColumnMapping(
        "DrainageAreaMeasure/MeasureUnitCode",
        "drainage_area_measure_unit_code",
        pa.string(),
    ),
    ColumnMapping(
        "ContributingDrainageAreaMeasure/MeasureValue",
        "contributing_drainage_area_measure_value",
        pa.float64(),
    ),
    ColumnMapping(
        "ContributingDrainageAreaMeasure/MeasureUnitCode",
        "contributing_drainage_area_measure_unit_code",
        pa.string(),
    ),
    ColumnMapping("LatitudeMeasure", "latitude_measure", pa.float64()),
    ColumnMapping("LongitudeMeasure", "longitude_measure", pa.float64()),
    ColumnMapping(
        "HorizontalAccuracyMeasure/MeasureValue",
        "horizontal_accuracy_measure_value",
        pa.float64(),
    ),
    ColumnMapping(
        "HorizontalAccuracyMeasure/MeasureUnitCode",
        "horizontal_accuracy_measure_unit_code",
        pa.string(),
    ),
    ColumnMapping(
        "HorizontalCollectionMethodName",
        "horizontal_collection_method_name",
        pa.string(),
    ),
    ColumnMapping(
        "HorizontalCoordinateReferenceSystemDatumName",
        "horizontal_coordinate_reference_system_datum_name",
        pa.string(),
    ),
    ColumnMapping(
        "VerticalMeasure/MeasureValue", "vertical_measure_value", pa.float64()
    ),
    ColumnMapping(
        "VerticalMeasure/MeasureUnitCode", "vertical_measure_unit_code", pa.string()
    ),
    ColumnMapping("CountryCode", "country_code", pa.string()),
    ColumnMapping("StateCode", "state_code", pa.string()),
    ColumnMapping("CountyCode", "county_code", pa.string()),
    ColumnMapping("AquiferName", "aquifer_name", pa.string()),
    ColumnMapping("FormationTypeText", "formation_type_text", pa.string()),
    ColumnMapping("AquiferTypeName", "aquifer_type_name", pa.string()),
    ColumnMapping("ConstructionDateText", "construction_date_text", pa.string()),
    ColumnMapping(
        "WellDepthMeasure/MeasureValue", "well_depth_measure_value", pa.float64()
    ),
    ColumnMapping(
        "WellDepthMeasure/MeasureUnitCode", "well_depth_measure_unit_code", pa.string()
    ),
    ColumnMapping("ProviderName", "provider_name", pa.string()),
]


def monitoring_locations_schema() -> pa.Schema:
    fields = [
        pa.field(col.field_name, col.arrow_type) for col in MONITORING_LOCATION_COLUMNS
    ]
    fields.append(pa.field("geometry", ga.wkb()))
    return pa.schema(fields)
