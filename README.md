# wqp_bulk_exports

Geoconnex [bulk integration](https://docs.geoconnex.us/contributing/bulk/) for the
[EPA Water Quality Portal](https://www.waterqualitydata.us/) (WQP). Replaces
[cgs-earth/wqp](https://github.com/cgs-earth/wqp), which crawled WQP monitoring
locations one at a time through a live pygeoapi server.

The workflow has two parts, matching the pattern used by
[usgs_monitoring_locations_bulk_exports](https://github.com/internetofwater/usgs_monitoring_locations_bulk_exports):

1. **Crawl + Geoparquet export** (`src/main.py`) — pages through the WQP
   [Station search](https://www.waterqualitydata.us/webservices_documentation/) one
   jurisdiction (`statecode`) at a time, since the endpoint has no native
   pagination, and writes every monitoring location to a single Geoparquet file.
   Triggered on demand (or monthly) by
   [`.github/workflows/export_geoparquet.yml`](.github/workflows/export_geoparquet.yml),
   which publishes the parquet file as a GitHub Release asset.
2. **Bulk RDF container** (`src/template.py`) — reads that Geoparquet file
   (locally, or downloaded from the latest GitHub Release) and streams one
   JSON-LD document per line to standard out, conforming to the
   [Geoconnex SHACL shape](https://docs.geoconnex.us/reference/data-formats/shacl_shape).
   Built and pushed to `ghcr.io/internetofwater/wqp_bulk_rdf` by
   [`.github/workflows/push_to_ghcr.yml`](.github/workflows/push_to_ghcr.yml) on every
   push to `main`.

No timeseries/results data is fetched or included — only monitoring location
(station) metadata. Each feature's `url` links back to its live WQP provider
landing page (`https://www.waterqualitydata.us/provider/.../.../.../`) rather
than bundling a data download.

## Identifiers

Geoconnex already has a `wqp` namespace
([namespaces/wqp](https://github.com/internetofwater/geoconnex.us/tree/master/namespaces/wqp)
in `geoconnex.us`) with the redirect pattern:

```
https://geoconnex.us/wqp/{ProviderName}/{OrganizationIdentifier}/{MonitoringLocationIdentifier}
  -> https://www.waterqualitydata.us/provider/{ProviderName}/{OrganizationIdentifier}/{MonitoringLocationIdentifier}/
```

## Development

Requires [uv](https://docs.astral.sh/uv/).

```bash
make deps          # uv sync
make run_test       # crawl a couple of states only, for a quick smoke test
make run_full        # crawl the full nationwide WQP station dataset
make check_metadata  # sanity-check the resulting parquet file
make build_geoconnex_bulk_container
make run_geoconnex_bulk_container   # stream JSON-LD from a local parquet file
```

`prek` (pre-commit) handles linting/formatting/license headers:

```bash
make prek
```
