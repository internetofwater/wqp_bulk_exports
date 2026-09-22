# WQP Bulk RDF

The following repo contains the ETL workflow to output all data from the EPA's water quality portal into RDF as a Geoconnex bulk loader. It should stream JSON-LD to standard out in accordance with the Geoconnex SHACL shape. It should generally match the structure of other bulk integrations like https://github.com/internetofwater/usgs_monitoring_locations_bulk_exports

Is a replacement for https://github.com/cgs-earth/wqp which previously hosted a live crawler and ran a live pygeoapi server which had to be crawled one by one. This was slow and less maintainable. That workflow previously also downloaded all the timeseries data as well. That is no longer needed. If the RDF needs to produce links to data downloads, it can refer to something which is closer to a landing page for a specific location. If that doesn't exist, it could be omitted.

The workflow should include 2 parts:

1. A crawl and geoparquet generation step which can be triggered by a Github workflow
2. A container which can be ran as a Geoconnex bulk loader. This container will take the geoparquet as input and stream the JSON-LD to standard out. It is essentially just applying a template to the data.
