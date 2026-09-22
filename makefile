init_sandbox_rules:
	sbx kit add claude-wqp-bulk ./dev-kit/

deps:
	uv sync --all-extras

run_test:
	TEST_MODE=true uv run src/main.py

run_full:
	uv run src/main.py

prek:
	prek install
	prek run --all-files

check_metadata:
	duckdb -c "SELECT * FROM read_parquet('wqp_monitoring_locations.parquet') LIMIT 5;"
	duckdb -c "SELECT COUNT(*) FROM read_parquet('wqp_monitoring_locations.parquet')"
	uv run gpio check spec wqp_monitoring_locations.parquet

build_geoconnex_bulk_container:
	docker build -t geoconnex_bulk .

run_geoconnex_bulk_container:
	docker run --rm -v "$(PWD)":/data geoconnex_bulk --parquet_file /data/wqp_monitoring_locations.parquet
