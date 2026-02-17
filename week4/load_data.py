#!/usr/bin/env python3
import argparse
from pathlib import Path

import duckdb
import requests

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
DEFAULT_TYPES = ("yellow", "green", "fhv")


def update_gitignore(root_dir: Path) -> None:
    gitignore_path = root_dir / ".gitignore"
    line = "data/"
    if gitignore_path.exists():
        content = gitignore_path.read_text()
        if line in content:
            return
        with gitignore_path.open("a") as file:
            if not content.endswith("\n"):
                file.write("\n")
            file.write("# Local raw taxi data\n")
            file.write(f"{line}\n")
        return

    gitignore_path.write_text("# Local raw taxi data\ndata/\n")


def download_csv_gz(csv_url: str, dest_path: Path) -> None:
    response = requests.get(csv_url, stream=True, timeout=120)
    response.raise_for_status()
    with dest_path.open("wb") as file:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                file.write(chunk)


def download_and_convert_month(
    work_dir: Path,
    taxi_type: str,
    year: int,
    month: int,
    converter: duckdb.DuckDBPyConnection,
) -> Path:
    taxi_dir = work_dir / "data" / taxi_type
    taxi_dir.mkdir(parents=True, exist_ok=True)

    parquet_name = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    parquet_path = taxi_dir / parquet_name
    if parquet_path.exists():
        print(f"Skipping {parquet_name} (already exists)")
        return parquet_path

    csv_gz_name = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
    csv_gz_path = taxi_dir / csv_gz_name
    csv_url = f"{BASE_URL}/{taxi_type}/{csv_gz_name}"

    print(f"Downloading {csv_gz_name}")
    download_csv_gz(csv_url, csv_gz_path)

    print(f"Converting {csv_gz_name} to parquet")
    converter.execute(
        f"""
        copy (
            select * from read_csv_auto('{csv_gz_path}')
        ) to '{parquet_path}' (format parquet)
        """
    )
    csv_gz_path.unlink()
    print(f"Completed {parquet_name}")
    return parquet_path


def load_duckdb_table(
    con: duckdb.DuckDBPyConnection, work_dir: Path, taxi_type: str
) -> int:
    parquet_glob = work_dir / "data" / taxi_type / "*.parquet"
    table_name = f"{taxi_type}_tripdata"
    print(f"Loading main.{table_name} from {parquet_glob}")
    con.execute(
        f"""
        create schema if not exists main;
        create or replace table main.{table_name} as
        select *
        from read_parquet('{parquet_glob}', union_by_name=true)
        """
    )
    row_count = con.execute(
        f"select count(*) from main.{table_name}"
    ).fetchone()[0]
    print(f"Loaded {row_count:,} rows into main.{table_name}")
    return row_count


def show_sample(con: duckdb.DuckDBPyConnection, table_name: str, limit: int = 3) -> None:
    full_name = f"main.{table_name}"
    print(f"\n--- {full_name} columns ---")
    for col in con.execute(f"describe {full_name}").fetchall():
        print(col)

    print(f"\n--- {full_name} sample ({limit} rows) ---")
    for row in con.execute(f"select * from {full_name} limit {limit}").fetchall():
        print(row)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download NYC taxi data, convert to parquet, and load into DuckDB."
    )
    parser.add_argument(
        "--db-path",
        default="nyc_taxi.duckdb",
        help="Path to DuckDB file. Default: nyc_taxi.duckdb",
    )
    parser.add_argument(
        "--work-dir",
        default=".",
        help="Directory where data/ cache is stored. Default: current directory",
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=[2019],
        help="Years to ingest. Default: 2019",
    )
    parser.add_argument(
        "--types",
        nargs="+",
        default=list(DEFAULT_TYPES),
        choices=list(DEFAULT_TYPES),
        help="Taxi types to ingest. Default: yellow green fhv",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Print schema and sample rows after loading tables.",
    )
    args = parser.parse_args()

    work_dir = Path(args.work_dir).expanduser().resolve()
    db_path = Path(args.db_path).expanduser().resolve()
    print(f"Using work dir: {work_dir}")
    print(f"Using DuckDB file: {db_path}")

    update_gitignore(work_dir)

    converter = duckdb.connect()
    try:
        for taxi_type in args.types:
            for year in args.years:
                for month in range(1, 13):
                    download_and_convert_month(
                        work_dir, taxi_type, year, month, converter
                    )
    finally:
        converter.close()

    con = duckdb.connect(str(db_path))
    try:
        for taxi_type in args.types:
            load_duckdb_table(con, work_dir, taxi_type)
        if args.sample:
            for taxi_type in args.types:
                show_sample(con, f"{taxi_type}_tripdata")
    finally:
        con.close()

    print("\nLoad completed.")


if __name__ == "__main__":
    main()
