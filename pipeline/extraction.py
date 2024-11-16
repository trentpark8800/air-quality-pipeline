"""
Example usage: python extraction.py --locations_file_path ../locations.json --start_date 2024-01 --end_date 2024-03 --database_path ../air_quality.db --extract_query_template_path ../sql/dml/raw/0_raw_air_quality_insert.sql --source_base_path s3://openaq-data-archive/records/csv.gz
"""
import argparse
import json
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import List

from duckdb import IOException
from jinja2 import Template

from database_manager import (
    connect_to_database,
    close_database_connection,
    execute_query,
    read_query
)


def read_location_ids(file_path: str) -> List[str]:
    with open(file_path, "r") as f:
        locations = json.load(f)
        f.close()

    location_ids = [str(id) for id in locations.keys()]
    return location_ids


def compile_data_file_paths(
    data_file_path_template: str, location_ids: List[str], start_date: str, end_date: str
) -> List[str]:
    
    start_date = datetime.strptime(start_date, "%Y-%m")
    end_date = datetime.strptime(end_date, "%Y-%m")

    data_file_paths = []
    for location_id in location_ids:
        index_date = start_date
        while index_date <= end_date:
            data_file_path = Template(data_file_path_template).render(
                location_id=location_id,
                year=str(index_date.year),
                month=str(index_date.month).zfill(2)
            )
            data_file_paths.append(data_file_path)
            index_date += relativedelta(months=1)
    return data_file_paths

def compile_data_file_query(
    base_path: str, data_file_path: str, extract_query_template: str
) -> str:
    extract_query = Template(extract_query_template).render(
        data_file_path=f"{base_path}/{data_file_path}"
    )
    return extract_query


def extract_data(args):
    
    location_ids = read_location_ids(args.locations_file_path)

    data_file_path_template = "locationid={{location_id}}/year={{year}}/month={{month}}/*"

    data_file_paths = compile_data_file_paths(
        data_file_path_template=data_file_path_template,
        location_ids=location_ids,
        start_date=args.start_date,
        end_date=args.end_date
    )

    extract_query_template = read_query(path=args.extract_query_template_path)

    con = connect_to_database(path=args.database_path)

    for data_file_path in data_file_paths:
        logging.info(f"Extracting data from {data_file_path}")
        query = compile_data_file_query(
            base_path=args.source_base_path,
            data_file_path=data_file_path,
            extract_query_template=extract_query_template
        )

        try:
            execute_query(con, query)
            logging.info(f"Extracted data from {data_file_path}!")
        except IOException as e:
            logging.warning(f"Could not find data from {data_file_path}: {e}")
    
    close_database_connection(con)


def main():
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser(description="CLI for ELT Extraction")
    parser.add_argument(
        "--locations_file_path",
        type=str,
        required=True,
        help="Path to the locations JSON file",
    )
    parser.add_argument(
        "--start_date", type=str, required=True, help="Start date in YYYY-MM format"
    )
    parser.add_argument(
        "--end_date", type=str, required=True, help="End date in YYYY-MM format"
    )
    parser.add_argument(
        "--extract_query_template_path",
        type=str,
        required=True,
        help="Path to the SQL extraction query template",
    )
    parser.add_argument(
        "--database_path", type=str, required=True, help="Path to the database"
    )
    parser.add_argument(
        "--source_base_path",
        type=str,
        required=True,
        help="Base path for the remote data files",
    )

    args = parser.parse_args()
    extract_data(args)


if __name__ == "__main__":
    main()
