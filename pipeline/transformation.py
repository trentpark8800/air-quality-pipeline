import argparse
import logging

from database_manager import (
    connect_to_database,
    close_database_connection,
    execute_query,
    collect_query_paths,
    read_query,
)


def transform_data(args) -> None:

    database_path = args.database_path
    con = connect_to_database(path=database_path)
    query_paths = collect_query_paths(args.query_directory)

    for query_path in query_paths:
        query = read_query(query_path)
        execute_query(con, query)

        logging.info(f"Executed query from {query_path}")

    close_database_connection(con)


def main():
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser(description="CLI for Data Transformation")
    parser.add_argument(
        "--database_path", type=str, required=True, help="Path to the DuckDB database"
    )
    parser.add_argument(
        "--query_directory",
        type=str,
        required=True,
        help="Directory containing SQL transformation queries",
    )

    args = parser.parse_args()
    transform_data(args)
    


if __name__ == "__main__":
    main()
