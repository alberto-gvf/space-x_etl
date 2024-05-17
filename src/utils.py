import argparse
from pathlib import Path

PATH_RESOURCES = Path(__file__).parent.parent / "resources"
PATH_RESOURCES_RAW = PATH_RESOURCES / "raw"
PATH_RESOURCES_STAGE = PATH_RESOURCES / "stage"
PATH_SQL = Path(__file__).parent.parent / "src" / "sql"

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=str, help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end_date', type=str, help='End date in YYYY-MM-DD format')
    args = parser.parse_args()
    start_date = args.start_date
    end_date = args.end_date
    return (start_date, end_date)