#Read data in json format.
# Transformation: split beween launches and cores tables
# Write parquet files for each table
import json
import gc
try:
    from utils import PATH_RESOURCES_RAW, PATH_RESOURCES_STAGE
except ImportError:
    from src.utils import PATH_RESOURCES_RAW, PATH_RESOURCES_STAGE
import pandas as pd


LAUNCHES_COLUMNS = [
    "launch_id",
    "success",
    "flight_number",
    "name",
    "date_utc",
    "date_local",
    "date_precision",
    "p_creation_date",
]


def read_json(path):
    try:
        with open(path, 'r') as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        print(f"Error: File '{path}' not found.")
        return None
    except Exception as e:
        print(f"An error occurred while reading file '{path}': {e}")
        return None


def add_date_partition_column(pdf):
    pdf['date_utc'] = pd.to_datetime(pdf['date_utc'])
    pdf['p_creation_date'] = pdf['date_utc'].dt.date
    return pdf


def save_parquet(pdf, path):
    pdf.to_parquet(path, partition_cols=["p_creation_date"], existing_data_behavior='delete_matching')
    gc.collect() # needed otherwise it'll hang after writing


def get_cores(data):
    cores = []
    for datapoint in data:
        for core in datapoint.get("cores"):
            core["launch_id"] = datapoint.get("id")
            core["date_utc"] = datapoint.get("date_utc")
            if core["core"]:
                cores.append(core)
    cores_pdf = (
        pd.DataFrame(cores)
        .rename(columns={'core': 'core_id'})
        .pipe(add_date_partition_column)
        .drop(columns=['date_utc'])
    )
    return cores_pdf


def get_launches(data):
    launches_pdf = pd.DataFrame(data)
    launches_pdf = (
        launches_pdf[launches_pdf["id"].notnull()]
        .rename(columns={'id': 'launch_id'})
        .pipe(add_date_partition_column)
        .drop_duplicates(subset=["launch_id"], keep='last')
    )
    return launches_pdf[LAUNCHES_COLUMNS]


def main():
    print(f"Running transformer for extracted data")
    data = read_json(f"{PATH_RESOURCES_RAW}/data.json")

    if data is None:
        return

    cores_df = get_cores(data)
    save_parquet(cores_df, f"{PATH_RESOURCES_STAGE}/cores")
    print("cores data saved to parquet table")
    launches_df = get_launches(data)
    save_parquet(launches_df, f"{PATH_RESOURCES_STAGE}/launches")
    print("launches data saved to parquet table")


if __name__ == '__main__':

    main()

