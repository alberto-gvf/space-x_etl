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
    """
    Reads json file from path
    :param path: Full Path where json is located
    :return: dictionary with content of the json file
    """
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
    """
    Adds partition column p_creation_date using date_utc
    :param pdf: pandas dataframe
    :return: pandas dataframe with partition column
    """
    pdf['date_utc'] = pd.to_datetime(pdf['date_utc'])
    pdf['p_creation_date'] = pdf['date_utc'].dt.date
    return pdf


def save_parquet(pdf, path):
    """
    Saves pandas dataframe to a parquet table. Dataframe needs to have p_creation_date column.
    Garbage collector is needed, otherwise performance of this method is extreamly bad.
    :param pdf: pandas dataframe to save to parquet.
    :param path: path where the data will be stored
    """
    pdf.to_parquet(path, partition_cols=["p_creation_date"], existing_data_behavior='delete_matching')
    gc.collect()  # needed otherwise it'll hang after writing


def get_cores(data):
    """
    Extract cores data from raw data
    :param data: raw data.
    :return: Pandas dataframe with cores information
    """
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
    """
    Extract launches data from raw data
    :param data: raw data.
    :return: Pandas dataframe with launches information
    """
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

