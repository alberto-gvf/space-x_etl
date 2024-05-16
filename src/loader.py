# loads parquet files into a postgres database
import argparse
from utils import PATH_RESOURCES_STAGE, PATH_SQL, parse_arguments
import pandas as pd
from sqlalchemy import create_engine, text
import uuid



def get_db_engine():
    """
    Gets Postgres DB engine
    """
    db_name = 'postgres'
    db_user = 'postgres'
    db_password = 'postgres'
    db_host = 'db'  # This needs to be 'localhost' to run from console. 'db' for airflow
    db_port = '5432'

    conn_str = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return create_engine(conn_str)


def read_parquet_table(table, date_from, date_to):
    """
    Read parquet table with time constraints
    :param table: table to be read
    :param date_from: time boundary for the query in YYYY-MM-DD format
    :param date_to: time boundary for the query in YYYY-MM-DD format
    :return: pandas dataframe with result of reading
    """
    try:
        filter = [('p_creation_date', '>=', date_from), ('p_creation_date', '<', date_to)]
        pdf = pd.read_parquet(f"{PATH_RESOURCES_STAGE}/{table}", filters=filter)
        return pdf
    except FileNotFoundError:
        print(f"Error: Parquet table '{table}' not found.")
        return None
    except Exception as e:
        print("Error:", e)
        return None


def get_insert_sql(table_name):
    """
    Reads SQL file with insert.
    :param table_name: table name that will be inserted.
    :return: insert query
    """
    try:
        with open(f'{PATH_SQL}/insert_{table_name}.sql', 'r') as sql_file:
            return sql_file.read()
    except FileNotFoundError:
        print(f"Error: SQL file for table '{table_name}' not found.")
        return None
    except Exception as e:
        print("Error:", e)
        return None


def load_table(table_name, date_from, date_to):
    """
    Reads data from a parquet table and loads it into the postgres db
    :param table_name: table name to read and load
    :param date_from: time boundary for the query in YYYY-MM-DD format
    :param date_to: time boundary for the query in YYYY-MM-DD format
    :return:
    """
    try:
        table_pdf = read_parquet_table(table_name, date_from, date_to)
        engine = get_db_engine()
        temp_table_name = f'{table_name}_{uuid.uuid4().hex[:8]}'
        table_pdf.to_sql(temp_table_name, engine, if_exists='replace', index=False)
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            connection.execute(text(get_insert_sql(table_name).format(temp_table_name=temp_table_name)))
            print(f"Data inserted successfully in {table_name}")
            connection.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))
    except Exception as e:
        print(f"Error when running load_table for {table_name}:", e)


def main():

    start_date, end_date = parse_arguments()
    print(f"Running loader for start_date:{start_date} and end_date: {end_date}")
    load_table("cores", start_date, end_date)
    load_table("launches", start_date, end_date)


if __name__ == '__main__':
    main()
