# loads parquet files into a postgres database
import argparse
from utils import PATH_RESOURCES_STAGE, PATH_SQL, parse_arguments
import pandas as pd
from sqlalchemy import create_engine, text
import uuid



def get_db_engine():
    db_name = 'postgres'
    db_user = 'postgres'
    db_password = 'postgres'
    db_host = 'db'  # This needs to be 'localhost' to run from console. 'db' for airflow
    db_port = '5432'

    conn_str = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return create_engine(conn_str)


def read_parquet_table(table, date_from, date_to):
    filter = [('p_creation_date', '>=', date_from), ('p_creation_date', '<', date_to)]
    pdf = pd.read_parquet(f"{PATH_RESOURCES_STAGE}/{table}", filters=filter)
    return pdf


def get_insert_sql(table_name):
    with open(f'{PATH_SQL}/insert_{table_name}.sql', 'r') as sql_file:
        return sql_file.read()


def load_table(table_name, date_from, date_to):
    table_pdf = read_parquet_table(table_name, date_from, date_to)
    engine = get_db_engine()
    temp_table_name = f'{table_name}_{uuid.uuid4().hex[:8]}'
    table_pdf.to_sql(temp_table_name, engine, if_exists='replace', index=False)
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        connection.execute(text(get_insert_sql(table_name).format(temp_table_name=temp_table_name)))
        print(f"Data inserted successfully in {table_name}")
        connection.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))


def main():

    start_date, end_date = parse_arguments()
    print(f"Running loader for start_date:{start_date} and end_date: {end_date}")
    load_table("cores", start_date, end_date)
    load_table("launches", start_date, end_date)


if __name__ == '__main__':
    main()
