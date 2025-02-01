
import pandas as pd
import pyarrow.parquet as pa
from sqlalchemy import create_engine
import argparse
import wget
import os
import time

def download_parquet_dataset(url):
    """
    Download the parquet dataset
    """
    parquet_file_name = url.split("/")[-1]
    cwd = os.getcwd()
    full_parquet_file_path = os.path.join(cwd, parquet_file_name)

    wget.download(url, full_parquet_file_path)
    

def ingest_data(engine, main_table_name, zones_table_name):
    """
    Ingest data from Parquet to Postgres
    """
    # ## Load Dataframe from Parquet
    pf = pa.ParquetFile('yellow_tripdata_2021-01.parquet')
    first_ten_rows = next(pf.iter_batches(batch_size=10))
    df = first_ten_rows.to_pandas()


    # make sure to update datetime columns before creating empty table
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])


    # ## Get Schema for CREATE TABLE (DDL)
    print(pd.io.sql.get_schema(df, name=f"{main_table_name}", con=engine))


    # ## Create Table with Columns But No Data (using `head(n=0)` to avoid inserting data)
    df.head(n=0).to_sql(name=f"{main_table_name}", con=engine, if_exists="replace")

    # ## For Each 100K Chunk of Data, Append To Table
    pf2 = pa.ParquetFile('yellow_tripdata_2021-01.parquet')
    df_iter = pf2.iter_batches(batch_size=100000)

    for i, data_chunk in enumerate(df_iter, 1):
        time_start = time.time()

        data_chunk_df = data_chunk.to_pandas()

        # update the datetime columns
        data_chunk_df["tpep_pickup_datetime"] = pd.to_datetime(data_chunk_df["tpep_pickup_datetime"])
        data_chunk_df["tpep_dropoff_datetime"] = pd.to_datetime(data_chunk_df["tpep_dropoff_datetime"])

        # append the data to the table
        data_chunk_df.to_sql(name=f"{main_table_name}", con=engine, if_exists="append")

        time_end = time.time()

        benchmark_time = f"{time_end - time_start:.2f}"
        
        print(f"Data chunk {i} appended to table. Took {benchmark_time} seconds")


    # ## Add and Ingest Zones Table
    # *This table contains the mapping of location IDs to boroughs and zones*
    df_zones = pd.read_csv("https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv")
    df_zones.to_sql(name=f"{zones_table_name}", con=engine, if_exists="replace")


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    main_table_name = params.table_name
    zones_table_name = "zones"
    parquet_url = params.parquet_url


    # ## Connect to Postgres Database via SQLAlchemy
    try:
        engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
        engine.connect()
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return
    
    # ## Download the Parquet File
    download_parquet_dataset(parquet_url)

    
    # ## Ingest the Data To Postgres
    ingest_data(engine, main_table_name, zones_table_name)


    # ## Verify Data Ingestion
    # *equivalent to `\dt`*
    query = """
        SELECT * 
        FROM pg_catalog.pg_tables
        WHERE schemaname != 'pg_catalog' AND 
            schemaname != 'information_schema';
    """
    print(f"Tables in {db} database:\n", pd.read_sql(query, con=engine))

    print(f"TOTAL RECORDS IN {main_table_name}:", pd.read_sql(f"SELECT COUNT(*) FROM {main_table_name}", con=engine))
    print(f"TOTAL RECORDS IN {zones_table_name}:", pd.read_sql(f"SELECT COUNT(*) FROM {zones_table_name}", con=engine))

    


if __name__ == "__main__":
    # ## Command Line Arguments
    parser = argparse.ArgumentParser(description="Ingest Parquet data to Postgres")

    # user
    # password
    # host
    # port
    # database name
    # table name
    # url of the parquet file

    parser.add_argument("--user", help="Username for Postgres")
    parser.add_argument("--password", help="Password for Postgres")
    parser.add_argument("--host", help="Hot for Postgres")
    parser.add_argument("--port", help="Port for Postgres")
    parser.add_argument("--db", help="Database name for Postgres")
    parser.add_argument("--table_name", help="Name of the table where we will write the results to")
    parser.add_argument("--parquet_url", help="URL of the Parquet file to download")

    args = parser.parse_args()

    main(args)








