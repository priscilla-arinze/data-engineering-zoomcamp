
import pandas as pd
from sqlalchemy import create_engine
import argparse
import wget
import os
import gzip
import time

def download_unzip_csv(csv_url, csv_output_file):
    """
    Download and unzip the CSV file
    """
    unzipped_csv_file = csv_url.split("/")[-1]

    file_extension = unzipped_csv_file.split(".")[-1]

    if file_extension == "csv":
        wget.download(csv_url, f"../{csv_output_file}")
        return
    elif file_extension == "gz":
        # unzip the file
        try:
            if not os.path.exists(f"../{csv_output_file}"):
                # download .gz file
                wget.download(csv_url, f"../{unzipped_csv_file}")

                with gzip.open(f"../{unzipped_csv_file}", "rb") as f_in:
                    with open(f"../{csv_output_file}", "wb") as f_out:
                        f_out.write(f_in.read())
        except Exception as e:
            print(f"Error unzipping file: {e}")
        finally:
            # remove the .gz file
            if os.path.exists(f"../{unzipped_csv_file}"):
                os.remove(f"../{unzipped_csv_file}")
    else:
        raise Exception("File extension not supported. Please provide a .csv or .csv.gz file")
    

def ingest_data(engine, main_table_name, zones_table_name, csv_output_file):
    """
    Ingest data from CSV to Postgres
    """
    # ## Load Dataframe from CSV
    df = pd.read_csv(f"../{csv_output_file}", nrows=100)


    # ## Load Dataframe from CSV In Batches
    # *To avoid inserting all rows all at the same time*
    df_iter = pd.read_csv(f"../{csv_output_file}", iterator=True, chunksize=100000)

    # df = next(df_iter)
    # df

    # make sure to update datetime columns before creating empty table
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])


    # ## Get Schema for CREATE TABLE (DDL)
    print(pd.io.sql.get_schema(df, name=f"{main_table_name}", con=engine))


    # ## Create Table with Columns But No Data (using `head(n=0)` to avoid inserting data)
    df.head(n=0).to_sql(name=f"{main_table_name}", con=engine, if_exists="replace")

    # ## For Each 100K Chunk of Data, Append To Table
    for i, data_chunk in enumerate(df_iter, 1):
        time_start = time.time()

        # update the datetime columns
        data_chunk["tpep_pickup_datetime"] = pd.to_datetime(data_chunk["tpep_pickup_datetime"])
        data_chunk["tpep_dropoff_datetime"] = pd.to_datetime(data_chunk["tpep_dropoff_datetime"])

        # append the data to the table
        data_chunk.to_sql(name=f"{main_table_name}", con=engine, if_exists="append")

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
    csv_url = params.csv_url
    csv_output_file = "yellow_tripdata_2021-01.csv"


    # ## Connect to Postgres Database via SQLAlchemy
    try:
        engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
        engine.connect()
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return
    
    # ## Download the CSV File
    download_unzip_csv(csv_url, csv_output_file)

    
    # ## Ingest the Data To Postgres
    ingest_data(engine, user, password, host, port, db, main_table_name, csv_output_file)


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
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

    # user
    # password
    # host
    # port
    # database name
    # table name
    # url of the csv file

    parser.add_argument("--user", help="Username for Postgres")
    parser.add_argument("--password", help="Password for Postgres")
    parser.add_argument("--host", help="Hot for Postgres")
    parser.add_argument("--port", help="Port for Postgres")
    parser.add_argument("--db", help="Database name for Postgres")
    parser.add_argument("--table_name", help="Name of the table where we will write the results to")
    parser.add_argument("--csv_url", help="URL of the CSV file to download")

    args = parser.parse_args()

    main(args)








