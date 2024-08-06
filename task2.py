import pandas as pd
import csv
import os
from dateutil.parser import parse
import psycopg2
import pyodbc
import pymssql
import mysql.connector
import shutil
import datetime

# Read database details from configuration file
with open('db_config_file.csv') as f:
    lines = f.readlines()
    db_type = lines[0].split(',')[1].strip()
    db_host = lines[1].split(',')[1].strip()
    db_name = lines[2].split(',')[1].strip()
    db_user = lines[3].split(',')[1].strip()
    db_password = lines[4].split(',')[1].strip()
    db_port = lines[5].split(',')[1].strip()
    db_schema_staging = lines[6].split(',')[1].strip()
    db_schema_raw = lines[7].split(',')[1].strip()

# Connect to the database based on the type specified in the configuration file
if db_type == 'postgresql':
    conn = psycopg2.connect(
        host=db_host,
        dbname=db_name,
        user=db_user,
        password=db_password,
        port=db_port
    )
elif db_type == 'mysql':
    conn = mysql.connector.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_password,
        port=db_port
    )
elif db_type == 'sqlserver':
    conn = pyodbc.connect(
        "Driver={SQL Server};"
        "Server="+db_host+";"
        "Database="+db_name+";"
        "uid="+db_user+";pwd="+db_password+";"
    )
else:
    raise ValueError(f"Unsupported database type: {db_type}")

# Create staging and raw schemas if they don't already exist
cur = conn.cursor()
cur.execute(f"CREATE SCHEMA IF NOT EXISTS {db_schema_staging}")
cur.execute(f"CREATE SCHEMA IF NOT EXISTS {db_schema_raw}")
conn.commit()

# Map pandas data types to database-specific data types
if db_type == 'postgresql':
    type_map = {'int64': 'integer', 'float64': 'numeric', 'object': 'text', 'bool': 'boolean', 'datetime64[ns]': 'date'}
elif db_type == 'mysql':
    type_map = {'int64': 'int', 'float64': 'decimal', 'object': 'text', 'bool': 'boolean', 'datetime64[ns]': 'date'}
elif db_type == 'sqlserver':
    type_map = {'int64': 'bigint', 'float64': 'numeric', 'object': 'nvarchar(max)', 'bool': 'bit', 'datetime64[ns]': 'date'}

else:
    raise ValueError(f"Unsupported database type: {db_type}")

# Folder details
folder_path = 'sample_files'
staging_table_prefix = 'stg_'
raw_table_prefix = 'raw_'

# Loop over all CSV files in the folder
for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        # CSV file details
        csv_file = os.path.join(folder_path, filename)
        staging_table_name = staging_table_prefix + os.path.splitext(filename)[0].lower()
        raw_table_name = raw_table_prefix + os.path.splitext(filename)[0].lower()
        
        # Load the CSV file into a pandas dataframe
        df = pd.read_csv(csv_file, encoding='ISO-8859-1')
        
        # Loop over the columns in the dataframe
        for col in df.columns:
            # Check if the column dtype is object and try to parse it as datetime
            if df[col].dtype == 'object':
                try:
                    df[col] = pd.to_datetime(df[col])
                except ValueError:
                    # If it can't be parsed as datetime, ignore it
                    pass

        # Get the appropriate data types for each column in the dataframe
        data_types = [type_map[str(dtype)] for dtype in df.dtypes]
        
        # Create a table with the appropriate data types in staging schema
        create_raw_table_query_staging = f"CREATE TABLE IF NOT EXISTS {db_schema_staging}.{staging_table_name} ({', '.join([f'{col} {data_types[i].lower()}' for i, col in enumerate(df.columns)])});"
        cur.execute(create_raw_table_query_staging)
        conn.commit()
        
        # Create a table with the appropriate data types in raw schema
        create_raw_table_query_raw = f"CREATE TABLE IF NOT EXISTS {db_schema_raw}.{raw_table_name} ({', '.join([f'{col} {data_types[i].lower()}' for i, col in enumerate(df.columns)])});"
        cur.execute(create_raw_table_query_raw)
        conn.commit()
        
        # Truncate staging table
        cur.execute(f"TRUNCATE TABLE {db_schema_staging}.{staging_table_name}")
        conn.commit()
        
        # Open CSV file and read data
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # skip header row
            data = [tuple(row) for row in reader] # The data is being Read row-wise.
            
        # Insert data into staging table
        placeholders = ', '.join(['%s'] * len(data[0]))
        insert_query = f"INSERT INTO {db_schema_staging}.{staging_table_name} VALUES ({placeholders})"
        cur.executemany(insert_query, data)
        conn.commit()
        
# Define the source and destination folders
source_folder = 'sample_files'
destination_folder = 'Archive_Files'

# Get the current date in the format yyyy-mm-dd
date_str = datetime.date.today().strftime("%Y-%m-%d")

# Create a subdirectory within the archive folder with the current date
archive_subfolder = os.path.join(destination_folder, date_str)
os.makedirs(archive_subfolder, exist_ok=True)

# Loop through all the files in the source folder
for file_name in os.listdir(source_folder):
    # Check if the file is a text file
    if file_name.endswith(".csv"):
        # Construct the full path of the source file
        source_file = os.path.join(source_folder, file_name)

        # Construct the full path of the destination file
        destination_file = os.path.join(archive_subfolder, file_name)

        # Move the file to the archive folder
        shutil.move(source_file, destination_file)
        
        
print(f"All CSV files in {source_folder} have been moved to {destination_folder}")

        
# Close the database connections
conn.close()
print(f"All CSV files in {folder_path} have been loaded into {db_type}")