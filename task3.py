import pandas as pd
import csv
import os
import psycopg2
import pyodbc
import pymssql
import mysql.connector

# Read database details from configuration file
with open('db_config_file.csv') as f:
    lines = f.readlines()
    db_type = lines[0].split(',')[1].strip()
    db_host = lines[1].split(',')[1].strip()
    db_name = lines[2].split(',')[1].strip()
    db_user = lines[3].split(',')[1].strip()
    db_password = lines[4].split(',')[1].strip()
    db_port = lines[5].split(',')[1].strip()
    db_schema_raw = lines[7].split(',')[1].strip()

file_pk_map = {}
with open('config.csv') as c:
    reader = csv.reader(c)
    next(reader)  # skip header row
    for row in reader:
        filename = row[1].strip()
        pk_cols = row[10].split(',')
        pk_cols_str = ",".join(pk_cols)
        file_pk_map[filename] = pk_cols_str

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

raw_table_prefix = 'raw_'

cur = conn.cursor()
for filename, pk_cols in file_pk_map.items():
    raw_table_name = raw_table_prefix + os.path.splitext(filename)[0].lower()
    cur.execute(f"SELECT constraint_name FROM information_schema.table_constraints WHERE table_name='{raw_table_name}' and constraint_type='PRIMARY KEY'")
    constraint = cur.fetchone()
    if constraint is not None:
        cur.execute(f"ALTER TABLE {db_schema_raw}.{raw_table_name} DROP CONSTRAINT {constraint[0]}")
    if pk_cols:
        cur.execute(f"ALTER TABLE {db_schema_raw}.{raw_table_name} ADD PRIMARY KEY ({pk_cols})")
    conn.commit()

# Close the database connections
conn.close()
print(f"Primary Keys Assigned Successfully")