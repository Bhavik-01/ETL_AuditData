import pandas as pd
import psycopg2
import csv
import os
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
    db_schema_stg = lines[6].split(',')[1].strip()
    db_schema_raw = lines[7].split(',')[1].strip()
    db_schema_temp = lines[8].split(',')[1].strip()
    

# Connect to the testing database
if db_type == 'postgresql':
    conn_test = psycopg2.connect(
        host=db_host,
        dbname=db_name,
        user=db_user,
        password=db_password,
        port=db_port
    )
elif db_type == 'mysql':
    conn_test = mysql.connector.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_password,
        port=db_port
    )
elif db_type == 'sqlserver':
    conn_test = pyodbc.connect(
        "Driver={SQL Server};"
        "Server="+db_host+";"
        "Database="+db_name+";"
        "uid="+db_user+";pwd="+db_password+";"
    )
else:
    raise ValueError(f"Unsupported database type: {db_type}")

cur_test = conn_test.cursor()

cur_test.execute(f"CREATE SCHEMA  IF NOT EXISTS {db_schema_temp};")
conn_test.commit()


# Get the list of all tables in the staging schema
cur_test.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{db_schema_stg}'")
tables = cur_test.fetchall()

column_names = {}


for table in tables:
    table_name = table[0]


 # Giving Table Prefix
raw_table_prefix = 'raw_'
staging_table_prefix = 'stg_'
tmp_table_prefix = 'tmp_'    

with open('config.csv') as c:
    reader = csv.reader(c)
    next(reader)  # skip header row
    for row in reader:
        filename = row[1].strip()
        staging_table_name = staging_table_prefix + os.path.splitext(filename)[0].lower()
        raw_table_name = raw_table_prefix + os.path.splitext(filename)[0].lower()
        tmp_table_name = tmp_table_prefix  + os.path.splitext(filename)[0].lower()
        
        
    # Get the column names of the table
        cur_test.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{raw_table_name}' AND table_schema = '{db_schema_raw}'")
        columns = cur_test.fetchall()
        column_names[table_name] = [col[0] for col in columns]
        
    # (col_names_str1) Only has Column names
        col_names_str1 = ",".join(column_names[table_name])
        
    # (col_names_str) Has Table name as well as Column Names  
        col_names_list = column_names[table_name]
        col_names_str = ', '.join([f"{tmp_table_name}.{col_name}" for col_name in col_names_list])        
     
    
    # Get the primary key columns of the table
        cur_test.execute(f"SELECT column_name FROM information_schema.key_column_usage WHERE table_name = '{raw_table_name}' AND table_schema = '{db_schema_raw}'")
        primary_key_cols = cur_test.fetchall()
        pk_col_names = [pk[0] for pk in primary_key_cols]
    
   
    # Join the primary key columns with the "AND" operator
        pk_col_str = ",".join(pk_col_names)
        
        
        
        # count the number of primary keys
        num_primary_keys = len(pk_col_str.split(","))

# check if there is only one primary key
        num_updated_records = 0
        num_inserted_records = 0
        if num_primary_keys == 1:
            
            removed_rows = []
            
            cur_test.execute(f"""CREATE TABLE IF NOT EXISTS {db_schema_temp}.{tmp_table_name} as  
            (Select * from {db_schema_stg}.{staging_table_name} 
            where {pk_col_str} not in (select {pk_col_str} from {db_schema_stg}.{staging_table_name} 
            GROUP BY {pk_col_str} HAVING COUNT(*) > 1))""")
            conn_test.commit()
            
            removed_query = f"""SELECT * FROM {db_schema_stg}.{staging_table_name} 
            WHERE {pk_col_str} in (select {pk_col_str} from {db_schema_stg}.{staging_table_name} 
            GROUP BY {pk_col_str} HAVING COUNT(*) > 1)"""
            cur_test.execute(removed_query)
            removed_rows = cur_test.fetchall()
            conn_test.commit()
            
                        # Get the current date and time
            now = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

            # Create the path for the archive folder
            archive_path = os.path.join(os.getcwd(), 'archive_log_file', now)

            # Create the archive folder if it doesn't exist
            if not os.path.exists(archive_path):
                os.makedirs(archive_path)

            # Create the path for the log file in the archive folder
            log_file_path = os.path.join(archive_path, 'log_file.csv')

            # Check if file exists and is not empty
            if os.path.isfile(log_file_path) and os.path.getsize(log_file_path) > 0:

                with open(log_file_path, 'a', newline='') as removed_file:
                    writer = csv.writer(removed_file)
                    for row in removed_rows:
                        record = '|'.join(str(x) for x in row) # concatenate all columns with pipe separator
                        writer.writerow([filename, record])
            else:
                # Write header row and open in append mode
                with open(log_file_path, 'w', newline='') as removed_file:
                    header_row = ['File_Name', 'Records']
                    writer = csv.writer(removed_file)
                    writer.writerow(header_row)

                        
                with open(log_file_path, 'a', newline='') as removed_file:
                    writer = csv.writer(removed_file)
                    for row in removed_rows:
                        record = '|'.join(str(x) for x in row) # concatenate all columns with pipe separator
                        writer.writerow([filename, record])
                        
                    
            
        # Insert new records
            cur_test.execute(f"""INSERT INTO {db_schema_raw}.{raw_table_name} ({col_names_str1})
                         SELECT {col_names_str} FROM {db_schema_temp}.{tmp_table_name}
                         left join {db_schema_raw}.{raw_table_name}
                         on {tmp_table_name}.{pk_col_str} = {raw_table_name}.{pk_col_str}
                         WHERE {raw_table_name}.{pk_col_str} is null;""")
            num_inserted_records = cur_test.rowcount
        

    # Generate update columns string
            update_columns = ", ".join([f"{col} = t.{col}" for col in column_names[table_name]])
        

    # Generate column comparison string
            column_comparison = " OR ".join([f"(r.{col} <> t.{col} OR r.{col} IS NULL OR t.{col} IS NULL)" for col in column_names[table_name]])
            
    # Generate SQL query
            sql_query = f"""
            UPDATE {db_schema_raw}.{raw_table_name} AS r
            SET {update_columns}
            FROM {db_schema_temp}.{tmp_table_name} AS t
            WHERE r.{pk_col_str} = t.{pk_col_str} AND ((
              {column_comparison}
            ))
        """
            cur_test.execute(sql_query)
            num_updated_records = cur_test.rowcount
            conn_test.commit()

        elif num_primary_keys >1:
            
            removed_rows = []
            
            cur_test.execute(f"""CREATE TABLE IF NOT EXISTS {db_schema_temp}.{tmp_table_name} as  
            (Select * from {db_schema_stg}.{staging_table_name} 
        where ({pk_col_str}) not in (select {pk_col_str} from {db_schema_stg}.{staging_table_name} 
        GROUP BY ({pk_col_str}) HAVING COUNT(*) > 1))""")
            conn_test.commit()
            
            
            removed_query = f"""SELECT * FROM {db_schema_stg}.{staging_table_name} 
            WHERE ({pk_col_str}) in (select {pk_col_str} from {db_schema_stg}.{staging_table_name} 
            GROUP BY {pk_col_str} HAVING COUNT(*) > 1)"""
            cur_test.execute(removed_query)
            removed_rows = cur_test.fetchall()
            conn_test.commit()
            
            now = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

            # Create the path for the archive folder
            archive_path = os.path.join(os.getcwd(), 'archive_log_file', now)

            # Create the archive folder if it doesn't exist
            if not os.path.exists(archive_path):
                os.makedirs(archive_path)

            # Create the path for the log file in the archive folder
            log_file_path = os.path.join(archive_path, 'log_file.csv')

            # Check if file exists and is not empty
            if os.path.isfile(log_file_path) and os.path.getsize(log_file_path) > 0:
                

                with open(log_file_path, 'a', newline='') as removed_file:
                    writer = csv.writer(removed_file)
                    for row in removed_rows:
                        record = '|'.join(str(x) for x in row) # concatenate all columns with pipe separator
                        writer.writerow([filename, record])
            else:
                # Write header row and open in append mode
                with open(log_file_path, 'w', newline='') as removed_file:
                    header_row = ['File_Name', 'Records']
                    writer = csv.writer(removed_file)
                    writer.writerow(header_row)


                        
                with open(log_file_path, 'a', newline='') as removed_file:
                    writer = csv.writer(removed_file)
                    for row in removed_rows:
                        record = '|'.join(str(x) for x in row) # concatenate all columns with pipe separator
                        writer.writerow([filename, record])
            
            
        # Insert new records
            cur_test.execute(f"""
            INSERT INTO {db_schema_raw}.{raw_table_name} ({col_names_str1})
            SELECT {col_names_str} FROM {db_schema_temp}.{tmp_table_name}
            LEFT JOIN {db_schema_raw}.{raw_table_name}
            ON {' AND '.join([f'{tmp_table_name}.{pk_col}' + f' = {raw_table_name}.{pk_col}' for pk_col in pk_col_str.split(",")])}
            WHERE {raw_table_name}.{pk_col_str.split(",")[0]} IS NULL;
            """)
            num_inserted_records = cur_test.rowcount

    # Generate update columns string
            update_columns = ", ".join([f"{col} = t.{col}" for col in column_names[table_name]])
        

    # Generate column comparison string
            column_comparison = " OR ".join([f"(r.{col} <> t.{col} OR r.{col} IS NULL OR t.{col} IS NULL)" for col in column_names[table_name]])
        
    # Generate SQL query
            sql_query = f"""
            UPDATE {db_schema_raw}.{raw_table_name} AS r
            SET {update_columns}
            FROM {db_schema_temp}.{tmp_table_name} AS t
            WHERE {' AND '.join([f'r.{pk_col}' + f' = t.{pk_col}' for pk_col in pk_col_str.split(",")])}
            AND (
              {column_comparison}
            )
            """
            cur_test.execute(sql_query)
            num_updated_records = cur_test.rowcount
            conn_test.commit()
        else:
    # handle the case where no primary keys are specified
            print("Error: no primary keys specified")
        
        
    # Dropping Temp Tables
        cur_test.execute(f"DROP TABLE IF EXISTS {db_schema_temp}.{tmp_table_name}")
        conn_test.commit()
        

print("Data successfully transferred to raw tables and temporary tables dropped")


# Close the database connections
cur_test.close()
conn_test.close()