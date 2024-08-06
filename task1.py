## Creating a new row in the configuration file for each CSV file found in the sample_files directory that is not already listed in the configuration file:
import os
import csv
from datetime import datetime

# Define the config file name
config_file_name = 'config.csv'

# Check if config file exists
if os.path.isfile(config_file_name):
    # Read the file and store the file ID for each file name in a dictionary
    file_name_to_id = {}
    with open(config_file_name, mode='r', newline='') as file:
        reader = csv.DictReader(file)
        for row in reader:
            file_name_to_id[row['File_Name']] = row['File_ID']
else:
    # If config file does not exist, create an empty dictionary
    file_name_to_id = {}

# Define the source directory path and delimiter
source_dir_path = 'sample_files'
delimiter = ','

# Create an empty list for storing file information
file_info = []

# Loop through all files in the source directory
for filename in os.listdir(source_dir_path):
    # Check if the file is a CSV file
    if filename.endswith('.csv'):
        # Check if the file name is already in the file name to ID mapping
        if filename in file_name_to_id:
            continue
        else:
            # Get the next file ID by incrementing the maximum existing file ID
            file_id = str(int(max(file_name_to_id.values(), default=999)) + 1)
        
        # Extract header names from the CSV file
        with open(os.path.join(source_dir_path, filename), mode='r+', newline='') as file:
            reader = csv.reader(file, delimiter=delimiter)
            header_names = next(reader) # Extract header names
            
        # Append file information to the list
        file_info.append({
            'File_ID': file_id,
            'File_Name': filename,
            'DeLimiter': delimiter,
            'Header_Name': ','.join(header_names),
            'Source_File_Path': os.path.join(source_dir_path, filename),
            'Staging_Table_Name': 'stg_' + filename.split('.')[0].lower(),
            'Target_Table_Name': 'raw_' + filename.split('.')[0].lower(),
            'Enable': True,
            'Has_Header': True,
            'Truncate_Flag': False,
            'PK': '',
            'Created_TS': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'Updated_TS': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        
        # Add the file name to ID mapping to the dictionary
        file_name_to_id[filename] = file_id
        
if file_info:
    with open(config_file_name, mode='a', newline='') as file:
        # Get the field names from the first dictionary in the list
        fieldnames = file_info[0].keys()
        # Create a dictionary writer object to write to the config file
        writer = csv.DictWriter(file, fieldnames)
        # If the file is empty, write the header row
        if file.tell() == 0:
            writer.writeheader()
        # Write each row of file information to the config file
        for row in file_info:
            writer.writerow(row)
            
# Updating the header names for each file that is listed in the configuration file:
# Loop through all files in the source directory
for filename in os.listdir(source_dir_path):
    # Check if the file is a CSV file
    if filename.endswith('.csv'):
        # Open the CSV file and extract header names
        with open(os.path.join(source_dir_path, filename), mode='r', newline='') as file:
            reader = csv.reader(file, delimiter=delimiter)
            header_names = next(reader) # Extract header names           
        # Update the config file with the new header names
        with open(config_file_name, mode='r+', newline='') as file:
            
            # Check if the config file is empty
            if os.stat(config_file_name).st_size == 0:
                fieldnames = ['File_ID', 'File_Name', 'DeLimiter', 'Header_Name', 'Source_File_Path', 'Staging_Table_Name', 'Target_Table_Name', 'Enable', 'Has_Header', 'Truncate_Flag', 'Key_Columns', 'Created_TS', 'Updated_TS']
                writer = csv.DictWriter(file, fieldnames)
                writer.writeheader()
            else:
                reader = csv.DictReader(file)
                fieldnames = reader.fieldnames
                rows = list(reader)
              

                for row in rows:
                    if row['File_Name'] == filename:
                        row['Header_Name'] = ','.join(header_names)
                        row['Updated_TS'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                file.seek(0)
                writer = csv.DictWriter(file, fieldnames)
                writer.writeheader()
                writer.writerows(rows)
                file.truncate()
                
