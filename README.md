**Automating the Manual Auditing Process.**    
This project designed to automate the management and processing of CSV files within a specified directory. The project ensures that each CSV file is properly tracked and updated within a configuration file. The project consists of several tasks that handle various aspects of this process.

**Description**

The Stock Audit project automates the following tasks:

Creating and updating configuration files for CSV files.    
Processing data within these CSV files.           
Validating the data to ensure accuracy and consistency.          
Generating reports based on the processed data.

**Task 1: Configuration Management.** 

The first task involves creating a new row in the configuration file for each CSV file found in the sample_files directory that is not already listed in the configuration file. Additionally, it updates the header names for each file listed in the configuration file.

**Script: task1.py**    
Steps:
1) **Check for Configuration File:** Check if the configuration file (config.csv) exists. If it does, read the existing file names and their corresponding IDs into a dictionary.   
2) **Process CSV Files**: Loop through all files in the sample_files directory.For each CSV file not already listed in the configuration file, extract its headers and add a new entry to the configuration file.   
3) **Update Header Names:** Update the header names for each file listed in the configuration file.

**Task 2: Data Processing**    
The second task involves reading the data from the CSV files and transforming it into a structured format suitable for further processing.

**Script: task2.py**       
Steps:
1) **Read Configuration File:** Load the configuration file (config.csv) to get the list of CSV files and their metadata.    
2) **Process Each CSV File:** For each CSV file listed in the configuration file:    
- Read the CSV file.   
- Apply transformations to the data based on predefined rules.   
- Store the transformed data in an output directory.

**Task 3: Data Validation**  
The third task involves validating the data within the CSV files to ensure it meets specified criteria such as data types, value ranges, and consistency.

**Script: task3.py**      
Steps:
1) **Read Configuration and Processed Data:** Load the configuration file and read each processed CSV file.  
2) **Validate Data:** For each CSV file:   
- Validate headers.  
- Validate data types and value ranges.  
- Generate validation reports.

**Task 4: Reporting**  
The fourth task involves generating summary reports based on the validated data, such as identifying discrepancies or summarizing key metrics.

**Script: task4.py**     
Steps:
1) **Aggregate Data:** Aggregate data from all validated CSV files.  
2) **Generate Reports:** Create Custom reports based on the Users Requirement 
