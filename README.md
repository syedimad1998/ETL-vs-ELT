# ETL-vs-ELT
This repository provides two Python scripts illustrating the implementation of ETL (Extract, Transform, Load) and ELT (Extract, Load, Transform) processes for data integration. The scripts use Python along with relevant libraries for data manipulation and SQL Server interaction.

## ETL Process:
The ETL script demonstrates a traditional ETL workflow with the following key steps:

Extraction: Initial log entry, data extraction from a source (illustrated with a sample extract() function).
Transformation: Log entry, transformation of the extracted data using a transform() function.
Loading: Loading the transformed data into a SQL Server table (load_to_sql_server() function).

## ELT Process:
The ELT script showcases an ELT approach with the following steps:

Extraction: Initial log entry, data extraction from a source using a extract() function.
Loading (Raw Data): Loading the extracted data into a staging table in SQL Server.
Transformation: Reading the raw data from SQL Server, obtaining exchange rates, and transforming the data accordingly.
Loading (Transformed Data): Loading the transformed data into another SQL Server table and saving it to a local CSV file.
Feel free to explore and compare these two approaches to understand the differences between ETL and ELT in the context of data integration.
