import datetime
import pandas as pd
from bs4 import BeautifulSoup
import requests
import numpy as np
import pypyodbc

def log_progress(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"{timestamp} : {message}\n"

    with open('code_log.txt', 'a') as log_file:
        log_file.write(log_entry)

def extract():
    # URL of the webpage
    url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"

    # Send a GET request to the URL
    response = requests.get(url)

    # Parse the HTML content of the page
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find the required table under the heading 'By market capitalization'
    table = soup.find('span', {'id': 'By_market_capitalization'}).find_next('table')
    print(table)
    # Extract data from the table to a Pandas DataFrame
    data = pd.read_html(str(table), header=0)[0]

    # Convert 'Market cap (US$ billion)' column to strings and remove the last character '\n'
    data['Market cap (US$ billion)'] = data['Market cap (US$ billion)'].astype(str).str.rstrip('\n')

    # Typecast the 'Market cap (US$ billion)' values to float
    data['Market cap (US$ billion)'] = data['Market cap (US$ billion)'].astype(float)

    return data

def transform(data):
    # Read exchange rate CSV file from the local directory and convert to dictionary
    exchange_rate_path = "./exchange_rate.csv"
    exchange_rate_df = pd.read_csv(exchange_rate_path, index_col=0)
    exchange_rate = exchange_rate_df.squeeze().to_dict()

    # Add new columns for MC_GBP_Billion, MC_EUR_Billion, and MC_INR_Billion
    data['MC_GBP_Billion'] = np.round(data['Market cap (US$ billion)'] * exchange_rate['GBP'], 2)
    data['MC_EUR_Billion'] = np.round(data['Market cap (US$ billion)'] * exchange_rate['EUR'], 2)
    data['MC_INR_Billion'] = np.round(data['Market cap (US$ billion)'] * exchange_rate['INR'], 2)

    return data

def run_queries(connection, query):
    # Create a cursor object
    cursor = connection.cursor()

    # Execute the query
    cursor.execute(query)

    # Fetch the results
    results = cursor.fetchall()

    # Print the query statement and results
    print(f"Query statement: {query}")
    print("Query output:")
    for row in results:
        print(row)

    # Close the cursor
    cursor.close()

def load_to_sql_server(connection_string, table_name, data):
    # Establish a connection to SQL Server
    conn = pypyodbc.connect(connection_string,autocommit=True)
    print("\n------------------------Connected !----------------------------\n")
    # Create a cursor object
    cursor = conn.cursor()

    # Create the table if it doesn't exist
    # Check if the table exists
    cursor.execute(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'")
    table_exists = cursor.fetchone()[0]
    print(table_exists)

    if not table_exists:
        # Table doesn't exist, create it
        cursor.execute(f"CREATE TABLE {table_name} ("
                       "Name NVARCHAR(MAX), "
                       "MC_USD_Billion FLOAT, "
                       "MC_GBP_Billion FLOAT, "
                       "MC_EUR_Billion FLOAT, "
                       "MC_INR_Billion FLOAT"
                       ")")
    # Insert data into the table
    for index, row in data.iterrows():
        # Use a tuple to pass parameters
        values_tuple = (row['Bank name'], row['Market cap (US$ billion)'],
                        row['MC_GBP_Billion'], row['MC_EUR_Billion'], row['MC_INR_Billion'])

        cursor.execute(f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?)", values_tuple)


    # Commit the changes and close the cursor
    conn.commit()
    cursor.close()

SERVER_NAME='SQLSERVERVM\SQLEXPRESS'
DRIVER_NAME='SQL SERVER'
DATABASE_NAME = 'test'
sql_server_connection_string = f"""
    DRIVER={{{DRIVER_NAME}}};
    SERVER={SERVER_NAME};
    DATABASE={DATABASE_NAME};
    Trusted_Connection=yes;
"""


# Create a SQL Server connection
#conn = odbc.connect(conn_str,autocommit=True)

# Create a cursor from the connection


# # Define the RESTORE DATABASE statement
# restore_db_stmt = (
#     f"RESTORE DATABASE [{database_name}] "
#     f"FROM DISK='{backup_file_path}' "
# )

# cursor = conn.cursor()

# SQL_command = """
#                 RESTORE DATABASE [MyDatabase]
#                 FROM DISK = N'C:\\Users\\AdventureWorksDW2019.bak' WITH
#                       REPLACE
#                 ,     STATS = 10
#               """

# cursor.execute(SQL_command)
# print(cursor)

# while cursor.nextset():
#     pass 


# Close the cursor and connection
# cursor.close()
# conn.close()

# print("Restored !")


# Initial log entry
log_progress("Preliminaries complete. Initiating ETL process")




# Call the extract() function and print the returning data frame
extracted_data = extract()
print(extracted_data)

# Log entry
log_progress("Data extraction complete. Initiating Transformation process")


# Call the transform() function and print the returning data frame
transformed_data = transform(extracted_data)
print(transformed_data)

# Log entry
log_progress("Data transformation complete. Initiating Loading process")


# SQL Server connection string
#sql_server_connection_string = "Driver={SQL Server};Server=YourServerName;Database=YourDatabaseName;UID=YourUsername;PWD=YourPassword"

# Call the load_to_sql_server() function
load_to_sql_server(sql_server_connection_string, "Largest_banks", transformed_data)

# Log entry
log_progress("Data loaded to SQL Server as a table. Executing queries")


# Execute query to print the contents of the entire table
query_1 = "SELECT * FROM Largest_banks"
run_queries(pypyodbc.connect(sql_server_connection_string), query_1)

# Execute query to print the average market capitalization in Billion USD
query_2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_queries(pypyodbc.connect(sql_server_connection_string), query_2)

# Execute query to print only the names of the top 5 banks
query_3 = "SELECT TOP 5 Name FROM Largest_banks"
run_queries(pypyodbc.connect(sql_server_connection_string), query_3)

# Log entry
log_progress("Process Complete")


