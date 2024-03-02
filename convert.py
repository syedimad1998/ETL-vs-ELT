import pyodbc
import pypyodbc as odbc

# Define the SQL Server connection string
# conn_str = (
#     r'DRIVER={SQL Server};'
#     r'SERVER=MSSQL$SQLEXPRESS;'
#     r'DATABASE=master;'
#     r'Trusted_Connection=yes;'
# )
# Trusted Connection to Named Instance
# connection = pyodbc.connect('DRIVER={SQL Server};SERVER=MSSQL$SQLEXPRESS;DATABASE=SampleDB;Trusted_Connection=yes;')
SERVER_NAME='SQLSERVERVM\SQLEXPRESS'
DRIVER_NAME='SQL SERVER'
DATABASE_NAME = 'test'
conn_str = f"""
    DRIVER={{{DRIVER_NAME}}};
    SERVER={SERVER_NAME};
    DATABASE={DATABASE_NAME};
    Trusted_Connection=yes;
"""

# Define the database name and backup file path
database_name = 'Migration'
backup_file_path = r'C:\Users\imad\Downloads\AdventureWorksDW2019.bak'

# Create a SQL Server connection
conn = odbc.connect(conn_str,autocommit=True)

print("Connected !")

print(conn)
# Create a cursor from the connection


# # Define the RESTORE DATABASE statement
# restore_db_stmt = (
#     f"RESTORE DATABASE [{database_name}] "
#     f"FROM DISK='{backup_file_path}' "
# )

cursor = conn.cursor()

SQL_command = """
                RESTORE DATABASE [MyDatabase]
                FROM DISK = N'C:\\Users\\AdventureWorksDW2019.bak' WITH
                      REPLACE
                ,     STATS = 10
              """

cursor.execute(SQL_command)
print(cursor)

while cursor.nextset():
    pass 


# Close the cursor and connection
cursor.close()
conn.close()

print("Restored !")

print("Completed")

print("Ok tata bye")