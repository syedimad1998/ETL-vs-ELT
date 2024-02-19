import datetime
import pandas as pd
from bs4 import BeautifulSoup
import requests
from sqlalchemy import create_engine
from io import StringIO

# Replace these with your actual SQL Server details
SERVER = 'SQLSERVERVM\SQLEXPRESS' # execute this query on sql server : Select @@Servername 
DATABASE = 'test'
STAGING_TABLE_NAME = 'staging_banks'

def log_progress(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"{timestamp} : {message}\n"
    print(log_entry)  # Printing log for demonstration purposes

def extract():
    url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('span', {'id': 'By_market_capitalization'}).find_next('table')
    data = pd.read_html(str(table), header=0)[0]
    data['Market cap (US$ billion)'] = data['Market cap (US$ billion)'].astype(str).str.rstrip('\n')
    data['Market cap (US$ billion)'] = data['Market cap (US$ billion)'].astype(float)
    data = pd.read_html(StringIO(str(table)), header=0)[0]
    return data

def load_to_sql(engine, data, table_name):
    data.to_sql(table_name, con=engine, index=False, if_exists='replace')

def get_exchange_rates(csv_path):
    exchange_rates = pd.read_csv(csv_path)
    return exchange_rates.set_index('Currency')['Rate'].to_dict()

def transform(data, exchange_rates):
    # Convert market cap to different currencies based on the exchange rate
    for currency, rate in exchange_rates.items():
        column_name = f"Market cap ({currency} billion)"
        data[column_name] = data['Market cap (US$ billion)'] * rate
    return data

def load_transformed_to_sql(engine, data, transformed_table_name):
    data.to_sql(transformed_table_name, con=engine, index=False, if_exists='replace')

# Inside your main function, after transforming the data, load it to SQL
def main():
    log_progress("Starting ELT Process")

    # Extract
    extracted_data = extract()
    log_progress("Data extracted from Wikipedia")

    # Setup database connection
    connection_string = f"mssql+pyodbc://{SERVER}/{DATABASE}?driver=ODBC+Driver+17+for+SQL+Server"
    engine = create_engine(connection_string)
    
    # Load (Raw Data)
    load_to_sql(engine, extracted_data, STAGING_TABLE_NAME)
    log_progress("Raw data loaded into SQL Server")

    # Read the data from SQL Server into a DataFrame back again
    with engine.connect() as connection:
        query = f"SELECT * FROM {STAGING_TABLE_NAME}"
        raw_data_to_be_transformed = pd.read_sql_query(query, connection)

    # Get exchange rates
    exchange_rates_path = 'C:\\Users\\imad\\Desktop\\Data_Eng\\exchange_rate.csv'  # Replace with the actual path to your CSV
    exchange_rates = get_exchange_rates(exchange_rates_path)
    log_progress("Exchange rates loaded from CSV")

    # Transform
    transformed_data = transform(raw_data_to_be_transformed, exchange_rates)
    log_progress("Data transformed with exchange rates from CSV")

    # Load (Transformed Data)
    TRANSFORMED_TABLE_NAME = 'transformed_banks'  # You can define a new table name for transformed data
    #load_transformed_to_sql(engine, transformed_data, TRANSFORMED_TABLE_NAME)
    load_transformed_to_sql(engine, transformed_data, TRANSFORMED_TABLE_NAME)
    log_progress(f"Transformed data loaded into SQL Server in table {TRANSFORMED_TABLE_NAME}")

    # The transformed data is also saved to CSV for local use
    path_to_desktop = './transformed_data.csv'
    transformed_data.to_csv(path_to_desktop, index=False)
    log_progress(f"Transformed data saved to CSV at {path_to_desktop}")

main()

#this is giving me a all the columns in the sql and also in excel file too