# Objectives:

# Write a function to extract the tabular information from the given URL under the heading By Market Capitalization, and save it to a data frame.
# Write a function to transform the data frame by adding columns for Market Capitalization in GBP, EUR, and INR, rounded to 2 decimal places, based on the exchange rate information shared as a CSV file.
# Write a function to load the transformed data frame to an output CSV file.
# Write a function to load the transformed data frame to an SQL database server as a table.
# Write a function to run queries on the database table.
# Run the following queries on the database table:
# a. Extract the information for the London office, that is Name and MC_GBP_Billion
# b. Extract the information for the Berlin office, that is Name and MC_EUR_Billion
# c. Extract the information for New Delhi office, that is Name and MC_INR_Billion
# Write a function to log the progress of the code.
# While executing the data initialization commands and function calls, maintain appropriate log entries.

import pandas as pd
from bs4 import BeautifulSoup
import sqlite3
import requests
from datetime import datetime
import numpy as np 


def extract(url, table_attribs):
    '''extract takes the url where data will be extracted from and 
    the list of attributes to be extracted as parameter '''
    # initiating an empty dataframe for data to be stored and then returned
    df = pd.DataFrame(columns=table_attribs)
    # creating a page object from the provided url
    page = requests.get(url)
    # using BeautifulSoup to parse the data from the page object
    data = BeautifulSoup(page.text, 'html.parser')
    # search and find all table bodys in the data
    tables = data.find_all('tbody')
    # in the second table find all table rows in the first table on the webpage
    rows = tables[0].find_all('tr')
    # for every row
    for row in rows:
        # find all the table datas and store them as colums
        col = row.find_all('td')  
        # as long as a colum exists     
        if len(col) != 0 :
            # for the second colum find the data with a hyperlink and is not empty
            if col[1].find('a') is not None :
                # store the data in a temporary dict object
                data_dict = {'Name':col[1].text.strip(), 'MC_USD_Billion':col[2].contents[0].strip()}
                # convert the dictionary to a temporary dataframe
                temp_df = pd.DataFrame(data_dict, index=[0])
                # add the dataframe to the existing dataframe
                df = pd.concat([df, temp_df],ignore_index=True)
    return df


def transform(df, file_to_read):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    # using pandas read the csv file
    exchange_rate_df = pd.read_csv(file_to_read)
    # create a dict object to read currency type as key
    exchange_rate = exchange_rate_df.set_index('Currency').to_dict()['Rate']
    # start by converting the data to a float
    df['MC_USD_Billion'] = [np.round(float(x)) for x in df['MC_USD_Billion']]
    # create the new converted columns
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]

    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)


def load_to_db(df, table_name, sql_connection):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection,if_exists='replace',index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(pd.read_sql(query_statement, sql_connection))

def log_progress(message, log_file):
    ''' This function logs the mentioned message at a given stage of the code 
    execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 


''' Here is defined the required entities and the relevant functions 
are called in the correct order to complete the project. '''

url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']
csv_path = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = './code_log.txt'
file_to_read = '/workspaces/codespaces-blank/exchange_rate.csv'
log_progress('Starting ETL Process', log_file)
log_progress('Starting Extract process from website', log_file)
df = extract(url,table_attribs)
log_progress('Extraction from website has ended', log_file)

log_progress('Starting transformation, converting data and adding as columns', log_file)
df = transform(df,file_to_read)
log_progress('Transformation has ended', log_file)

log_progress('Starting Load to CSV file', log_file)
load_to_csv(df,csv_path)
log_progress('Loading has ended', log_file)

log_progress('Start connecting to database', log_file)
conn = sqlite3.connect(db_name)
log_progress('Starting load to database', log_file)
load_to_db(df, table_name, conn)
log_progress('Loading to database ended', log_file)

query_stmt = f'SELECT * FROM {table_name}'
query_stmt2 = f'SELECT AVG(MC_GBP_Billion) FROM {table_name}'
query_stmt3 = f'SELECT Name from {table_name} LIMIT 5'
log_progress('Running quieries on database', log_file)
run_query(query_stmt, conn)
run_query(query_stmt2, conn)
run_query(query_stmt3, conn)
log_progress('Running quieries on database has ended', log_file)

log_progress('Closing database connection', log_file)
conn.close()
log_progress('Connection to database is closed', log_file)
log_progress('ETL process has ended', log_file)


''' Expected output below'''

#  Name  MC_USD_Billion  MC_EUR_Billion  MC_GBP_Billion  MC_INR_Billion
# 0                           JPMorgan Chase           433.0          402.69           346.4        35917.35
# 1                          Bank of America           232.0          215.76           185.6        19244.40
# 2  Industrial and Commercial Bank of China           195.0          181.35           156.0        16175.25
# 3               Agricultural Bank of China           161.0          149.73           128.8        13354.95
# 4                                HDFC Bank           158.0          146.94           126.4        13106.10
# 5                              Wells Fargo           156.0          145.08           124.8        12940.20
# 6                        HSBC Holdings PLC           149.0          138.57           119.2        12359.55
# 7                           Morgan Stanley           141.0          131.13           112.8        11695.95
# 8                  China Construction Bank           140.0          130.20           112.0        11613.00
# 9                            Bank of China           137.0          127.41           109.6        11364.15
#    AVG(MC_GBP_Billion)
# 0               152.16
#                                       Name
# 0                           JPMorgan Chase
# 1                          Bank of America
# 2  Industrial and Commercial Bank of China
# 3               Agricultural Bank of China
# 4                                HDFC Bank