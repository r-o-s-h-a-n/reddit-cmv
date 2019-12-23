from google.cloud import bigquery
from google.oauth2 import service_account
import os


def write_to_table(table_name, rows_to_insert):
    '''
    Writes the rows to the table

    Arguments:
        table_name - str, name of table in test1 datset
        rows_to_insert - list of tuples with number of elements in tuple equal to number of columns in table_name
    Returns
        nothing, writes to table
    '''
    client, table = get_table(table_name)
    errors = client.insert_rows(table, rows_to_insert)  # API request
    assert errors == []
    return

def query_and_write(sql_query, write_table):
    '''
    Queries a table and writes results to given table name
    '''
    results = query(sql_query)
    write_to_table(write_table, results)

def query_generator(sql_query):
    '''
    Queries a table and returns results
    '''
    client, dataset = connect_db()
    query_job = client.query(
        sql_query,
        location="US",)  # API request - starts the query
    for row in  query_job:
        yield row

def query(sql_query):
    '''
    Queries a table and returns results
    '''
    client, dataset = connect_db()
    query_job = client.query(
        sql_query,
        location="US",)  # API request - starts the query
    return [row for row in query_job]

def connect_db():
    '''
    Connects to datset and returns bigquery client and dataset reference
    '''
    credentials = service_account.Credentials.from_service_account_file( \
        os.environ['BQ_SERVICE_KEY'])
    project_id = 'reddit-239517'
    client = bigquery.Client(credentials= credentials,project=project_id)
    dataset_ref = client.dataset('test1')
    return client, dataset_ref

def get_table(table_name):
    '''
    Connects to dataset and returns bigquery client and table reference
    '''
    client, dataset_ref = connect_db()
    table_ref = dataset_ref.table(table_name)
    table = client.get_table(table_ref)
    return client, table


if __name__=='__main__':
    rows1  = [('roshan',25),('bill',30)]
    write_to_table('deleteme', rows1)

    query1 = (
        "SELECT commenter, post_id "
        "FROM `test1.deltaAwarded`"
        "WHERE commenter like 'R120Tunisia'"
    )
    query2 = (
        "SELECT * "
        "FROM `test1.deleteme` "
    )
    print(query(query2))




    query1 = (
        "CREATE TABLE `test1.deleteme` ("
        "bloopy VARCHAR(20)"
        "create_date DATE"
    )


    query2 = (
        "INSERT INTO TABLE `test1.deleteme` ("
        " `blah` "
        " `2011-01-01` "
    )


    query3 = (
        "SELECT name FROM `bigquery-public-data.usa_names.usa_1910_2013` "
        'WHERE state = "TX" '
        "LIMIT 100"
    )