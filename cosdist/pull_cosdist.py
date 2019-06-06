import sys
import os
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account


def connect_db():
    '''
    Connects to datset and returns bigquery client and dataset reference
    '''
    credentials = service_account.Credentials.from_service_account_file('../reddit-239517_service_key.json')
    project_id = 'reddit-239517'
    client = bigquery.Client(credentials= credentials,project=project_id)
    dataset_ref = client.dataset('cosDist')
    return client, dataset_ref

def query_generator(sql_query):
    '''
    Queries a table and returns results
    '''
    client, dataset = connect_db()
    query_job = client.query(
        sql_query,
        location='US')  # API request - starts the query
    for row in query_job:
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

def get_table(table_name):
    '''
    Connects to dataset and returns bigquery client and table reference
    '''
    client, dataset_ref = connect_db()
    table_ref = dataset_ref.table(table_name)
    table = client.get_table(table_ref)
    return client, table




 
# # API request - fetches results
# # Row values can be accessed by field name or index
# query_job = query_generator(query)

# # Writes QueryJob rows to a list to parallelize into Spark RDD
# query_job_list = list()
# for row in query_job:
#     # row = list(row)
#     query_job_list.append(tuple(i for i in row))


t = 0
with open('costdist_testfile.csv', 'r') as f:
    for i, line in enumerate(f):             
        if i == 0:
            continue
        line = line.strip().split(',')

        op_name = line[0]
        challenger_name = line[2]
        dt_object = datetime.fromtimestamp(int(float(line[4])))
        ym_str = str(dt_object.year)+'_'+str(dt_object.month).zfill(2)

        query_tosend = (
            "SELECT cosineDist "
            "FROM `cosDist."+ym_str+"` as cd "
            "WHERE cd.user1 = '"+op_name+"' AND cd.user2 = '"+challenger_name+"'"
        )
        print(query_tosend)
        query_job = query(query_tosend)

        if len(query_job) == 1:
            print(query_job[0][0])
        elif(len(query_job) > 1):
            print('Dupes')
        else:
            print('None')

        t += 1
        if t == 10:
            break



