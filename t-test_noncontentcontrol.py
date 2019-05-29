import sys
import os
import datetime
from pyspark import SparkConf, SparkContext
from google.cloud import bigquery
from google.oauth2 import service_account
import pickle
import pandas as pd
import numpy as np



# # Set Spark Configuration
# cores = '\'local[*]\''
# driver_memory = '10g'
# # network_timeout = '10000000'
# # executor_heartbeat_interval = '1000000'
# pyspark_submit_args = \
#     ' --master ' + cores + \
#     ' --driver-memory ' + driver_memory + \
#     # ' --conf spark.network.timeout=' + network_timeout + \
#     # ' --conf spark.executor.heartbeatInterval=' + executor_heartbeat_interval + \
#     ' pyspark-shell'

# os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args

# conf = SparkConf()
# sc = SparkContext(conf=conf)



def connect_db():
    '''
    Connects to datset and returns bigquery client and dataset reference
    '''
    credentials = service_account.Credentials.from_service_account_file('reddit-239517_service_key.json')
    project_id = 'reddit-239517'
    client = bigquery.Client(credentials= credentials,project=project_id)
    dataset_ref = client.dataset('test1')
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


### Uncomment the below to pull fresh data from BigQuery (otherwise use the pickle file)
# query = (
#     "SELECT full_delta_res.*, cos.cosineDist "
#     "FROM ("
#     "SELECT commenter, op, comment_id, post_id, 1 AS delta_result FROM `test1.deltaAwardedv2` "
#     "UNION ALL "
#     "SELECT commenter, op, comment_id, post_id, 0 AS delta_result FROM `test1.negAwardedv2`"
#     ") AS full_delta_res "
#     "INNER JOIN "
#     "`test1.cosDistUserPairv2` AS cos "
#     "ON (full_delta_res.commenter = cos.user1 AND full_delta_res.op = cos.user2)"
# )

# # API request - fetches results
# # Row values can be accessed by field name or index
# query_job = query_generator(query)

# # Writes QueryJob rows to a list to parallelize into Spark RDD
# query_job_list = list()
# for row in query_job:
#     # row = list(row)
#     query_job_list.append(tuple(i for i in row))

# # Save to pickle so I don't have to keep querying BigQuery for development 
# pickle.dump( query_job_list, open( "query_ttest_noncontentcontrol.p", "wb" ) )

# Load query from pickle file
query_job_list = pickle.load( open( "query_ttest_noncontentcontrol.p", "rb" ) )

# Write raw data to a CSV file
data_df = pd.DataFrame(query_job_list)
data_df.columns = ['commenter', 'op', 'comment_id', 'post_id', 'delta_result', 'cosDist']
data_df.to_csv('ttest_noncontentcontrol.csv', index=False)


pos = np.fromiter([i[5] for i in query_job_list if i[4] == 1], dtype = np.float64)
neg = np.fromiter([i[5] for i in query_job_list if i[4] == 0], dtype = np.float64)

print("Positive Result n:",len(pos))
print("Negative Result n:",len(neg))
print("")
print("Positive Result Mean:",np.mean(pos))
print("Negative Result Mean:",np.mean(neg))
print("")

import statsmodels.api as sm
print("Statsmodel T-Test Results")
print(sm.stats.ttest_ind(pos, neg, usevar='unequal'))

from scipy import stats
print("Scipy T-Test Results")
print(stats.ttest_ind(pos, neg, equal_var=False))
