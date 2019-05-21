import sys
import os
from pyspark import SparkConf, SparkContext
from google.cloud import bigquery
from google.oauth2 import service_account

# Set custom amount of memory/cores to use
cores = '\'local[*]\''
# memory = '10g'
# pyspark_submit_args = ' --master ' + cores + ' --driver-memory ' + memory + ' pyspark-shell'
pyspark_submit_args = ' --master ' + cores + ' pyspark-shell'
os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args

conf = SparkConf()
sc = SparkContext(conf=conf)



credentials = service_account.Credentials.from_service_account_file(
    'reddit-239517_service_key.json')
project_id = 'reddit'
client = bigquery.Client(credentials= credentials,project=project_id)


client = bigquery.Client()

# query = (
#     "SELECT name FROM `bigquery-public-data.usa_names.usa_1910_2013` "
#     'WHERE state = "TX" '
#     "LIMIT 100"
# )

query = (
    "SELECT author, subreddit_id, n_comments "
    "FROM `test1.subredditMembershipv2`"
    "WHERE author like 'Useless_Patrick'"
)

# API request - starts the query
query_job = client.query(
    query,
    location="US",  # Location must match that of the dataset(s) referenced in the query.
)  
# API request - fetches results
# Row values can be accessed by field name or index
print(type(query_job))

for row in query_job:
    print(row)
    print(row[0])

bucket_name = 'geoffreyli16'
project = 'reddit-cmv'
dataset_id = 'test1'
table_id = 'subredditMembershipv2'
destination_uri = "gs://{}/{}".format(bucket_name, "shakespeare.csv")
dataset_ref = client.dataset(dataset_id, project=project)
table_ref = dataset_ref.table(table_id)

# extract_job = client.extract_table(
#     table_ref,
#     destination_uri,
#     # Location must match that of the source table.
#     location="US",
# )  # API request
# extract_job.result()  # Waits for job to complete.

print(
    "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
)



# scquery = sc.parallelize(query_job)
# print(scquery.collect())