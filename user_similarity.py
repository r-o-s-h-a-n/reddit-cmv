import sys
import os
import datetime
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


# credentials = service_account.Credentials.from_service_account_file(
#     'reddit-239517_service_key.json')
# project_id = 'reddit'
# client = bigquery.Client(credentials= credentials,project=project_id)

# client = bigquery.Client()

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


query = (
    "SELECT author, subreddit_id, n_comments "
    "FROM `test1.subredditMembershipv2`"
    "LIMIT 1000"
)

 
# API request - fetches results
# Row values can be accessed by field name or index
query_job = query_generator(query)


# Writes QueryJob rows to a list to parallelize into Spark RDD
query_job_list = list()
for row in query_job:
    row = list(row)
    row.append(1)
    query_job_list.append(tuple(i for i in row))

# Convert output from QueryJob (list of tuples) into Spark RDD
user_sub = sc.parallelize(query_job_list)

# Test RDD Data
# user_sub_count = sc.parallelize([(1, 1), (1, 1), (1, 1), (2, 1), (2, 1), (3, 1), (4, 1), (5, 1)])
# sub_user = sc.parallelize([('a',[2]), ('a',[1]), ('b',[1]), ('c',[3]), ('c',[4]), ('c',[5]), ('c',[1]), ('c',[2])])
# print('sub_user')
# print(sub_user.collect())
# sub_members = sub_user.reduceByKey(lambda a, b: a+b)
# print('sub_members complete')
# print(sub_members.collect())


user_sub_count = user_sub.map(lambda x: (x[0], 1))
sub_user = user_sub.map(lambda x: (x[1], [x[0]]))
sub_members = sub_user.reduceByKey(lambda a, b: a+b)
print('sub_members complete: '+str(datetime.datetime.now()))


# For each subreddit membership list, make tuples of all pairs of users, and map 1 as value
# The 1 can be replaced as the weighted IDF constant (later)
user_pairs = sub_members\
    .map(lambda x: [(x[1][i], x[1][j]) for i in range(len(x[1])) for j in range(i+1, len(x[1]))])\
    .flatMap(lambda x: x)\
    .map(lambda x: (x, 1))


# Sort the user-user pairs so they'll group together
user_pairs = user_pairs.map(lambda x: ((sorted(x[0])[0], sorted(x[0])[1]), x[1]))
print('user_pairs complete: '+str(datetime.datetime.now()))
# print(user_pairs.collect())

# Number of shared subreddits by user
# Numerator of the cosine similarity metric (dot product of User1,User2 vectors)
shared_subs_by_user_pair = user_pairs.reduceByKey(lambda a, b: a+b).map(lambda x: (x[0][0], x[0][1], x[1]))
print(shared_subs_by_user_pair.collect())
write_to_table('sharedSubsUserGraph', shared_subs_by_user_pair.collect())
print('shared_subs_by_user_pair complete: '+str(datetime.datetime.now()))


# Norm of each user's subreddit vector 
user_norm = user_sub_count.reduceByKey(lambda s1, s2: s1+s2).map(lambda x: (x[0], x[1]**0.5))
user_norm_lookup = user_norm.collectAsMap()
print('user_norm complete: '+str(datetime.datetime.now()))
# print(user_norm.collect())

# Denominator of the cosine similarity distance, norm(user1)*norm(user2), for each user pair
cos_sim_user_pair = shared_subs_by_user_pair\
    .map(lambda x: (x[0], x[1], 1-x[2]/(user_norm_lookup[x[0]]*user_norm_lookup[x[1]])))
# write_to_table('cosDistUserPair', cos_sim_user_pair.collect())
print('cos_sim_user_pair complete:'+str(datetime.datetime.now()))
# print(cos_sim_user_pair.collect())
