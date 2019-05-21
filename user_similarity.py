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


# credentials = service_account.Credentials.from_service_account_file(
#     'reddit-239517_service_key.json')
# project_id = 'reddit'
# client = bigquery.Client(credentials= credentials,project=project_id)

# client = bigquery.Client()

def connect_db():
    '''
    Connects to datset and returns bigquery client and dataset reference
    '''
    credentials = service_account.Credentials.from_service_account_file( \
        'reddit-239517_service_key.json')
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
        location="US",)  # API request - starts the query
    for row in  query_job:
        yield row

query = (
    "SELECT author, subreddit_id, n_comments "
    "FROM `test1.subredditMembershipv2`"
    "WHERE author in ('Useless_Patrick','TheScapeQuest')"
)

# API request - starts the query
# query_job = client.query(
#     query,
#     location="US",  # Location must match that of the dataset(s) referenced in the query.
# )  
# API request - fetches results
# Row values can be accessed by field name or index


# query_job = query_generator(query)

# print(type(query_job))

# query_job_list = list()

# for row in query_job:
#     row = list(row)
#     row.append(1)
#     query_job_list.append(tuple(i for i in row))

# # Convert output from QueryJob (list of tuples) into Spark RDD
# user_sub = sc.parallelize(query_job_list)

# Test RDD Data
user_sub_count = sc.parallelize([(1, 1), (1, 1), (1, 1), (2, 1), (2, 1), (3, 1), (4, 1), (5, 1)])
sub_user = sc.parallelize([('a',[2]), ('a',[1]), ('b',[1]), ('c',[3]), ('c',[4]), ('c',[5]), ('c',[1]), ('c',[2])])
print('sub_user')
print(sub_user.collect())
sub_members = sub_user.reduceByKey(lambda a, b: a+b)
print('sub_members')
print(sub_members.collect())

# user_sub_count = user_sub.map(lambda x: (x[0], 1))
# sub_user = user_sub.map(lambda x: (x[1], [x[0]]))
# sub_members = sub_user.reduceByKey(lambda a, b: a+b)

# For each subreddit membership list, make tuples of all pairs of users, and map 1 as value
# The 1 can be replaced as the weighted IDF constant (later)
user_pairs = sub_members\
    .map(lambda x: [(x[1][i], x[1][j]) for i in range(len(x[1])) for j in range(i+1, len(x[1]))])\
    .flatMap(lambda x: x)\
    .map(lambda x: (x, 1))


# Sort the user-user pairs so they'll group together
user_pairs = user_pairs.map(lambda x: ((sorted(x[0])[0], sorted(x[0])[1]), x[1]))
print('user_pairs')
print(user_pairs.collect())

# Number of shared subreddits by user
# Numerator of the cosine similarity metric (dot product of User1,User2 vectors)
shared_subs_by_user = user_pairs.reduceByKey(lambda a, b: a+b)
print('shared_subs_by_user')
print(shared_subs_by_user.collect())

# Norm of each user's subreddit vector 
user_norm = user_sub_count.reduceByKey(lambda s1, s2: s1+s2).map(lambda x: (x[0], x[1]**0.5))
print('user_norm')
print(user_norm.collect())

