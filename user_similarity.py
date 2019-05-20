from google.cloud import bigquery

from google.oauth2 import service_account


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
    "SELECT commenter, post_id "
    "FROM `test1.deltaAwarded`"
    "WHERE commenter like 'R120Tunisia'"
)
query_job = client.query(
    query,
    # Location must match that of the dataset(s) referenced in the query.
    location="US",
)  # API request - starts the query

for row in query_job:  # API request - fetches results
    # Row values can be accessed by field name or index
    print(row)