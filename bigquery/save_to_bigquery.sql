from google.cloud import bigquery

# TODO(developer): Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the destination table.
table_id = "my-first-gcp-project-271812.test"

job_config = bigquery.QueryJobConfig(destination=table_id)

sql = """
    SELECT name, gender, sum(number) as total
    from `bigquery-public-data.usa_names.usa_1910_current`
    group by name,gender
    order by total DESC
    limit 100
"""

# Start the query, passing in the extra configuration.
query_job = client.query(sql, job_config=job_config)  # Make an API request.
query_job.result()  # Wait for the job to complete.

print("Query results loaded to the table {}".format(table_id))