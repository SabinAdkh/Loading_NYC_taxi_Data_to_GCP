import Data_Loaders as DL
import Create_Schema as cs
from google.cloud import bigquery


def insert_data_to_bigquery(data, schema, dataset_id, table_id):
    # Create a client
    client = bigquery.Client()

    # Specify the dataset ID and table ID
    job_config = bigquery.LoadJobConfig(schema=schema)

    job = client.load_table_from_dataframe(
        dataframe=data,
        destination=f"{dataset_id}.{table_id}",
        job_config=job_config,
    )

if __name__=="__main__":
    
    # Load data
    data = DL.load_data()
    # Create a database bigquery schema
    schema = cs.get_bq_schema(data)
    
    
    dataset_id = 'nyc_taxi_data_2022'
    table_id = 'green_taxi_data_2022'
    
    # Create a table in bigquery
    cs.create_bq_table_from_dataframe(dataset_id, table_id, data)
    print("Schema Created!! \n")
    
    # Insert data into bigquery
    insert_data_to_bigquery(data, schema, dataset_id, table_id)
    print("Data Loaded.")



