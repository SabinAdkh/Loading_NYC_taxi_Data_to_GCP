from Data_Loaders import load_data
from google.cloud import bigquery


# Converting pandas datatypes to GCP_Bigquery datatyeps
def pandas_dtype_to_bq_dtype(pandas_dType):
    if pandas_dType=="int64":
        return "INT64"
    elif pandas_dType=="datetime64[us]":
        return "TIMESTAMP"
    elif pandas_dType=="float64":
        return "FLOAT64"
    elif pandas_dType=="object":
        return "STRING"
    else:
        return "STRING"

# Creating a schema using dataframe columns 
def get_bq_schema(dataframe):
    schema = []
    for column, dtype in dataframe.dtypes.items():
        schema.append(bigquery.SchemaField(column, pandas_dtype_to_bq_dtype(str(dtype))))
    return schema

# Table schema
# print(get_bq_schema(data))
    
    
def create_bq_table_from_dataframe(dataset_id, table_id, dataframe):
    client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table(table_id)
    table = bigquery.Table(table_ref, schema=get_bq_schema(dataframe))
    try:
        table = client.create_table(table)  
        print(f"Table {table.table_id} created successfully.")
    except Exception as e:
        print(f"Table creation failed: {e}")





# Replace 'your_dataset_id' and 'your_table_id' with your actual dataset and table IDs respectively
# create_bq_table_from_dataframe('nyc_taxi_data_2022', 'green_taxi_data_2022', data)