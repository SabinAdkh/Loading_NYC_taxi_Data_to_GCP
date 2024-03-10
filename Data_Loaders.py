import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def load_data():
    green_taxi_data = pd.DataFrame()

    for month in range(1, 13):
        if month < 10:
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-0{month}.parquet"
        else: url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{month}.parquet"
        data = pd.read_parquet(url, engine="pyarrow")
        # print(f"Finished reading data for month {month}\n")
        green_taxi_data = pd.concat([green_taxi_data, data], ignore_index=True)
    # print("Done.")

    return green_taxi_data

if __name__=="__main__": 
    pass

# Saving the data in parquet file
# green_taxi_data.to_parquet()
# pq.write_table(data, 'green_taxi_data_2022.parquet', compression=None)
