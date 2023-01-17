import pyspark.pandas as ps


pq_file_path = '../../data/interim/parquet/'

pdf = ps.read_parquet(pq_file_path)

