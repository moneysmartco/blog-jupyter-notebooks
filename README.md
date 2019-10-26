# blog-jupyter-notebooks
Jupyter notebooks for SageMaker analysis of blog analytics


## Libraries

Mainly using Pandas.

## Running Environment

This is being designed to work on the amazon sagemaker platform, with access to Athena for event data (and also a db connection to the postgres rds to get some wordpress data).

Sagemaker allows you to start and stop instances, keep them running, and persist some data to drive which (I believe) can be retained for later even if you shut down the server.  This allows for local caching strategies using the likes of parquet files from pandas.