# blog-jupyter-notebooks
Jupyter notebooks for SageMaker analysis of blog analytics


## Libraries

Mainly using Pandas.

## Running Environment

This is being designed to work on the amazon sagemaker platform, with access to Athena for event data (and also a db connection to the postgres rds to get some wordpress data).

Sagemaker allows you to start and stop instances, keep them running, and persist some data to drive which (I believe) can be retained for later even if you shut down the server.  This allows for local caching strategies using the likes of parquet files from pandas.

The versions of libraries / tools might not be that up to date, so consider running updates to conda and its packages.  In particular jupyterlab and plotly should be pretty up to date to reduce issues with plotting (which have been a headache).

* conda update -n base -c defaults conda
* conda update --all


To use jupyterlab (which is good generally) with plotting, you will need to make sure the plotly jupyterlab extension is installed either through the gui, or see https://plot.ly/python/getting-started/ something like:

```
# Avoid "JavaScript heap out of memory" errors during extension installation
# (OS X/Linux)
export NODE_OPTIONS=--max-old-space-size=4096
# (Windows)
set NODE_OPTIONS=--max-old-space-size=4096

# Jupyter widgets extension
jupyter labextension install @jupyter-widgets/jupyterlab-manager@1.1 --no-build

# jupyterlab renderer support
jupyter labextension install jupyterlab-plotly@1.5.0 --no-build

# FigureWidget support
jupyter labextension install plotlywidget@1.5.0 --no-build

# Build extensions (must be done to activate extensions since --no-build is used above)
jupyter lab build

# Unset NODE_OPTIONS environment variable
# (OS X/Linux)
unset NODE_OPTIONS
# (Windows)
set NODE_OPTIONS=

```