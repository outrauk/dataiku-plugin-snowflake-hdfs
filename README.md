# dataiku-plugin-snowflake-hdfs

DSS plugin for fast loading Parquet files between Snowflake and HDFS.

## Prerequisites

* Snowflake S3 `STAGE` that points to the same directory as DSS's managed HDFS connection

## Building in PyCharm

Custom Recipe libraries aren't included in DSS's `dataiku-internal-client` package so we need to fake it 'til we make it.

First, create the package:

```bash
cd ~/Downloads/
wget https://cdn.downloads.dataiku.com/public/dss/5.1.5/dataiku-dss-5.1.5.tar.gz
tar -zxf dataiku-dss-5.1.5.tar.gz
cd -
cp setup-template.py.txt ~/Downloads/dataiku-dss-5.1.5/python/setup.py
cd ~/Downloads/dataiku-dss-5.1.5/python/
python setup.py sdist
```

Second, install or update the package in this library's virtual environment. If you're using PyCharm, open a Terminal (from "View") and enter:

```bash
pip install dataiku --no-index --find-links file:///path/to/Downloads/dataiku-dss-5.1.5/python/dist
```

## TODO

- [x] When Snowflake is an input, check that it's a table and not a query
- [ ] Add support for Snowflake input being a query rather than a table
- [x] Check that the HDFS input or output is Parquet and Snappy
- [ ] Add support for partitioned datasources
- [ ] For Snowflake to HDFS, delete the *.parquet.snappy file in the output path (it's the one created when inserting the empty dataframe)
- [ ] Support more types than Parquet (e.g., `CSV`, `AVRO`, etc.)

