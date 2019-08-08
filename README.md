# dataiku-plugin-snowflake-hdfs

DSS plugin for fast loading Parquet files between Snowflake and HDFS.

## Prerequisites

* Snowflake S3 `STAGE` that points to the same directory as DSS's managed HDFS connection

## Creating a Snowflake `STAGE`

Though you may be able to create a stage that uses AWS credentials, it hasn't been tested. Instead, use an IAM role. The `STAGE` can be created like so:

```sql
CREATE OR REPLACE STAGE YOUR_ACCOUNT_DATAIKU_EMR_MANAGED_STAGE
    URL = 's3://your-account-dataiku-emr/data'
    CREDENTIALS = (AWS_ROLE = 'arn:aws:iam::123456:role/SnowflakeCrossAccountRole');
GRANT USAGE ON YOUR_ACCOUNT_DATAIKU_EMR_MANAGED_STAGE TO DSP_RW;
```

Where the `URL` is the same S3 path configured in the HDFS connection within DSS.

And you'd then reference it in the plugin as `@PUBLIC.YOUR_ACCOUNT_DATAIKU_EMR_MANAGED_STAGE`.


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
- [ ] Verify that the HDFS connection is actually an S3 location (possibly the best way to enforce the `STAGE` lining up to HDFS)
