# dataiku-plugin-snowflake-hdfs

DSS plugin for fast loading Parquet files between Snowflake and HDFS.

## Prerequisites

* An HDFS connection in Dataiku that points to an S3 bucket
* A [Snowflake S3 `STAGE`](https://docs.snowflake.net/manuals/user-guide/data-load-s3-create-stage.html) that points to the same S3 bucket and path as DSS's managed HDFS connection

Your HDFS connection here:

![image](https://user-images.githubusercontent.com/939816/62780119-da3bbc80-baac-11e9-8791-b9ee61da9d0d.png)

Should match the `URL` here:

```sql
CREATE OR REPLACE STAGE YOUR_ACCOUNT_DATAIKU_EMR_MANAGED_STAGE
    URL = 's3://your-account-dataiku-emr/data'
    CREDENTIALS = (AWS_ROLE = 'arn:aws:iam::123456:role/SnowflakeCrossAccountRole');
GRANT USAGE ON YOUR_ACCOUNT_DATAIKU_EMR_MANAGED_STAGE TO DSP_RW;
```

Note that this example uses an [AWS IAM role](https://docs.snowflake.net/manuals/user-guide/data-load-s3-config.html#option-2-configuring-an-aws-iam-role) for securing the stage's connection to your S3 bucket. There's no reason a stage secured using AWS access keys wouldn't work, but it has not been tested. 


## Configuring

You can (optionally) configure a _Default Snowflake Stage_ in the plugin's settings. For example, the `STAGE` created above would be entered as `@PUBLIC.YOUR_ACCOUNT_DATAIKU_EMR_MANAGED_STAGE`.

When using the recipe, you can override the default stage in the _Snowflake Stage_ setting. 



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

## Installing

To build the distribution, you'll either just install the plugin by referencing the GitHub repository or create a distribution zip:

1. Ensure you have `json_pp` and `node` installed locally
2. `make plugin`
3. Upload the zip file from the `/dist` directory


## TODO

- [x] When Snowflake is an input, check that it's a table and not a query
- [ ] Add support for Snowflake input being a query rather than a table
- [x] Check that the HDFS input or output is Parquet and Snappy
- [ ] Add support for partitioned datasources
- [ ] For Snowflake to HDFS, delete the *.parquet.snappy file in the output path (it's the one created when inserting the empty dataframe)
- [ ] Support more types than Parquet (e.g., `CSV`, `AVRO`, etc.)
- [ ] Verify that the HDFS connection is actually an S3 location (possibly the best way to enforce the `STAGE` lining up to HDFS)
