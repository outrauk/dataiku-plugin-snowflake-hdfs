# Snowflake HDFS Tools

This is a [Dataiku plugin](https://doc.dataiku.com/dss/latest/plugins/index.html) that makes it easy to enable fast loads of [Parquet](https://en.wikipedia.org/wiki/Apache_Parquet) files between [Snowflake](https://www.snowflake.com) and HDFS.

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

GRANT USAGE ON YOUR_ACCOUNT_DATAIKU_EMR_MANAGED_STAGE TO DSS_SF_ROLE_NAME;
```

Note that this example uses an [AWS IAM role](https://docs.snowflake.net/manuals/user-guide/data-load-s3-config.html#option-2-configuring-an-aws-iam-role) for securing the stage's connection to your S3 bucket. There's no reason a stage secured using AWS access keys wouldn't work, but it has not been tested.


## Installing

You can install the plugin by referencing this GitHub repository and following [these instructions](https://doc.dataiku.com/dss/latest/plugins/installing.html#installing-from-a-git-repository).

Or, you can create a Zip file and following [these instructions](https://doc.dataiku.com/dss/latest/plugins/installing.html#installing-from-a-zip-file). To create the Zip file, you'll need to build it:

1. Ensure you have `json_pp` and `node` installed locally
2. `make plugin`
3. Upload the zip file from the `/dist` directory

## Configuring

You can (optionally) configure a _Default Snowflake Stage_ in the plugin's settings. For example, the `STAGE` created above would be entered as `@PUBLIC.YOUR_ACCOUNT_DATAIKU_EMR_MANAGED_STAGE`.

When using the recipe, you can override the default stage in the _Snowflake Stage_ setting.

## Usage

### Snowflake &rarr; HDFS

1. In a Dataiku flow, select an existing Snowflake dataset (note that the dataset must point to a Snowflake table; it can't be a query)
2. From the _Plugin recipes_ section on the right, click _Snowfl..._ (it uses exchange arrows as the icon)
3. From the popup, pick _Sync Snowflake to HDFS_
4. Make sure your existing Snowflake table is the _Input_ and set a new or existing HDFS dataset. Make sure _Store into_ is a connection with the same path as the `STAGE`, as described above, and the _Format_ is *Parquet*.
5. Click _Create_
6. Set the _Snowflake Stage_ to the stage created above (note that if you've set a default stage in the plugin's settings, you can skip this step)
7. Click _Run_

### HDFS &rarr; Snowflake

1. In a Dataiku flow, select an existing HDFS dataset. (Note that the dataset's connection must point to the same S3 bucket as is configured in the `STAGE`, as described above. Additionally, the dataset must be Parquet format with Snappy compression.)
2. From the _Plugin recipes_ section on the right, click _Snowfl..._ (it uses exchange arrows as the icon)
3. From the popup, pick _Sync HDFS to Snowflake_
4. Make sure your existing HDFS dataset is the _Input_ and set a new or existing Snowflake dataset
5. Click _Create_
6. Set the _Snowflake Stage_ to the stage created above (note that if you've set a default stage in the plugin's settings, you can skip this step)
7. Click _Run_

## Building in PyCharm

Make sure that [wget](https://www.gnu.org/software/wget/) is installed. For macOS, you can install via `brew install wget`.

Custom Recipe libraries aren't included in DSS's `dataiku-internal-client` package so we need to fake it 'til we make it.

First, create the package by executing `./make_dss_pip.sh`. If successful, the last line it prints is a `pip` command.

Second, use the `pip` command from the previous step to install the package in the library's virtual environment. (If you're using PyCharm, open _View_ &rarr; _Tool Windows_ &rarr; _Terminal_ and paste the `pip` command in.)

## Running Tests in PyCharm

1. Make sure you can build the project by doing all of the `pip` steps above
2. In PyCharm, run the _Unit Tests_ configuration.

## Known Issues
* Neither Snowflake nor Dataiku use [Logical Type annotations](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#datetime-types) for timezone offsets (Dataiku doesn't use Logical Types at all). When synching from Snowflake to HDFS, the plugin casts any `TIMESTAMP_TZ` or `TIMESTAMP_LTZ` columns to `TIMESTAMP_NTZ` which simply drops the timezone offset attribute. For greater control of this behaviour, transform your Snowflake table before passing it to this plugin. Consider using `CONVERT_TIMEZONE('UTC', t."date")::TIMESTAMP_NTZ`.
  See [Date & Time Data Types in Snowflake](https://docs.snowflake.com/en/sql-reference/data-types-datetime.html) for more details.
+ All TIMESTAMP (LTZ, TZ, NTZ) and DATE columns from Snowflake are stored as INTEGER in Parquet.
  See [this article](https://bigpicture.pl/blog/2017/11/timestamps-parquet-hadoop/#:~:text=Timestamp%20in%20Hive&text=They%20are%20interpreted%20as%20timestamps,depends%20on%20the%20file%20format.) for more details about Timestamp in Parquet.

## TODO

- [x] When Snowflake is an input, check that it's a table and not a query
- [ ] Add support for Snowflake input being a query rather than a table
- [x] Check that the HDFS input or output is Parquet and Snappy
- [ ] Add support for partitioned datasources
- [ ] For Snowflake to HDFS, delete the *.parquet.snappy file in the output path (it's the one created when inserting the empty dataframe)
- [ ] Support more types than Parquet (e.g., `CSV`, `AVRO`, etc.)
- [ ] Verify that the HDFS connection is actually an S3 location (possibly the best way to enforce the `STAGE` lining up to HDFS)
