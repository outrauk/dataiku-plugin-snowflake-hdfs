from typing import AnyStr, Mapping, Any, List
import re


def get_stage_name(recipe_config: Mapping[AnyStr, AnyStr], plugin_config: Mapping[AnyStr, AnyStr]) -> AnyStr:
    """
    Gets the best Snowflake STAGE to use based on configuration settings. Does not check that the stage itself
    is valid or accessible.
    :param recipe_config:
    :param plugin_config:
    :return: a Snowflake STAGE
    """
    sf_stage_name = recipe_config.get('sf_stage', '')
    if not sf_stage_name:
        sf_stage_name = plugin_config.get('default_sf_stage', '')

    if not sf_stage_name:
        raise ValueError('Snowflake Stage must be specified in the settings of the plugin or recipe.')

    return sf_stage_name if sf_stage_name.startswith('@') else f'@{sf_stage_name}'


def _assert_dataset_type(expected_type: AnyStr, actual_type: AnyStr) -> None:
    if expected_type != actual_type:
        raise ValueError(f'Unsupported dataset type {actual_type}. Must be {expected_type}.')


def get_connection_name(dataset_config: Mapping[AnyStr, Any]) -> AnyStr:
    """
    Get the connection name for a SQLExecutor2
    :param dataset_config:
    :return: a DSS connection name
    """
    _assert_dataset_type('Snowflake', dataset_config['type'])

    return dataset_config['params']['connection']


def get_table_name(dataset_config: Mapping[AnyStr, Any]) -> AnyStr:
    """
    Get the destination or source Snowflake table (schema and name)
    :param dataset_config:
    :return: A double-quote-escaped schema and table
    """
    _assert_dataset_type('Snowflake', dataset_config['type'])

    project_key: AnyStr = dataset_config['projectKey']

    params: Mapping[AnyStr, AnyStr] = dataset_config['params']

    if params['mode'] != 'table':
        # e.g., mode == query
        raise ValueError(f'Snowflake connection mode {params["mode"]} is not currently supported.')

    table_name = f'"{params["table"]}"'

    if params.get('schema', ''):
        table_name = f'"{params["schema"]}".{table_name}'

    return table_name.replace('${projectKey}', project_key)


def get_hdfs_location(dataset_config: Mapping[AnyStr, Any], sf_stage_name: AnyStr) -> AnyStr:
    """
    Get the stage and path to HDFS files
    :param dataset_config:
    :param sf_stage_name:
    :return: a Snowflake location
    """
    _assert_dataset_type('HDFS', dataset_config['type'])

    format_type: AnyStr = dataset_config['formatType']

    if format_type != 'parquet':
        raise ValueError(f'Unsupported dataset format type {format_type}. Must be parquet.')

    compression_method: AnyStr = dataset_config['formatParams']['parquetCompressionMethod']

    if compression_method != 'SNAPPY':
        raise ValueError(f'Unsupported compression method {compression_method}. Must be SNAPPY.')

    project_key: AnyStr = dataset_config['projectKey']

    params: Mapping[AnyStr, AnyStr] = dataset_config['params']

    path = params['path'].replace('${projectKey}', project_key)

    return f'{sf_stage_name}{path}/'


def get_table_schema_sql(sf_table_name: AnyStr) -> AnyStr:
    """
    Generates SQL to extract a table's column schema (using `sfType` for the Snowflake data type)
    :param sf_table_name: Snowflake table
    :return: SQL for getting table columns
    """

    # finds a quoted table name and, optionally, a schema prefix
    # see tests for example inputs
    tbl_sch_re = re.search(r'("?(?P<schema>[^"]+)"?\.)?"?(?P<table>[^"]+)"?', sf_table_name)

    schema_clause = f'AND table_schema = \'{tbl_sch_re.group("schema")}\'' if tbl_sch_re.group('schema') else ''

    # Dataiku's `originalType` parameter is Snowflake's data_type but without `_`
    return f"""
SELECT column_name AS "name", data_type AS "sfType"
FROM information_schema.columns
WHERE table_name = '{tbl_sch_re.group("table")}' {schema_clause}
    """


def get_snowflake_to_hdfs_query(sf_location: AnyStr, sf_table_name: AnyStr,
                                sf_schema: List[Mapping[AnyStr, AnyStr]]) -> AnyStr:
    """
    Gets a COPY statement for copying from a Snowflake Table to an HDFS location
    :param sf_location: HDFS location
    :param sf_table_name: Snowflake table
    :param sf_schema: Snowflake schema with a `sfType` property
    :return: a SQL COPY statement
    """
    # moving this function here isn't strictly necessary as it's not re-used, but it makes
    # things significantly easier to unit test.

    # Generate SELECT clause and cast TIMESTAMP_TZ, TIMESTAMP_LTZ to TIMESTAMP_NTZ
    # This is required as the COPY command does not support TZ and LTZ
    columns = [
        f'"{c["name"]}"' + (
            f'::TIMESTAMP_NTZ AS "{c["name"]}"'
            if c['sfType'] in ['TIMESTAMP_LTZ', 'TIMESTAMP_TZ']
            else ''
        )
        for c in sf_schema
    ]

    sql = f"""
COPY INTO '{sf_location}'
FROM (
    SELECT {', '.join(columns)}
    FROM {sf_table_name}
)
FILE_FORMAT = (TYPE = PARQUET)
OVERWRITE = TRUE
HEADER = TRUE;
    """

    return sql


def get_hdfs_to_snowflake_query(sf_location: AnyStr, sf_table_name: AnyStr,
                                hdfs_schema: List[Mapping[AnyStr, AnyStr]]) -> AnyStr:
    """
    Gets a COPY statement for copying from an HDFS location to a Snowflake table. Assumes the table
    already exists.
    :param sf_location: a STAGE path
    :param sf_table_name: an existing table
    :param hdfs_schema: columns of the HDFS source
    :return: a SQL COPY statement
    """
    if not hdfs_schema:
        raise ValueError('hdfs_schema must have at least one column')

    columns = [f'$1:"{col["name"]}"' for col in hdfs_schema]

    sf_file_pattern = '.*\\.snappy\\.parquet'

    # "TYPE = ?" could come from in_config['formatType']}, theoretically
    sql = f"""
COPY INTO {sf_table_name}
FROM (
    SELECT {', '.join(columns)}
    FROM '{sf_location}'
)
FILE_FORMAT = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE)
PATTERN = '{sf_file_pattern}'
FORCE = TRUE;
    """

    return sql
