
import dataiku
from dataiku.customrecipe import *
from dataiku.core.sql import SQLExecutor2
from common import get_logger
from config import *

# Logging setup
logger = get_logger()

sf_input = dataiku.Dataset(get_input_names_for_role('sf_input')[0])
sf_input_config = sf_input.get_config()
hdfs_output = dataiku.Dataset(get_output_names_for_role('hdfs_output')[0])

sf_stage_name = get_stage_name(get_recipe_config(), get_plugin_config())
sf_table_name = get_table_name(sf_input_config)
sf_connection_name = get_connection_name(sf_input_config)
# we want our output files to be prefixed with "part"
sf_location = f'{get_hdfs_location(hdfs_output.get_config(), sf_stage_name)}part'

logger.info(f'SF Stage Location: {sf_location}')
logger.info(f'SF Stage: {sf_stage_name}')
logger.info(f'SF Table: {sf_table_name}')
logger.info(f'SF Connection Name: {sf_connection_name}')

# write the schema for our HDFS table
hdfs_output.write_schema(sf_input.read_schema(), dropAndCreate=True)

# open a writer and close it so that it'll force DSS to create the table.
with hdfs_output.get_writer():
    pass

sql = get_snowflake_to_hdfs_query(sf_location, sf_table_name)

logger.info(f'SF Query: {sql}')

executor = SQLExecutor2(connection=sf_connection_name)
results = executor.query_to_df(sql)

logger.info(f'COPY results: ${results.to_string()}')
