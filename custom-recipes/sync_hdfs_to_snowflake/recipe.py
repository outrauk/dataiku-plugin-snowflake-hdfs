import dataiku
from dataiku.customrecipe import *
from dataiku.core.sql import SQLExecutor2
from common import get_logger
from config import *

# Logging setup
logger = get_logger()

hdfs_input = dataiku.Dataset(get_input_names_for_role('hdfs_input')[0])
hdfs_input_config = hdfs_input.get_config()
sf_output = dataiku.Dataset(get_output_names_for_role('sf_output')[0])
sf_output_config = sf_output.get_config()

sf_stage_name = get_stage_name(get_recipe_config(), get_plugin_config())
sf_table_name = get_table_name(sf_output_config)
sf_connection_name = get_connection_name(sf_output_config)
# When creating parquet, we want to prefix with "part". But when importing parquet, we don't care.
sf_location = get_hdfs_location(hdfs_input_config, sf_stage_name)

logger.info(f'SF Stage Location: {sf_location}')
logger.info(f'SF Stage: {sf_stage_name}')
logger.info(f'SF Table: {sf_table_name}')
logger.info(f'SF Connection Name: {sf_connection_name}')

# create output dataframe of zero rows
sf_output_df = hdfs_input.get_dataframe(sampling='head', limit=1).head(n=0)

# write Snowflake table
sf_output.write_with_schema(sf_output_df)

sql = get_hdfs_to_snowflake_query(sf_location, sf_table_name, hdfs_input.read_schema())

logger.info(f'SF Query: {sql}')

executor = SQLExecutor2(connection=sf_connection_name)
results = executor.query_to_df(sql, post_queries=['COMMIT'])

logger.info(f'COPY results: ${results.to_string()}')
