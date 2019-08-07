
import dataiku
from dataiku.customrecipe import *
from dataiku.core.sql import SQLExecutor2
import logging

# Logging setup
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger('snowflake-hdfs-plugin')




# Inputs and outputs are defined by roles. In the recipe's I/O tab, the user can associate one
# or more dataset to each input and output role.
# Roles need to be defined in recipe.json, in the inputRoles and outputRoles fields.
# To  retrieve the datasets of an input role named 'input_A' as an array of dataset names:
hdfs_input = dataiku.Dataset(get_input_names_for_role('hdfs_input')[0])
hdfs_input_config = hdfs_input.get_config()
sf_output = dataiku.Dataset(get_output_names_for_role('sf_output')[0])
sf_output_config = sf_output.get_config()
# TODO: check input and output types. Check that output is Snowflake and that it's a table and not a query
# check that the input is HDFS and Parquet
# TODO: check that it's actually Parquet and Snappy

sf_stage_name = get_recipe_config().get('snowflake_stage', '')

sf_stage_name = sf_stage_name if sf_stage_name else get_plugin_config().get('default_sf_stage', '')

if not sf_stage_name:
    raise ValueError("Specify a Snowflake stage either in the plugin's settings or the recipe's settings.")

sf_stage_name = sf_stage_name if sf_stage_name.startswith('@') else f'@{sf_stage_name}'


logger.info(f'SF Stage: {sf_stage_name}')

sql_table_name = f'"{sf_input_config["params"]["schema"]}"."{sf_input_config["params"]["table"]}"'


logger.info(f'SF Table: {sql_table_name}')

# create output dataframe of zero rows
hdfs_output_df = sf_input.get_dataframe().head(n=0)

# write HDFS
hdfs_output.write_with_schema(hdfs_output_df)


path = hdfs_output_config['params']['path']
proj_key = dataiku.get_custom_variables()['projectKey']
path = path.replace('${projectKey}', proj_key)

# @PUBLIC.OUTRA_DATA_DATAIKU_EMR_MANAGED_STAGE
sf_location = f'{sf_stage_name}{path}/part'


logger.info(f'SF Stage Location: {sf_location}')

# 1) delete the *.snappy.parquet that exists: threeuk_cell_id_location_parq.get_files_info()['globalPaths']
# ... or don't care because we know it has zero rows
# 2) run the COPY INTO
# TODO: get stage name from a parameter or something...

# "TYPE = ?" could come from {out_config['formatType']}, theoretically
sql = f"""COPY INTO '{sf_location}'
FROM {sql_table_name}
FILE_FORMAT = (TYPE = PARQUET)
OVERWRITE = TRUE
HEADER = TRUE;
"""

logger.info(f'SF Query: {sql}')


executor = SQLExecutor2(connection=sf_input_config['params']['connection'])
results = executor.query_to_df(sql)
logger.info(results)




