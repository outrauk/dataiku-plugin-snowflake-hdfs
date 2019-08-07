
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

sql_table_name = f'"{sf_output_config["params"]["schema"]}"."{sf_output_config["params"]["table"]}"'


logger.info(f'SF Table: {sql_table_name}')

# create output dataframe of zero rows
sf_output_df = hdfs_input.get_dataframe().head(n=0)

# write HDFS
sf_output.write_with_schema(sf_output_df)


path = hdfs_input_config['params']['path']
proj_key = dataiku.get_custom_variables()['projectKey']
path = path.replace('${projectKey}', proj_key)

# @PUBLIC.OUTRA_DATA_DATAIKU_EMR_MANAGED_STAGE
# using "part_" gives file that were output by SF. "part-" is what gets output by DSS 
sf_location = f'{sf_stage_name}{path}/part*.snappy.parquet'


logger.info(f'SF Stage Location: {sf_location}')

# 1) delete the *.snappy.parquet that exists: threeuk_cell_id_location_parq.get_files_info()['globalPaths']
# ... or don't care because we know it has zero rows
# 2) run the COPY INTO
# TODO: get stage name from a parameter or something...


columns = [f'$1:"{col["name"]}"' for col in hdfs_input.read_schema()]

# "TYPE = ?" could come from in_config['formatType']}, theoretically
sql = f"""COPY INTO sql_table_name
FROM (
SELECT {', '.join(columns)}
FROM '{sf_location}'
)
FILE_FORMAT = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE)
PURGE = TRUE
FORCE = TRUE;
"""

logger.info(f'SF Query: {sql}')


executor = SQLExecutor2(connection=sf_output_config['params']['connection'])
results = executor.query_to_df(sql)
logger.info(results)




