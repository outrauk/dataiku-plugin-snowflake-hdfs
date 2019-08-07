# Code for custom code recipe compute_threeuk_cell_id_location_parq (imported from a Python recipe)

# To finish creating your custom recipe from your original PySpark recipe, you need to:
#  - Declare the input and output roles in recipe.json
#  - Replace the dataset names by roles access in your code
#  - Declare, if any, the params of your custom recipe in recipe.json
#  - Replace the hardcoded params values by acccess to the configuration map

# See sample code below for how to do that.
# The code of your original recipe is included afterwards for convenience.
# Please also see the "recipe.json" file for more information.

# import the classes for accessing DSS objects from the recipe
import dataiku
# Import the helpers for custom recipes
from dataiku.customrecipe import *
from dataiku.core.sql import SQLExecutor2





# Inputs and outputs are defined by roles. In the recipe's I/O tab, the user can associate one
# or more dataset to each input and output role.
# Roles need to be defined in recipe.json, in the inputRoles and outputRoles fields.

# To  retrieve the datasets of an input role named 'input_A' as an array of dataset names:
sf_input = dataiku.Dataset(get_input_names_for_role('sf_input')[0])
sf_input_config = sf_input.get_config()
hdfs_output = dataiku.Dataset(get_output_names_for_role('hdfs_output')[0])
hdfs_output_config = hdfs_output.get_config()
# TODO: check input and output types. Check that input is Snowflake and that it's a table and not a query
# check that the output is HDFS and Parquet


sf_stage_name = get_recipe_config()['snowflake_stage']

sql_table_name = f'"{sf_input_config["params"]["schema"]}"."{sf_input_config["params"]["table"]}"'

# create output dataframe of zero rows
hdfs_output_df = sf_input.get_dataframe().head(n=0)

# write HDFS
hdfs_output.write_with_schema(hdfs_output_df)


path = hdfs_output_config['params']['path']
proj_key = dataiku.get_custom_variables()['projectKey']
path = path.replace('${projectKey}', proj_key)

# @PUBLIC.OUTRA_DATA_DATAIKU_EMR_MANAGED_STAGE
stage = f'{sf_stage_name}{path}/part'

# 1) delete the *.snappy.parquet that exists: threeuk_cell_id_location_parq.get_files_info()['globalPaths']
# ... or don't care because we know it has zero rows
# 2) run the COPY INTO
# TODO: get stage name from a parameter or something...

# "TYPE = ?" could come from {out_config['formatType']}, theoretically
sql = f"""COPY INTO '{stage}'
FROM {sql_table_name}
FILE_FORMAT = (TYPE = PARQUET)
OVERWRITE = TRUE
HEADER = TRUE;
"""

print(sql)


# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
executor = SQLExecutor2(connection=sf_input_config['params']['connection'])
results = executor.query_to_df(sql)
print(results)




