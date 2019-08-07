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

# Inputs and outputs are defined by roles. In the recipe's I/O tab, the user can associate one
# or more dataset to each input and output role.
# Roles need to be defined in recipe.json, in the inputRoles and outputRoles fields.

# To  retrieve the datasets of an input role named 'input_A' as an array of dataset names:
input_A_names = get_input_names_for_role('input_A_role')
# The dataset objects themselves can then be created like this:
input_A_datasets = [dataiku.Dataset(name) for name in input_A_names]

# For outputs, the process is the same:
output_A_names = get_output_names_for_role('main_output')
output_A_datasets = [dataiku.Dataset(name) for name in output_A_names]


# The configuration consists of the parameters set up by the user in the recipe Settings tab.

# Parameters must be added to the recipe.json file so that DSS can prompt the user for values in
# the Settings tab of the recipe. The field "params" holds a list of all the params for wich the
# user will be prompted for values.

# The configuration is simply a map of parameters, and retrieving the value of one of them is simply:
my_variable = get_recipe_config()['parameter_name']

# For optional parameters, you should provide a default value in case the parameter is not present:
my_variable = get_recipe_config().get('parameter_name', None)

# Note about typing:
# The configuration of the recipe is passed through a JSON object
# As such, INT parameters of the recipe are received in the get_recipe_config() dict as a Python float.
# If you absolutely require a Python int, use int(get_recipe_config()["my_int_param"])


#############################
# Your original recipe
#############################

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from dataiku.core.sql import SQLExecutor2

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Read recipe inputs
threeuk_CELL_ID_LOCATION = dataiku.Dataset("THREEUK_CELL_ID_LOCATION")
threeuk_CELL_ID_LOCATION_df = threeuk_CELL_ID_LOCATION.get_dataframe()

threeuk_CELL_ID_LOCATION_df = threeuk_CELL_ID_LOCATION_df.head(n=0)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
in_config = threeuk_CELL_ID_LOCATION.get_config()

sql_table_name = f'"{in_config["params"]["schema"]}"."{in_config["params"]["table"]}"'

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Compute recipe outputs from inputs
# TODO: Replace this part by your actual code that computes the output, as a Pandas dataframe
# NB: DSS also supports other kinds of APIs for reading and writing data. Please see doc.

threeuk_cell_id_location_parq_df = threeuk_CELL_ID_LOCATION_df # For this sample code, simply copy input to output

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Write recipe outputs
threeuk_cell_id_location_parq = dataiku.Dataset("threeuk_cell_id_location_parq")
threeuk_cell_id_location_parq.write_with_schema(threeuk_cell_id_location_parq_df)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
out_config = threeuk_cell_id_location_parq.get_config()
# out_config

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# threeuk_cell_id_location_parq.get_files_info()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
path = threeuk_cell_id_location_parq.get_config()['params']['path']
pk = dataiku.get_custom_variables()['projectKey']
path.replace('${projectKey}', pk)


# 1) delete the *.snappy.parquet that exists: threeuk_cell_id_location_parq.get_files_info()['globalPaths']
# ... or don't care because we know it has zero rows
# 2) run the COPY INTO
# TODO: get stage name from a parameter or something...
stage = f'@PUBLIC.OUTRA_DATA_DATAIKU_EMR_MANAGED_STAGE{path.replace("${projectKey}", pk)}/part'
stage

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# "TYPE = ?" could come from {out_config['formatType']}, theoretically
sql = f"""COPY INTO '{stage}'
FROM {sql_table_name}
FILE_FORMAT = (TYPE = PARQUET)
OVERWRITE = TRUE
HEADER = TRUE;
"""

print(sql)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
executor = SQLExecutor2(connection=in_config['params']['connection'])
results = executor.query_to_df(sql)
results