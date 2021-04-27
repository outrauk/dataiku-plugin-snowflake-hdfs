import dataiku
from dataiku.customrecipe import *
from dataiku.core.sql import SQLExecutor2
from common import get_logger
from config import *

logger = get_logger()

folder_input = dataiku.Folder(get_input_names_for_role('folder_input')[0])
folder_input_config = folder_input.get_info()
sf_output = dataiku.Dataset(get_output_names_for_role('sf_output')[0])
sf_output_config = sf_output.get_config()

sf_table_name = get_table_name(sf_output_config)
sf_connection_name = get_connection_name(sf_output_config)
sf_storage_integration = get_storage_integration_name(get_recipe_config(), get_plugin_config())

sub_path = get_recipe_config().get('sub_path', None)
folder_input_path = get_folder_location(folder_input_config, sub_path)

# if true, we should check if the table exists
# should_append = False

logger.info(f'S3 Folder Location: {folder_input_path}')
logger.info(f'SF Storage Integration: {sf_storage_integration}')
logger.info(f'SF Table: {sf_table_name}')
logger.info(f'SF Connection Name: {sf_connection_name}')


sf_create_tbl = get_create_table_query(sf_table_name)
# TODO: store table statement in params['customCreateStatement']
sql = get_s3_to_snowflake_query(sf_table_name, folder_input_path, 'OUTRA_DATA_STORAGE_INTEGRATION')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
logger.info(f'SF Table Create: {sf_create_tbl}')
logger.info(f'SF Copy Statement: {sql}')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# we won't actually use DSS to create the table
sf_output.write_schema([{
    'name': 'src',
    'type': 'string',
    'meaning': 'JSONObjectMeaning',
    'maxLength': -1
}])

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
executor = SQLExecutor2(connection=sf_connection_name)
results = executor.query_to_df(sql, post_queries=['COMMIT'], pre_queries=[sf_create_tbl])

logger.info(f'COPY results: {results.to_string()}')
