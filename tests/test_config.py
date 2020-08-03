import unittest
import re
from config import *


class ConfigTests(unittest.TestCase):

    def _assert_invalid_type_exception(self, exception: Exception, bad_type: AnyStr, expected_type: AnyStr):
        self.assertRegex(str(exception), re.escape(bad_type), 'Should tell us the invalid type.')
        self.assertRegex(str(exception), re.escape(expected_type), 'Should tell us the type that is required.')


class StageNameTests(ConfigTests):

    def test_get_stage_name_uses_recipe_config(self):
        recipe_config = {
            'sf_stage': '@my_stage'
        }

        plugin_config = {
            'default_sf_stage': '@my_default_stage'
        }

        stage = get_stage_name(recipe_config, plugin_config)

        self.assertEqual(stage, recipe_config['sf_stage'])

    def test_get_stage_name_uses_plugin_config(self):
        recipe_config = {
            'sf_stage': ''
        }

        plugin_config = {
            'default_sf_stage': '@my_default_stage'
        }

        stage = get_stage_name(recipe_config, plugin_config)

        self.assertEqual(stage, plugin_config['default_sf_stage'])

    def test_get_stage_name_requires_a_stage(self):
        recipe_config = {
            'sf_stage': ''
        }
        plugin_config = {
            'default_sf_stage': ''
        }

        with self.assertRaisesRegex(ValueError, 'Stage must be specified.+plugin.+recipe'):
            _ = get_stage_name(recipe_config, plugin_config)

    def test_get_stage_name_prepends_at_sign(self):
        recipe_config = {
            'sf_stage': 'missing_at'
        }
        stage = get_stage_name(recipe_config, {})

        self.assertRegex(stage, f'^@{recipe_config["sf_stage"]}')


class ConnectionNameTests(ConfigTests):

    def test_get_connection_name_returns_name(self):
        config = {
            'type': 'Snowflake',
            'params': {
                'connection': 'FOO_BAR'
            }
        }
        name = get_connection_name(config)
        self.assertEqual(name, 'FOO_BAR')

    def test_get_connection_name_requires_snowflake(self):
        bad_type = 'SnozzleSql'

        with self.assertRaises(ValueError) as cm:
            _ = get_connection_name({
                'type': bad_type,
                'params': {
                    'connection': 'BAZ'
                }
            })

        self._assert_invalid_type_exception(cm.exception, bad_type, 'Snowflake')


class TableNameTests(ConfigTests):

    def test_get_table_name_requires_snowflake(self):
        bad_type = 'SnozzleSql'

        with self.assertRaises(ValueError) as cm:
            _ = get_table_name({
                'type': bad_type,
                'params': {
                    'connection': 'BAZ'
                }
            })

        self._assert_invalid_type_exception(cm.exception, bad_type, 'Snowflake')

    def test_get_table_name_must_be_table_mode(self):
        bad_mode = 'query'
        with self.assertRaisesRegex(ValueError, f'{re.escape(bad_mode)}.+not.+supported'):
            _ = get_table_name({
                'type': 'Snowflake',
                'projectKey': 'SOME_PROJECT',
                'params': {
                    'mode': bad_mode
                }
            })

    def test_get_table_name_replaces_project_key(self):
        """
        Tests table and schema names that include the `${projectKey}` replacement variable
        """
        project_key = 'SOME_PROJECT'
        table_name = get_table_name({
            'type': 'Snowflake',
            'projectKey': project_key,
            'params': {
                'mode': 'table',
                'table': 'Foo${projectKey}Bar',
                'schema': '${projectKey}blah'
            }
        })
        self.assertNotRegex(table_name, re.escape('${projectKey}'))
        self.assertRegex(table_name, re.escape(project_key))

    def test_get_table_name_quotes_and_concatenates_table_and_schema(self):
        """
        Tests table name and schema name
        """
        input_table = 'My Table Name'
        input_schema = 'SCHEMAGIC'
        table_name = get_table_name({
            'type': 'Snowflake',
            'projectKey': 'SOME_PROJECT',
            'params': {
                'mode': 'table',
                'table': input_table,
                'schema': input_schema
            }
        })

        self.assertEqual(table_name, f'"{input_schema}"."{input_table}"')

    def test_get_table_name_quotes_table(self):
        """
        Tests table name without schema
        """
        input_table = 'My Table Name'
        table_name = get_table_name({
            'type': 'Snowflake',
            'projectKey': 'SOME_PROJECT',
            'params': {
                'mode': 'table',
                'table': input_table
            }
        })

        self.assertEqual(table_name, f'"{input_table}"')


class HdfsLocationTests(ConfigTests):

    def test_get_hdfs_location_requires_hdfs(self):
        bad_type = 'SFDH'

        with self.assertRaises(ValueError) as cm:
            _ = get_hdfs_location({
                'type': bad_type,
                'params': {
                    'connection': 'BAZ'
                }
            }, 'Some Stage')

        self._assert_invalid_type_exception(cm.exception, bad_type, 'HDFS')

    def test_get_hdfs_location_requires_parquet(self):
        bad_format_type = 'AVROcadabra'
        config = {
            'type': 'HDFS',
            'formatType': bad_format_type
        }

        with self.assertRaisesRegex(ValueError, f'Unsupported.+{re.escape(bad_format_type)}.+parquet'):
            _ = get_hdfs_location(config, 'some_stage_name')

    def test_get_hdfs_location_requires_snappy(self):
        bad_compression_type = 'Zippy'
        config = {
            'type': 'HDFS',
            'formatType': 'parquet',
            'formatParams': {
                'parquetCompressionMethod': bad_compression_type
            }
        }

        with self.assertRaisesRegex(ValueError, f'Unsupported.+{re.escape(bad_compression_type)}.+SNAPPY'):
            _ = get_hdfs_location(config, 'some_stage_name')

    def test_get_hdfs_location_replaces_project_key(self):
        proj_key = 'SOME_PROJ_KEY'
        config = {
            'type': 'HDFS',
            'formatType': 'parquet',
            'formatParams': {
                'parquetCompressionMethod': 'SNAPPY'
            },
            'projectKey': proj_key,
            'params': {
                'path': '/foo/${projectKey}/blah'
            }
        }
        location = get_hdfs_location(config, '@some_stage')
        self.assertRegex(location, re.escape(proj_key))
        self.assertNotRegex(location, re.escape('${projectKey'))

    def test_get_hdfs_location_prepends_stage_name(self):
        stage_name = '@some_stage'
        config = {
            'type': 'HDFS',
            'formatType': 'parquet',
            'formatParams': {
                'parquetCompressionMethod': 'SNAPPY'
            },
            'projectKey': 'some_porject',
            'params': {
                'path': '/foo/${projectKey}/blah'
            }
        }
        location = get_hdfs_location(config, stage_name)
        self.assertRegex(location, f'^{re.escape(stage_name)}')

    def test_get_hdfs_location_appends_forward_slash(self):
        config = {
            'type': 'HDFS',
            'formatType': 'parquet',
            'formatParams': {
                'parquetCompressionMethod': 'SNAPPY'
            },
            'projectKey': 'some_porject',
            'params': {
                'path': '/foo/${projectKey}/blah'
            }
        }
        location = get_hdfs_location(config, 'some_stage')
        self.assertRegex(location, '/$')


class QueryCreationTests(ConfigTests):

    def test_get_snowflake_to_hdfs_query_creates_query(self):
        location = '@STAGE_EXAMPLE/foo/bar/'
        table = '"SCHEMAGIC"."TABLELATOR"'
        schema = [
            {
                'name': 'col1', 'sfType': 'VARCHAR', 'type': 'string'
            },
            {
                'name': 'col2', 'sfType': 'DATE', 'type': 'date'
            },
            {
                'name': 'col3', 'sfType': 'BOOLEAN', 'type': 'bool'
            }
        ]

        sql = get_snowflake_to_hdfs_query(location, table, schema)

        self.assertEqual(sql.strip(), f"""
COPY INTO '{location}'
FROM (
    SELECT "col1" AS "col1", "col2" AS "col2", "col3" AS "col3"
    FROM {table}
)
FILE_FORMAT = (TYPE = PARQUET)
OVERWRITE = TRUE
HEADER = TRUE;
            """.strip())

    def test_get_snowflake_to_hdfs_query_casts_columns(self):
        schema = [
            {
                'name': 'col0', 'sfType': 'TIMESTAMP_LTZ', 'type': 'timestamp'
            },
            {
                'name': 'col1', 'sfType': 'TIMESTAMP_TZ', 'type': 'timestamp'
            },
            {
                'name': 'col2', 'sfType': 'DATE', 'type': 'date'
            },
            {
                'name': 'col3', 'sfType': 'VARCHAR', 'type': 'string'
            },
            {
                # Alias for TIMESTAMP_NTZ https://docs.snowflake.com/en/sql-reference/data-types-datetime.html#datetime
                'name': 'col4', 'sfType': 'DATETIME', 'type': 'timestamp'
            },
            {
                'name': 'col5', 'sfType': 'NUMBER', 'type': 'bigint'
            },
            {
                'name': 'col6', 'sfType': 'NUMBER', 'type': 'int'
            },
            {
                'name': 'col7', 'sfType': 'NUMBER', 'type': 'decimal'
            }
        ]

        sql = get_snowflake_to_hdfs_query('@STAGE_EXAMPLE/foo/bar/', '"SCHEMAGIC"."TABLELATOR"', schema)

        expected_columns = f'"{schema[0]["name"]}"::TIMESTAMP_NTZ AS "{schema[0]["name"]}", ' \
                           f'"{schema[1]["name"]}"::TIMESTAMP_NTZ AS "{schema[1]["name"]}", ' \
                           f'"{schema[2]["name"]}" AS "{schema[2]["name"]}", ' \
                           f'"{schema[3]["name"]}" AS "{schema[3]["name"]}", ' \
                           f'"{schema[4]["name"]}" AS "{schema[4]["name"]}", ' \
                           f'"{schema[5]["name"]}"::bigint AS "{schema[5]["name"]}", ' \
                           f'"{schema[6]["name"]}"::int AS "{schema[6]["name"]}", ' \
                           f'"{schema[7]["name"]}" AS "{schema[7]["name"]}"'

        self.assertRegex(sql, re.escape(expected_columns))

    def test_get_hdfs_to_snowflake_query_requires_a_column(self):
        with self.assertRaisesRegex(ValueError, 'must have at least one column'):
            _ = get_hdfs_to_snowflake_query('@some_location', '"SOME"."TABLE"', [])

    def test_get_hdfs_to_snowflake_query_concatenates_columns(self):
        schema = [
            {
                'name': 'col1'
            },
            {
                'name': 'col2'
            },
            {
                'name': 'col3'
            }
        ]

        sql = get_hdfs_to_snowflake_query('@STAGE_EXAMPLE/foo/bar/', '"SCHEMAGIC"."TABLELATOR"', schema)

        expected_columns = f'$1:"{schema[0]["name"]}", ' \
                           f'$1:"{schema[1]["name"]}", ' \
                           f'$1:"{schema[2]["name"]}"'

        self.assertRegex(sql, re.escape(expected_columns))

    def test_get_hdfs_to_snowflake_query_creates_query(self):
        location = '@STAGE_EXAMPLE/foo/bar/'
        table = '"SCHEMAGIC"."TABLELATOR"'
        schema = [
            {
                'name': 'col1'
            },
            {
                'name': 'col2'
            },
            {
                'name': 'col3'
            }
        ]
        sql = get_hdfs_to_snowflake_query(location, table, schema)
        self.assertEqual(sql.strip(), f"""
COPY INTO {table}
FROM (
    SELECT $1:"col1", $1:"col2", $1:"col3"
    FROM '{location}'
)
FILE_FORMAT = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE)
PATTERN = '.*\\.snappy\\.parquet'
FORCE = TRUE;
            """.strip())


class TableSchemaTests(ConfigTests):

    def test_get_table_schema_sql_no_schema(self):
        sql = get_table_schema_sql('"B"')
        self.assertEqual(sql.strip(), """
SELECT column_name AS "name", data_type AS "sfType"
FROM information_schema.columns
WHERE table_name = 'B'
        """.strip())

    def test_get_table_schema_sql_with_schema(self):
        sql = get_table_schema_sql('"A"."B"')
        self.assertEqual(sql.strip(), """
SELECT column_name AS "name", data_type AS "sfType"
FROM information_schema.columns
WHERE table_name = 'B' AND table_schema = 'A'
        """.strip())


class CombineSnowflakeSchemaInformationTests(ConfigTests):

    def test_combine_snowflake_schema_information_adds_sf_type(self):
        dss_schema = [
            {
                'name': 'col1', 'type': 'string'
            },
            {
                'name': 'col2', 'type': 'date'
            },
            {
                'name': 'col3', 'type': 'bool'
            }
        ]
        sf_schema = [
            {
                'name': 'col1', 'sfType': 'VARCHAR'
            },
            {
                'name': 'col2', 'sfType': 'DATE'
            },
            {
                'name': 'col3', 'sfType': 'BOOLEAN'
            }
        ]
        schema = combine_snowflake_schema_information(dss_schema, sf_schema)

        self.assertEqual([
            {
                'name': 'col1', 'sfType': 'VARCHAR', 'type': 'string'
            },
            {
                'name': 'col2', 'sfType': 'DATE', 'type': 'date'
            },
            {
                'name': 'col3', 'sfType': 'BOOLEAN', 'type': 'bool'
            }
        ], schema)

        pass


if __name__ == '__main__':
    unittest.main()
