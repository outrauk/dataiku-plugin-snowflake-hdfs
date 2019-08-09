import unittest
from common import get_logger


class CommonTests(unittest.TestCase):

    def test_get_logger(self):
        logger = get_logger()
        self.assertIsNotNone(logger)

        self.assertEqual(logger.name, 'snowflake-hdfs-plugin')


if __name__ == '__main__':
    unittest.main()
