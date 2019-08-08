import unittest

from common import get_logger

class CommonTests(unittest.TestCase):
    def test_get_logger(self):
        logger = get_logger()
        self.assertIsNotNone(logger)

if __name__ == '__main__':
    unittest.main()
