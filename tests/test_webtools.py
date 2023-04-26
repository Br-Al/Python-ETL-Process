import unittest
import os
import sys
from dotenv import load_dotenv
from scripts.webtools import get_data
load_dotenv('../.env')

class TestWebtools(unittest.TestCase):
    def test_get_data(self):
        data = get_data(table_name="webtools_powerbi_install")
        self.assertGreater(data.shape[0], 0, "No data found in the provided table.")

if __name__ == '__main__':
    unittest.main()