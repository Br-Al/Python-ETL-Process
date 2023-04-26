import unittest
import os
from dotenv import load_dotenv
load_dotenv('../.env')

class TestEnvVariable(unittest.TestCase):
    def test_load(self):
        self.assertIn('DBNAME', os.environ.keys(), '.env was not loaded correctly!')

if __name__ == '__main__':
    unittest.main()
    