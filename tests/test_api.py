import unittest
from src.application.api import app


class TestAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = app.test_client()

    def test_get_first_chunk(self):
        response = self.app.get('/read/first-chunk')

        # Check if the response status code is 200
        self.assertEqual(response.status_code, 200)

        # Check if response contains JSON data
        self.assertEqual(response.headers['Content-Type'], 'application/json')

        # Check if the response body contains a list of 10 JSON objects
        data = response.json()
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 10)


if __name__ == '__main__':
    unittest.main()
