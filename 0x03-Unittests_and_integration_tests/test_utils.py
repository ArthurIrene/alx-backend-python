
import unittest
from parameterized import parameterized 
from utils import access_nested_map

class TestAccessNestedMap(unittest.TestCase):
    @parameterized.expand([
        ({"a": 1}, ("a",), 1),
        ({"a": {"b": 2}},("a",),{"b": 2}),
        ({"a": {"b": 2}},("a", "b"),2)      
        ])
    def test_access_nested_map(self,nested_map, path , result):
        actual_value = access_nested_map(nested_map, path)

        self.assertEqual(actual_value, result)
        pass 