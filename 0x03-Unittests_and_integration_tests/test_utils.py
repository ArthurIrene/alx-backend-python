#!/usr/bin/env python3
import unittest
from parameterized import parameterized 
from utils import access_nested_map , get_json
from unittest import mock

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
    @parameterized.expand([
        ({}, ("a",) , 'a'),
        ({"a": 1}, ("a", "b"), 'b')
    ])
    def test_access_nested_map_exception(self,nested_map, path , result):
        with self.assertRaises(KeyError) as context_manager:
            access_nested_map(nested_map, path)

        self.assertEqual(context_manager.exception.args[0], result)
        pass    
class TestGetJson(unittest.TestCase):
    @parameterized.expand([
        ("http://example.com", {"payload": True}), # First tuple
        ("http://holberton.io", {"payload": False})  # Second tuple
    ])
    @mock.patch('utils.requests.get')
    def test_get_json(self, test_url, test_payload, mock_requests_get):
        mock_requests_get.return_value = mock.Mock()
        mock_requests_get.return_value.json.return_value = test_payload
        real_result = get_json(test_url)
        self.assertEqual(real_result, test_payload)
        mock_requests_get.assert_called_once_with(test_url)
