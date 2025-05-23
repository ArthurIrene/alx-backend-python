#!/usr/bin/python3
from itertools import islice

# Import the module
stream_users_module = __import__('0-stream_users')

# Call the function from inside the module
for user in islice(stream_users_module.stream_users(), 6):
    print(user)
