#!/usr/bin/python3
import sys
processing = __import__('1-batch_processing')

try:
    for user in processing.batch_processing(50):
        print(user)
except BrokenPipeError:
    sys.stderr.close()
