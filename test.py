#!/usr/bin/python3 -u

import os
import sys

if __name__ == '__main__':
    query_string = os.environ.get('QUERY_STRING')
    print('Content-type: text/html\n\n', end='')
    print('<html><body><h1>OK</h1></body></html>')


