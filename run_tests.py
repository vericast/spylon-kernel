#!/usr/bin/env python
import sys
import pytest

if __name__ == '__main__':
    # call pytest and exit with the return code from pytest so that
    # travis will fail correctly if tests fail
    sys.exit(pytest.main(sys.argv[1:]))
