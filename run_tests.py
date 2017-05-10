#!/usr/bin/env python
if __name__ == '__main__':
    import coverage
    cov = coverage.Coverage()
    cov.start()

    # Import required modules after coverage starts
    import sys
    import pytest

    # Call pytest and exit with the return code from pytest so that
    # CI systems will fail if tests fail.
    ret = pytest.main(sys.argv[1:])

    cov.stop()
    cov.save()
    # Save HTML coverage report to disk
    cov.html_report()
    # Emit coverage report to stdout
    cov.report()

    sys.exit(ret)