"""Example use of jupyter_kernel_test, with tests for IPython."""

import unittest
import jupyter_kernel_test
import os
from textwrap import dedent


coverage_rc = os.path.abspath(os.path.join(os.path.dirname(__file__), ".coveragerc"))
os.environ["COVERAGE_PROCESS_START"] = coverage_rc


class SpylonKernelTests(jupyter_kernel_test.KernelTests):
    kernel_name = "spylon-kernel"
    language_name = "scala"
    # code_hello_world = "disp('hello, world')"
    completion_samples = [
        {'text': 'val x = 8; x.toL',
         'matches': {'x.toLong'}},
    ]
    code_page_something = "x?"

    code_hello_world = '''
        println("hello, world")
        // Sleep for a bit since the process for getting text output is asynchronous
        Thread.sleep(1000)
        '''

    code_stderr = '''
        Console.err.println("Error")
        // Sleep for a bit since the process for getting text output is asynchronous
        Thread.sleep(1000)
        '''

    complete_code_samples = ['val y = 8']
    incomplete_code_samples = ['{ val foo = 9 ']
    invalid_code_samples = ['val {}']

    code_generate_error = "4 / 0"

if __name__ == '__main__':
    unittest.main()