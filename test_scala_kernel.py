"""Example use of jupyter_kernel_test, with tests for IPython."""

import unittest
import jupyter_kernel_test
import os


class SpylonKernelTests(jupyter_kernel_test.KernelTests):
    kernel_name = "spylon-kernel"
    language_name = "scala"
    # code_hello_world = "disp('hello, world')"
    completion_samples = [
        {'text': 'val x = 8; x.toL',
         'matches': {'x.toLong'}},
    ]
    code_page_something = "x?"

    complete_code_samples = ['val y = 8']
    incomplete_code_samples = ['{ val foo = 9 ']
    invalid_code_samples = ['val {}']


if __name__ == '__main__':
    unittest.main()