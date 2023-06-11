"""Example use of jupyter_kernel_test, with tests for IPython."""

import unittest

import jupyter_kernel_test

from spylon_kernel.scala_interpreter import init_spark
from textwrap import dedent


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
        Console.err.println("oh noes!")
        // Sleep for a bit since the process for getting text output is asynchronous
        Thread.sleep(1000)
        '''

    complete_code_samples = ['val y = 8']
    incomplete_code_samples = ['{ val foo = 9 ']
    invalid_code_samples = ['val {}']

    code_generate_error = "4 / 0"

    code_execute_result = [{
        'code': 'val x = 1',
        'result': 'x: Int = 1\n'
    }, {
        'code': 'val y = 1 to 3',
        'result': 'y: scala.collection.immutable.Range.Inclusive = Range 1 to 3\n'
    }]

    spark_configured = False

    def setUp(self):
        """Set up to capture stderr for testing purposes."""
        super(SpylonKernelTests, self).setUp()
        self.flush_channels()
        if not self.spark_configured:
            self.execute_helper(code='%%init_spark --stderr')
            self.spark_configured = True


if __name__ == '__main__':
    unittest.main()