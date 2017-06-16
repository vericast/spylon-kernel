"""Example use of jupyter_kernel_test, with tests for IPython."""

import os
import unittest

import jupyter_kernel_test

from textwrap import dedent
from unittest import SkipTest


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
        Console.err.println("oh noes!")
        // Sleep for a bit since the process for getting text output is asynchronous
        Thread.sleep(1000)
        '''

    complete_code_samples = ['val y = 8']
    incomplete_code_samples = ['{ val foo = 9 ']
    invalid_code_samples = ['val {}']

    code_generate_error = "4 / 0"

    def test_execute_stderr(self):
        if not self.code_stderr:
            raise SkipTest

        self.flush_channels()
        reply, output_msgs = self.execute_helper(code=self.code_stderr)

        self.assertEqual(reply['content']['status'], 'ok')

        self.assertGreaterEqual(len(output_msgs), 1)
        for msg in output_msgs:
            if (msg['msg_type'] == 'stream') and msg['content']['name'] == 'stderr':
                self.assertIn('oh noes!', msg['content']['text'])
                break
        else:
            self.assertTrue(False, "Expected at least one 'stream' message of type 'stderr'")

    def test_execute_stdout(self):
        if not self.code_hello_world:
            raise SkipTest

        self.flush_channels()
        reply, output_msgs = self.execute_helper(code=self.code_hello_world)

        self.assertEqual(reply['content']['status'], 'ok')

        self.assertGreaterEqual(len(output_msgs), 1)
        for msg in output_msgs:
            if (msg['msg_type'] == 'stream') and msg['content']['name'] == 'stdout':
                self.assertIn('hello, world', msg['content']['text'])
                break
        else:
            self.assertTrue(False, "Expected at least one 'stream' message of type 'stdout' ")


if __name__ == '__main__':
    unittest.main()