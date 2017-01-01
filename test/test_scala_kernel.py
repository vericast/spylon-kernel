import pytest
from spylon_kernel import SpylonKernel
import re
from textwrap import dedent

class MockingSpylonKernel(SpylonKernel):

    def __init__(self, *args, **kwargs):
        super(MockingSpylonKernel, self).__init__(*args, **kwargs)
        self.Displays = []
        self.Errors = []

    def Display(self, *args, **kwargs):
        self.Displays.append((args, kwargs))

    def Error(self, *args, **kwargs):
        self.Errors.append((args, kwargs))


@pytest.fixture(scope="module")
def spylon_kernel(request):
    return MockingSpylonKernel()


def test_simple_expression(spylon_kernel):
    assert isinstance(spylon_kernel, MockingSpylonKernel)
    result = spylon_kernel.do_execute_direct("4 + 4")
    assert re.match('res\d+: Int = 8\n', result.output)


def test_exception(spylon_kernel):
    spylon_kernel.do_execute_direct("4 / 0 ")
    error, _ = spylon_kernel.Errors.pop()
    assert "java.lang.ArithmeticException: / by zero" in error[0]


def test_completion(spylon_kernel):
    spylon_kernel.do_execute_direct("val x = 4")
    code = "x.toL"
    result = spylon_kernel.do_complete(code, len(code))
    assert set(result['matches']) == {'x.toLong'}


def test_iscomplete(spylon_kernel):
    result = spylon_kernel.do_is_complete('val foo = 99')
    assert result['status'] == 'complete'

    result = spylon_kernel.do_is_complete('val foo = {99')
    assert result['status'] == 'incomplete'

    result = spylon_kernel.do_is_complete('val foo {99')
    assert result['status'] == 'invalid'


def test_last_result(spylon_kernel):
    spylon_kernel.do_execute_direct("""
    case class LastResult(member: Int)
    val foo = LastResult(8)
    """)
    res = spylon_kernel.do_execute("x = %scala foo")
    assert res['status'] == 'ok'


def test_help(spylon_kernel):
    spylon_kernel.do_execute_direct("val x = 4")
    h = spylon_kernel.get_help_on("x")
    assert h.strip() == 'val x: Int'


def test_init_magic(spylon_kernel):
    code = dedent("""\
        %%init_spark
        launcher.conf.spark.executor.cores = 2
        """)
    spylon_kernel.do_execute(code)


def test_init_magic_completion(spylon_kernel):
    code = dedent("""\
        %%init_spark
        launcher.conf.spark.executor.cor""")
    result = spylon_kernel.do_complete(code, len(code))
    assert set(result['matches']) == {'launcher.conf.spark.executor.cores'}
