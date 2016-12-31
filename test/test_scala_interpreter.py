import pytest
import re
from spylon_kernel._scala_interpreter import initialize_scala_kernel


@pytest.fixture(scope="module")
def scala_kernel(request):
    wrapper = initialize_scala_kernel()


    return wrapper


def test_simple_expression(scala_kernel):
    result = scala_kernel.interpret("4 + 4")
    assert re.match('res\d+: Int = 8\n', result)


def test_completion(scala_kernel):
    scala_kernel.interpret("val x = 4")
    code = "x.toL"
    result = scala_kernel.complete(code, len(code))
    assert result == ['toLong']


def test_iscomplete(scala_kernel):
    result = scala_kernel.is_complete('val foo = 99')
    assert result == 'complete'

    result = scala_kernel.is_complete('val foo = {99')
    assert result == 'incomplete'

    result = scala_kernel.is_complete('val foo {99')
    assert result == 'invalid'


# def test_interpreter_help(scala_kernel):
#     scala_kernel.interpret("val z = 5")
#     print(scala_kernel.jiloop.getClass().toString())
#     print(dir(scala_kernel.iMainOps))
#     scala_kernel.interpret("case class Foo(bar: String)")
#     h = scala_kernel.iMainOps.implicitsCommand("")
#     assert h == ''

def test_last_result(scala_kernel):
    scala_kernel.interpret("""
    case class LastResult(member: Int)
    val foo = LastResult(8)
    """)
    jres = scala_kernel.last_result()

    assert jres.getClass().getName().endswith("LastResult")
    assert jres.member() == 8

def test_help(scala_kernel):
    scala_kernel.interpret("val x = 4")
    h = scala_kernel.get_help_on("x")

    scala_kernel.interpret("case class Foo(bar: String)")
    scala_kernel.interpret('val y = Foo("something") ')

    h1 = scala_kernel.get_help_on("y")
    h2 = scala_kernel.get_help_on("y.bar")

    assert h == "Int"
    assert h1 == "Foo"
    assert h2 == "String"


def test_spark_rdd(scala_kernel):
    """Simple test to ensure we can do RDD things"""
    result = scala_kernel.interpret("sc.parallelize(0 until 10).sum().toInt")
    assert result.strip().endswith(str(sum(range(10))))


def test_spark_dataset(scala_kernel):
    scala_kernel.interpret("""
    case class DatasetTest(y: Int)
    import spark.implicits._
    val df = spark.createDataset((0 until 10).map(DatasetTest(_)))
    import org.apache.spark.sql.functions.sum
    val res = df.agg(sum('y)).collect().head
    """)
    strres = scala_kernel.interpret("res.getLong(0)")
    result = scala_kernel.last_result()
    assert result == sum(range(10))
