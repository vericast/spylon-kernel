import pytest
import re
from spylon_kernel.scala_interpreter import initialize_scala_interpreter, get_web_ui_url


@pytest.fixture(scope="module")
def scala_interpreter(request):
    wrapper = initialize_scala_interpreter()
    return wrapper


def test_simple_expression(scala_interpreter):
    result = scala_interpreter.interpret("4 + 4")
    assert re.match('res\d+: Int = 8\n', result)


def test_completion(scala_interpreter):
    scala_interpreter.interpret("val x = 4")
    code = "x.toL"
    result = scala_interpreter.complete(code, len(code))
    assert result == ['toLong']


def test_is_complete(scala_interpreter):
    result = scala_interpreter.is_complete('val foo = 99')
    assert result == 'complete'

    result = scala_interpreter.is_complete('val foo = {99')
    assert result == 'incomplete'

    result = scala_interpreter.is_complete('val foo {99')
    assert result == 'invalid'


def test_last_result(scala_interpreter):
    scala_interpreter.interpret("""
    case class LastResult(member: Int)
    val foo = LastResult(8)
    """)
    jres = scala_interpreter.last_result()

    assert jres.getClass().getName().endswith("LastResult")
    assert jres.member() == 8


def test_help(scala_interpreter):
    scala_interpreter.interpret("val x = 4")
    h = scala_interpreter.get_help_on("x")

    scala_interpreter.interpret("case class Foo(bar: String)")
    scala_interpreter.interpret('val y = Foo("something") ')

    h1 = scala_interpreter.get_help_on("y")
    h2 = scala_interpreter.get_help_on("y.bar")

    assert h == "Int"
    assert h1 == "Foo"
    assert h2 == "String"


def test_spark_rdd(scala_interpreter):
    """Simple test to ensure we can do RDD things"""
    result = scala_interpreter.interpret("sc.parallelize(0 until 10).sum().toInt")
    assert result.strip().endswith(str(sum(range(10))))


def test_spark_dataset(scala_interpreter):
    scala_interpreter.interpret("""
    case class DatasetTest(y: Int)
    import spark.implicits._
    val df = spark.createDataset((0 until 10).map(DatasetTest(_)))
    import org.apache.spark.sql.functions.sum
    val res = df.agg(sum('y)).collect().head
    """)
    strres = scala_interpreter.interpret("res.getLong(0)")
    result = scala_interpreter.last_result()
    assert result == sum(range(10))


def test_web_ui_url(scala_interpreter):
    url = get_web_ui_url(scala_interpreter.sc)
    assert url != ""


def test_anon_func(scala_interpreter):
    result = scala_interpreter.interpret("sc.parallelize(0 until 10).map(x => x * 2).sum().toInt")
    assert result.strip().endswith(str(sum(x * 2 for x in range(10))))


def test_case_classes(scala_interpreter):
    scala_interpreter.interpret('case class DatasetTest(y: Int)')
    scala_interpreter.interpret('''
    val df = spark.createDataset((0 until 10).map(DatasetTest(_)))
    val res = df.agg(sum('y)).collect().head''')
    strres = scala_interpreter.interpret("res.getLong(0)")
    result = scala_interpreter.last_result()
    assert result == sum(range(10))