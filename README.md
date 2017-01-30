# spylon-kernel
[![Build Status](https://travis-ci.org/maxpoint/spylon-kernel.svg?branch=master)](https://travis-ci.org/maxpoint/spylon-kernel)
[![codecov](https://codecov.io/gh/maxpoint/spylon-kernel/branch/master/graph/badge.svg)](https://codecov.io/gh/maxpoint/spylon-kernel)

This is a beta level concept for using metakernel in combination with py4j to make a simpler kernel for scala.

## Installation

On python 3.5+

:exclamation: Due to [SPARK-19019](https://issues.apache.org/jira/browse/SPARK-19019) Apache Spark does not current work
 in Python 3.6

```bash
pip install .
```

For runtime purposes you need to have Apache Spark installed.  Minimum required is Apache Spark 2.0 compiled for Scala 2.11.
See [examples](./examples/basic_example.ipynb) for how to configure your spark instance.

## Installing the jupyter kernel

```
python -m spylon_kernel install
```

## Using the kernel

The scala spark metakernel provides a scala kernel by default. On the first execution of scala code, a spark session
will be constructed so that a user can interact with the interpreter.

### Customizing the spark context

The launch arguments can be customized using the `%%init_spark` magic as follows

```python
%%init_spark
launcher.jars = ["file://some/jar.jar"]
launcher.master = "local[4]"
launcher.conf.spark.executor.cores = 8
```

### Other languages

Since this makes use of metakernel you can evaluate normal python code using the `%%python` magic.  In addition once 
the spark context has been created the `spark` variable will be added to your python environment.

```python
%%python
df = spark.read.json("examples/src/main/resources/people.json")
```

## Using as a magic

Spylon-kernel can be used as a magic in an existing ipykernel.  This is the recommended solution when you want to write
relatively small blocks of scala.

```python
from spylon_kernel import register_ipython_magics
register_ipython_magics()
```

```scala
%%scala
val x = 8
x
```

## Using as a library

If you just want to send a string of scala code to the interpreter and evaluate it you can
do that too.

```python
from spylon_kernel import get_scala_interpreter

interp = get_scala_interpreter()

# Evaluate the result of a scala code block.
interp.interpret("""
    val x = 8
    x
    """)

interp.last_result()
```
