# spylon-kernel
[![Build Status](https://travis-ci.org/maxpoint/spylon-kernel.svg?branch=master)](https://travis-ci.org/maxpoint/spylon-kernel)
[![codecov](https://codecov.io/gh/maxpoint/spylon-kernel/branch/master/graph/badge.svg)](https://codecov.io/gh/maxpoint/spylon-kernel)

A Scala [Jupyter kernel](http://jupyter.readthedocs.io/en/latest/projects/kernels.html) that uses [metakernel](https://github.com/Calysto/metakernel) in combination with [py4j](https://www.py4j.org/).

## Prerequisites

* Apache Spark 2.1.1 compiled for Scala 2.11
* Jupyter Notebook
* Python 3.5+

## Install

You can install the spylon-kernel package using `pip` or `conda`.

```bash
pip install spylon-kernel
# or
conda install -c conda-forge spylon-kernel
```

## Using it as a Scala Kernel

You can use spylon-kernel as Scala kernel for Jupyter Notebook. Do this when you want
to work with Spark in Scala with a bit of Python code mixed in.

Create a kernel spec for Jupyter notebook by running the following command:

```bash
python -m spylon_kernel install
```

Launch `jupyter notebook` and you should see a `spylon-kernel` as an option
in the *New* dropdown menu.

See [the basic example notebook](./examples/basic_example.ipynb) for information
about how to intiialize a Spark session and use it both in Scala and Python.

## Using it as an IPython Magic

You can also use spylon-kernel as a magic in an IPython notebook. Do this when
you want to mix a little bit of Scala into your primarily Python notebook.

```python
from spylon_kernel import register_ipython_magics
register_ipython_magics()
```

```scala
%%scala
val x = 8
x
```

## Using it as a Library

Finally, you can use spylon-kernel as a Python library. Do this when you
want to evaluate a string of Scala code in a Python script or shell.

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

# Release Process

Push a tag and submit a source dist to PyPI.

```
git commit -m 'REL: 0.2.1' --allow-empty
git tag -a 0.2.1 # and enter the same message as the commit
git push origin master # or send a PR

# if everything builds / tests cleanly, release to pypi
make release
```

Then update https://github.com/conda-forge/spylon-kernel-feedstock.
