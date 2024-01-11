from __future__ import absolute_import, print_function, division

from .scala_kernel import SpylonKernel
from .scala_magic import ScalaMagic
from .init_spark_magic import InitSparkMagic
from .scala_interpreter import get_scala_interpreter


def register_ipython_magics():
    """For usage within ipykernel.

    This will instantiate the magics for IPython
    """
    from metakernel import IPythonKernel
    from IPython.core.magic import register_cell_magic, register_line_cell_magic
    kernel = IPythonKernel()
    scala_magic = ScalaMagic(kernel)
    init_spark_magic = InitSparkMagic(kernel)

    @register_line_cell_magic
    def scala(line, cell):
        if line:
            return scala_magic.line_scala(line)
        else:
            scala_magic.code = cell
            return scala_magic.cell_scala()

    @register_cell_magic
    def init_spark(line, cell):
        init_spark_magic.code = cell
        return init_spark_magic.cell_init_spark()

from . import _version
__version__ = _version.get_versions()['version']
