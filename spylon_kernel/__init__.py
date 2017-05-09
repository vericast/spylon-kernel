from __future__ import absolute_import, print_function, division

from .scala_kernel import SpylonKernel
from .scala_magic import ScalaMagic
from .init_spark_magic import InitSparkMagic
from .scala_interpreter import get_scala_interpreter

# Version info from versioneer
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
