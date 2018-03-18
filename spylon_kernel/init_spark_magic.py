"""Metakernel magic for configuring and automatically initializing a Spark session."""
import logging
import spylon.spark

from metakernel import Magic, option
from .scala_interpreter import init_spark

try:
    import jedi
    from jedi.api.helpers import get_on_completion_name
    from jedi import common
except ImportError as ex:
    jedi = None


class InitSparkMagic(Magic):
    """Cell magic that supports configuration property autocompletion and
    initializes a Spark session.

    Attributes
    ----------
    env : dict
        Copy of the Python builtins plus a spylon.spark.launcher.SparkConfiguration
        object for use when initializing the Spark context
    log : logging.Logger
        Logger for this instance
    """
    def __init__(self, kernel):
        super(InitSparkMagic, self).__init__(kernel)
        self.env = globals()['__builtins__'].copy()
        self.env['launcher'] = spylon.spark.launcher.SparkConfiguration()
        self.log = logging.Logger(self.__class__.__name__)

    # Use optparse to parse the whitespace delimited cell magic options
    # just as we would parse a command line.
    @option(
        "--stderr", action="store_true", default=False,
        help="Capture stderr in the notebook instead of in the kernel log"
    )
    def cell_init_spark(self, stderr=False):
        """%%init_spark [--stderr] - starts a SparkContext with a custom
        configuration defined using Python code in the body of the cell

        Includes a `spylon.spark.launcher.SparkConfiguration` instance
        in the variable `launcher`. Looks for an `application_name`
        variable to use as the name of the Spark session.

        Example
        -------
        %%init_spark
        launcher.jars = ["file://some/jar.jar"]
        launcher.master = "local[4]"
        launcher.conf.spark.app.name = "My Fancy App"
        launcher.conf.spark.executor.cores = 8
        """
        # Evaluate the cell contents as Python
        exec(self.code, self.env)
        # Use the launcher to initialize a spark session
        init_spark(conf=self.env['launcher'], capture_stderr=stderr)
        # Do not evaluate the cell contents using the kernel
        self.evaluate = False

    def get_completions(self, info):
        """Gets Python completions based on the current cursor position
        within the %%init_spark cell.

        Based on
        https://github.com/Calysto/metakernel/blob/master/metakernel/magics/python_magic.py

        Parameters
        ----------
        info : dict
            Information about the current caret position
        """
        if jedi is None:
            return []

        text = info['code']
        position = (info['line_num'], info['column'])
        interpreter = jedi.Interpreter(text, [self.env])

        lines = common.splitlines(text)
        name = get_on_completion_name(
            interpreter._get_module_node(),
            lines,
            position
        )

        before = text[:len(text) - len(name)]
        completions = interpreter.completions()
        completions = [before + c.name_with_symbols for c in completions]
        return [c[info['start']:] for c in completions]