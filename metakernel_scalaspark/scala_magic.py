from __future__ import absolute_import, division, print_function

import os
from metakernel import Magic
from metakernel import MetaKernel
from metakernel import option
from ._scala_interpreter import get_scala_interpreter, ScalaException
from . import _scala_interpreter


class ScalaMagic(Magic):

    def __init__(self, kernel):
        super(ScalaMagic, self).__init__(kernel)
        self.retval = None
        self._interp = None

    def _get_scala_interpreter(self):
        if self._interp is None:
            assert isinstance(self.kernel, MetaKernel)
            self.kernel.Display("Intitializing scala interpreter....")
            self._interp = get_scala_interpreter()
            self.kernel.Display("Scala interpreter initialized.")
            # Ensure that spark is available in the python session as well.
            self.kernel.cell_magics['python'].env['spark'] = _scala_interpreter.spark_session
            # self.Display("Registered spark session in scala and python context as `spark`")
        return self._interp

    def line_scala(self, *args):
        """
        %python CODE - evaluate code as Python
        This line magic will evaluate the CODE (either expression or
        statement) as Python code.
        Note that the version of Python is that of the notebook server.
        Examples:
            %scala val x = 42
            %scala import scala.math
            %scala x + math.pi
        """
        code = " ".join(args)
        self.retval = self.eval(code)

    def eval(self, code):
        intp = self._get_scala_interpreter()
        try:
            return TextOutput(intp.interpret(code.strip()))
        except ScalaException as e:
            return self.kernel.Error(e.scala_message)

    @option(
        "-e", "--eval_output", action="store_true", default=False,
        help="Use the retval value from the Scala cell as code in the kernel language."
    )
    def cell_scala(self, eval_output=False):
        """
        %%scala - evaluate contents of cell as Scala code

        This cell magic will evaluate the cell (either expression or statement) as Scala code.

        This will instantiate a scala interpreter prior to running the code.

        The -e or --eval_output flag signals that the retval value expression will be used as code for the cell to be
        evaluated by the host language.

        Examples:
            %%scala
            val x = 42

            %%scala
            import collections.mutable._
            val y = mutable.Map.empty[Int, String]

            %%scala -e
            retval = "'(this is code in the kernel language)"

            %%python -e
            "'(this is code in the kernel language)"
        """
        if self.code.strip():
            if eval_output:
                # TODO: Validate this works?
                self.eval(self.code)
                # self.code = str(self.env["retval"]) if ("retval" in self.env and
                #                                         self.env["retval"] != None) else ""
                self.retval = None
                #self.env["retval"] = None
                self.evaluate = True
            else:
                self.retval = self.eval(self.code)
                #self.env["retval"] = None
                self.evaluate = False

    def post_process(self, retval):
        if retval is not None:
            return retval
        else:
            return self.retval

    def get_completions(self, info):
        intp = self._get_scala_interpreter()
        # raise Exception(repr(info))
        c = intp.complete(info['code'], info['help_pos'])

        # Find common bits in the middle
        def trim(prefix, completions):
            """Due to the nature of scala's completer we get full method names.
            We need to trim out the common pieces.  Try longest prefix first etc
            """
            potential_prefix = os.path.commonprefix(completions)
            for i in reversed(range(len(potential_prefix)+1)):
                if prefix.endswith(potential_prefix[:i]):
                    return i
            return 0

        prefix = info['code'][info['start']:info['help_pos']]

        offset = trim(prefix, c)

        a = [prefix + h[offset:] for h in c]
        self.kernel.log.critical("info %s\n    completions %s\n     final %s", info, c, a)
        return a

    def get_help_on(self, info, level=0, none_on_fail=False):
        intp = self._get_scala_interpreter()
        self.kernel.log.critical(info['help_obj'])
        # Calling this twice produces different output
        code = intp.complete(info['help_obj'], len(info['help_obj']))
        code = intp.complete(info['help_obj'], len(info['help_obj']))
        self.kernel.log.critical(code)
        return '\n'.join(code)


class TextOutput(object):
    """Wrapper for text output whose repr is the text itself.

    This avoids the repr(output) quoting our output strings.

    Notes
    -----
    Adapted from eclairjs-kernel
    """
    def __init__(self, output):
        self.output = output

    def __repr__(self):
        return self.output


def register_magics(kernel):
    kernel.register_magics(ScalaMagic)
