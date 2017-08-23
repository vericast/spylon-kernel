"""Metakernel magic for evaluating cell code using a ScalaInterpreter."""
from __future__ import absolute_import, division, print_function

import os
from metakernel import ExceptionWrapper
from metakernel import Magic
from metakernel import MetaKernel
from metakernel import option
from metakernel.process_metakernel import TextOutput
from tornado import ioloop, gen
from textwrap import dedent

from .scala_interpreter import get_scala_interpreter, scala_intp, ScalaException
from . import scala_interpreter


class ScalaMagic(Magic):
    """Line and cell magic that supports Scala code execution.

    Attributes
    ----------
    _interp : spylon_kernel.ScalaInterpreter
    _is_complete_ready : bool
        Guard for whether certain actions can be taken based on whether the
        ScalaInterpreter is instantiated or not
    retval : any
        Last result from evaluating Scala code
    """
    def __init__(self, kernel):
        super(ScalaMagic, self).__init__(kernel)
        self.retval = None
        self._interp = scala_intp
        self._is_complete_ready = False

    def _get_scala_interpreter(self):
        """Ensure that we have a scala interpreter around and set up the stdout/err
        handlers if needed.

        Returns
        -------
        scala_intp : scala_interpreter.ScalaInterpreter
        """
        if self._interp is None:
            assert isinstance(self.kernel, MetaKernel)
            self.kernel.Display(TextOutput("Intitializing Scala interpreter ..."))
            self._interp = get_scala_interpreter()

            # Ensure that spark is available in the python session as well.
            if 'python' in self.kernel.cell_magics: # tests if in scala mode
                self.kernel.cell_magics['python'].env['spark'] = self._interp.spark_session
                self.kernel.cell_magics['python'].env['sc'] = self._interp.sc

            # Display some information about the Spark session
            sc = self._interp.sc
            self.kernel.Display(TextOutput(dedent("""\
                Spark Web UI available at {webui}
                SparkContext available as 'sc' (version = {version}, master = {master}, app id = {app_id})
                SparkSession available as 'spark'
                """.format(
                    version=sc.version,
                    master=sc.master,
                    app_id=sc.applicationId,
                    webui=self._interp.web_ui_url
                )
            )))

            # Let down the guard: the interpreter is ready for use
            self._is_complete_ready = True

            # Send stdout to the MetaKernel.Write method
            # and stderr to MetaKernel.Error
            self._interp.register_stdout_handler(self.kernel.Write)
            self._interp.register_stderr_handler(self.kernel.Error)

        return self._interp

    def line_scala(self, *args):
        """%scala - evaluates a line of code as Scala

        Parameters
        ----------
        *args : list of string
            Line magic arguments joined into a single-space separated string

        Examples
        --------
        %scala val x = 42
        %scala import scala.math
        %scala x + math.pi
        """
        code = " ".join(args)
        self.eval(code, True)

    # Use optparse to parse the whitespace delimited cell magic options
    # just as we would parse a command line.
    @option(
        "-e", "--eval_output", action="store_true", default=False,
        help="Evaluate the return value from the Scala code as Python code"
    )
    def cell_scala(self, eval_output=False):
        """%%scala - evaluate contents of cell as Scala code

        This cell magic will evaluate the cell (either expression or statement) as
        Scala code. This will instantiate a Scala interpreter prior to running the code.

        The -e or --eval_output flag signals that the result of the Scala execution
        will be evaluated as Python code. The result of that evaluation will be the
        output for the cell.

        Examples
        --------
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
        # Ensure there is code to execute, not just whitespace
        if self.code.strip():
            if eval_output:
                # Evaluate the Scala code
                self.eval(self.code, False)
                # Don't store the Scala as the return value
                self.retval = None
                # Tell the base class to evaluate retval as Python
                # source code
                self.evaluate = True
            else:
                # Evaluate the Scala code
                self.retval = self.eval(self.code, False)
                # Tell the base class not to touch the Scala result
                self.evaluate = False

    def eval(self, code, raw):
        """Evaluates Scala code.

        Parameters
        ----------
        code: str
            Code to execute
        raw: bool
            True to return the raw result of the evalution, False to wrap it with
            MetaKernel classes

        Returns
        -------
        metakernel.process_metakernel.TextOutput or metakernel.ExceptionWrapper or
        the raw result of the evaluation
        """
        intp = self._get_scala_interpreter()
        try:
            res = intp.interpret(code.strip())
            if raw:
                self.res = intp.last_result()
                return self.res
            else:
                if res:
                    return TextOutput(res)
        except ScalaException as ex:
            # Get the kernel response so far
            resp = self.kernel.kernel_resp
            # Wrap the exception for MetaKernel use
            resp['status'] = 'error'
            tb = ex.scala_message.split('\n')
            first = tb[0]
            assert isinstance(first, str)
            eclass, _, emessage = first.partition(':')
            return ExceptionWrapper(eclass, emessage, tb)

    def post_process(self, retval):
        """Processes the output of one or stacked magics.

        Parameters
        ----------
        retval : any or None
            Value from another magic stacked with this one in a cell

        Returns
        -------
        any
            The received value if it's not None, otherwise the stored
            `retval` of the last Scala code execution
        """
        if retval is not None:
            return retval
        return self.retval

    def get_completions(self, info):
        """Gets completions from the kernel based on the provided info.

        Parameters
        ----------
        info : dict
            Information returned by `metakernel.parser.Parser.parse_code`
            including `code`, `help_pos`, `start`, etc.

        Returns
        -------
        list of str
            Possible completions for the code
        """
        intp = self._get_scala_interpreter()
        completions = intp.complete(info['code'], info['help_pos'])

        # Find common bits in the middle
        def trim(prefix, completions):
            """Due to the nature of Scala's completer we get full method names.
            We need to trim out the common pieces. Try longest prefix first, etc.
            """
            potential_prefix = os.path.commonprefix(completions)
            for i in reversed(range(len(potential_prefix)+1)):
                if prefix.endswith(potential_prefix[:i]):
                    return i
            return 0

        prefix = info['code'][info['start']:info['help_pos']]
        offset = trim(prefix, completions)
        final_completions = [prefix + h[offset:] for h in completions]

        self.kernel.log.debug('''info %s\ncompletions %s\nfinal %s''', info, completions, final_completions)
        return final_completions

    def get_help_on(self, info, level=0, none_on_fail=False):
        """Gets help text for the `info['help_obj']` identifier.

        Parameters
        ----------
        info : dict
            Information returned by `metakernel.parser.Parser.parse_code`
            including `help_obj`, etc.
        level : int, optional
            Level of help to request, 0 for basic, 1 for more, etc.
            By convention only. There is no true maximum.
        none_on_fail : bool, optional
            Return none when execution fails, ignored

        Returns
        -------
        str
            Help text
        """
        intp = self._get_scala_interpreter()
        self.kernel.log.debug(info['help_obj'])
        # Calling this twice produces different output
        code = intp.complete(info['help_obj'], len(info['help_obj']))
        code = intp.complete(info['help_obj'], len(info['help_obj']))
        self.kernel.log.debug(code)
        return '\n'.join(code)
