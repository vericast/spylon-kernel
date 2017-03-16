from __future__ import absolute_import, division, print_function

import os
from metakernel import ExceptionWrapper
from metakernel import Magic
from metakernel import MetaKernel
from metakernel import option
from metakernel.process_metakernel import TextOutput
from tornado import ioloop, gen
from textwrap import dedent

from .scala_interpreter import get_scala_interpreter, ScalaException
from . import scala_interpreter


class ScalaMagic(Magic):
    """
    Attributes
    ----------
    _interp : spylon_kernel.ScalaInterpreter
    """

    def __init__(self, kernel):
        super(ScalaMagic, self).__init__(kernel)
        self.retval = None
        self._interp = None
        self._is_complete_ready = False
        self.spark_web_ui_url = ""

    def _get_scala_interpreter(self):
        """Ensure that we have a scala interpreter around and set up the stdout/err handlers if needed.

        Returns
        -------
        scala_intp : scala_interpreter.SparkInterpreter
        """
        if self._interp is None:
            assert isinstance(self.kernel, MetaKernel)
            self.kernel.Display(TextOutput("Intitializing scala interpreter...."))
            self._interp = get_scala_interpreter()
            # Ensure that spark is available in the python session as well.
            self.kernel.cell_magics['python'].env['spark'] = self._interp.spark_session
            self.kernel.cell_magics['python'].env['sc'] = self._interp.sc

            sc = self._interp.sc
            self.kernel.Display(TextOutput(dedent("""\
                Web ui available at {webui}
                Spark context available as 'sc' (master = {master}, app id = {app_id})
                Spark session available as 'spark'
                """.format(
                master=sc.master,
                app_id=sc.applicationId,
                webui=self._interp.web_ui_url
                )
            )))

            self._is_complete_ready = True
            self._interp.register_stdout_handler(self.kernel.Write)
            self._interp.register_stderr_handler(self.kernel.Error)
            # Set up the callbacks
            self._initialize_pipes()
        return self._interp

    def _initialize_pipes(self):
        self.kernel.log.info("Starting STDOUT/ERR callback")
        ioloop.IOLoop.current().spawn_callback(self._loop_alive)

    @gen.coroutine
    def _loop_alive(self):
        """This is a little hack to ensure that during the tornado eventloop we also run one iteration of the asyncio
        eventloop.

        """
        loop = self._interp.loop
        while True:
            loop.call_soon(loop.stop)
            loop.run_forever()
            yield gen.sleep(0.01)

    def line_scala(self, *args):
        """
        %scala CODE - evaluate code as Scala
        This line magic will evaluate the CODE (either expression or
        statement) as Scala code.

        Examples:
            %scala val x = 42
            %scala import scala.math
            %scala x + math.pi
        """
        code = " ".join(args)
        self.eval(code, True)

    def eval(self, code, raw):
        """Evaluate Scala code.

        Parameters
        ----------
        code: str
            Code to execute
        raw: bool
            Raw result of the eval, not wrapped by metakernel classes

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
        except ScalaException as e:
            resp = self.kernel.kernel_resp
            resp['status'] = 'error'
            tb = e.scala_message.split('\n')
            first = tb[0]
            assert isinstance(first, str)
            eclass, _, emessage = first.partition(':')
            # Include the entire traceback for notebook use
            return ExceptionWrapper(eclass, emessage, tb)

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
                self.eval(self.code, False)
                # self.code = str(self.env["retval"]) if ("retval" in self.env and
                #                                         self.env["retval"] != None) else ""
                self.retval = None
                #self.env["retval"] = None
                self.evaluate = True
            else:
                self.retval = self.eval(self.code, False)
                #self.env["retval"] = None
                self.evaluate = False

    def post_process(self, retval):
        if retval is not None:
            return retval
        else:
            return self.retval

    def get_completions(self, info):
        intp = self._get_scala_interpreter()
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
        self.kernel.log.debug("info %s\n    completions %s\n     final %s", info, c, a)
        return a

    def get_help_on(self, info, level=0, none_on_fail=False):
        intp = self._get_scala_interpreter()
        self.kernel.log.debug(info['help_obj'])
        # Calling this twice produces different output
        code = intp.complete(info['help_obj'], len(info['help_obj']))
        code = intp.complete(info['help_obj'], len(info['help_obj']))
        self.kernel.log.debug(code)
        return '\n'.join(code)


def register_magics(kernel):
    kernel.register_magics(ScalaMagic)
