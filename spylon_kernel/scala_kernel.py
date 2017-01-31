from __future__ import absolute_import, print_function, division

import sys

from metakernel import MetaKernel
from metakernel.magics.python_magic import PythonMagic
from traitlets import Instance, Any

from spylon_kernel.scala_interpreter import ScalaException, SparkInterpreter
from .init_spark_magic import InitSparkMagic
from .scala_magic import ScalaMagic
from ._version import get_versions


class SpylonKernel(MetaKernel):
    implementation = 'spylon-kernel'
    implementation_version = get_versions()['version']
    language = 'scala'
    language_version = '2.11'
    banner = "spylon-kernel - evaluates Scala statements and expressions."
    language_info = {
        'mimetype': 'text/x-scala',
        'name': 'scala',
        'codemirror_mode': "text/x-scala",
        'pygments_lexer': 'scala',
        'file_extension': '.scala',
        'help_links': MetaKernel.help_links,
        'version': implementation_version,
    }
    kernel_json = {
        "argv": [
            sys.executable, "-m", "spylon_kernel", "-f", "{connection_file}"],
        "display_name": "spylon-kernel",
        "env": {
            "SPARK_SUBMIT_OPTS": "-Dscala.usejavacp=true",
            "PYTHONUNBUFFERED": "1",
        },
        "language": "scala",
        "name": "spylon-kernel"
    }

    def __init__(self, *args, **kwargs):
        self._scalamagic = None
        super(SpylonKernel, self).__init__(*args, **kwargs)
        self.register_magics(ScalaMagic)
        self.register_magics(InitSparkMagic)
        self._scalamagic = self.line_magics['scala']
        self._pythonmagic = self.line_magics['python']
        # assert isinstance(self._scalamagic, ScalaMagic)
        # assert isinstance(self._pythonmagic, PythonMagic)

    @property
    def pythonmagic(self):
        return self._pythonmagic

    @property
    def scala_interpreter(self):
        # noinspection PyProtectedMember
        intp = self._scalamagic._get_scala_interpreter()
        assert isinstance(intp, SparkInterpreter)
        return intp

    def get_usage(self):
        return "This is spylon-kernel. It implements a Scala interpreter."

    def set_variable(self, name, value):
        """
        Set a variable in the kernel language.
        """
        # Since metakernel calls this to bind kernel into the remote space we don't actually want that to happen.
        # Simplest is just to have this flag as None initially.
        # Furthermore the metakernel will attempt to set things like _i1, _i, _ii etc.  These we dont want in the kernel
        # for now.
        if self._scalamagic and (not name.startswith("_i")):
            self.scala_interpreter.bind(name, value)

    def get_variable(self, name):
        """
        Get a variable from the kernel as a Python-typed value.
        """
        if self._scalamagic:
            intp = self.scala_interpreter
            intp.interpret(name)
            return intp.last_result()

    def do_execute_direct(self, code, silent=False):
        try:
            res = self._scalamagic.eval(code.strip(), raw=False)
            if res:
                return res
        except ScalaException as e:
            return self.Error(e.scala_message)

    def get_completions(self, info):
        return self._scalamagic.get_completions(info)

    def get_kernel_help_on(self, info, level=0, none_on_fail=False):
        return self._scalamagic.get_help_on(info, level, none_on_fail)

    def do_is_complete(self, code):
        """
        Given code as string, returns dictionary with 'status' representing
        whether code is ready to evaluate. Possible values for status are:

           'complete'   - ready to evaluate
           'incomplete' - not yet ready
           'invalid'    - invalid code
           'unknown'    - unknown; the default unless overridden

        Optionally, if 'status' is 'incomplete', you may indicate
        an indentation string.

        Example:

            return {'status' : 'incomplete',
                    'indent': ' ' * 4}
        """
        if code.startswith(self.magic_prefixes['magic']) or not self._scalamagic._is_complete_ready:
            # force requirement to end with an empty line
            if code.endswith("\n"):
                return {'status': 'complete', 'indent': ''}
            else:
                return {'status': 'incomplete', 'indent': ''}
        # The scala interpreter can take a while to be alive, only use the fancy method when we don't need to lazily
        # instantiate the interpreter.
        status = self.scala_interpreter.is_complete(code)
        # TODO: We can probably do a better job of detecting a good indent level here by making use of a code parser
        #       such as pygments
        return {'status': status, 'indent': ' ' * 4 if status == 'incomplete' else ''}


# TODO: Comm api style thing.  Basically we just need a server listening on a port that we can push stuff to.
# localhost:PORT/output
# {
#     "output_id": "string",
#     "mimetype": "plain",
#     "data": object()
# }
