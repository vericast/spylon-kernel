"""Jupyter Scala + Spark kernel built on Calysto/metakernel"""
import sys

from metakernel import MetaKernel
from .init_spark_magic import InitSparkMagic
from .scala_interpreter import ScalaException, ScalaInterpreter
from .scala_magic import ScalaMagic
from ._version import get_versions


class SpylonKernel(MetaKernel):
    """Jupyter kernel that supports code evaluation using the Scala REPL
    via py4j.

    Currently uses a ScalaMagic instance as a bridge to a `ScalaInterpreter`
    to let that functionality remain separate for reuse outside the kernel.
    """
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
        # Register the %%scala and %%init_spark magics
        # The %%scala one is here only because this classes uses it
        # to interact with the ScalaInterpreter instance
        self.register_magics(ScalaMagic)
        self.register_magics(InitSparkMagic)
        self._scalamagic = self.line_magics['scala']

    @property
    def scala_interpreter(self):
        """Gets the `ScalaInterpreter` instance associated with the ScalaMagic
        for direct use.

        Returns
        -------
        ScalaInterpreter
        """
        # noinspection PyProtectedMember
        intp = self._scalamagic._get_scala_interpreter()
        assert isinstance(intp, ScalaInterpreter)
        return intp

    def get_usage(self):
        """Gets usage information about the kernel itself.

        Implements the expected MetaKernel interface for this method.
        """
        return "This is spylon-kernel. It implements a Scala interpreter."

    def set_variable(self, name, value):
        """Sets a variable in the kernel language.

        Implements the expected MetaKernel interface for this method.

        Parameters
        ----------
        name : str
            Variable name
        value : any
            Variable value to set

        Notes
        -----
        Since metakernel calls this to bind kernel into the remote space we
        don't actually want that to happen. Simplest is just to have this
        flag as None initially. Furthermore the metakernel will attempt to
        set things like _i1, _i, _ii etc.  These we dont want in the kernel
        for now.
        """
        if self._scalamagic and (not name.startswith("_i")):
            self.scala_interpreter.bind(name, value)
        else:
            self.log.debug('Not setting variable %s', name)

    def get_variable(self, name):
        """Get a variable from the kernel as a Python-typed value.

        Implements the expected MetaKernel interface for this method.

        Parameters
        ----------
        name : str
            Scala variable name

        Returns
        -------
        value : any
            Scala variable value, tranformed to a Python type
        """
        if self._scalamagic:
            intp = self.scala_interpreter
            intp.interpret(name)
            return intp.last_result()

    def do_execute_direct(self, code, silent=False):
        """Executes code in the kernel language.

        Implements the expected MetaKernel interface for this method,
        including all positional and keyword arguments.

        Parameters
        ----------
        code : str
            Scala code to execute
        silent : bool, optional
            Silence output from this execution, ignored

        Returns
        -------
        any
            Result of the execution to be formatted for inclusion in
            a `execute_result` or `error` message from the kernel to
            frontends
        """
        try:
            res = self._scalamagic.eval(code.strip(), raw=False)
            if res:
                return res
        except ScalaException as e:
            return self.Error(e.scala_message)

    def get_completions(self, info):
        """Gets completions from the kernel based on the provided info.

        Implements the expected MetaKernel interface for this method.

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
        return self._scalamagic.get_completions(info)

    def get_kernel_help_on(self, info, level=0, none_on_fail=False):
        """Gets help text for the `info['help_obj']` identifier.

        Implements the expected MetaKernel interface for this method,
        including all positional and keyword arguments.

        Parameters
        ----------
        info : dict
            Information returned by `metakernel.parser.Parser.parse_code`
            including `help_obj`, etc.
        level : int, optional
            Level of help to request, 0 for basic, 1 for more, etc.
        none_on_fail : bool, optional
            Return none when code excution fails

        Returns
        -------
        str
            Help text
        """
        return self._scalamagic.get_help_on(info, level, none_on_fail)

    def do_is_complete(self, code):
        """Given code as string, returns a dictionary with 'status' representing
        whether code is ready to evaluate. Possible values for status are:

           'complete'   - ready to evaluate
           'incomplete' - not yet ready
           'invalid'    - invalid code
           'unknown'    - unknown; the default unless overridden

        Optionally, if 'status' is 'incomplete', you may indicate
        an indentation string.

        Parameters
        ----------
        code : str
            Scala code to check for completion

        Returns
        -------
        dict
            Status of the completion

        Example
        -------
        return {'status' : 'incomplete', 'indent': ' ' * 4}
        """
        # Handle magics and the case where the interpreter is not yet
        # instantiated. We don't want to create it just to do completion
        # since it will take a while to initialize and appear hung to the user.
        if code.startswith(self.magic_prefixes['magic']) or not self._scalamagic._is_complete_ready:
            # force requirement to end with an empty line
            if code.endswith("\n"):
                return {'status': 'complete', 'indent': ''}
            else:
                return {'status': 'incomplete', 'indent': ''}
        status = self.scala_interpreter.is_complete(code)
        # TODO: We can probably do a better job of detecting a good indent
        # level here by making use of a code parser such as pygments
        return {'status': status, 'indent': ' ' * 4 if status == 'incomplete' else ''}
