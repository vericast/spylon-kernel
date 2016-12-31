from __future__ import absolute_import, print_function, division

import sys
import weakref

from metakernel import MetaKernel
from .init_spark_magic import InitSparkMagic
from .scala_magic import ScalaMagic
from tempfile import mkdtemp
import shutil
import os
import threading
import time


class SpylonKernel(MetaKernel):
    implementation = 'spylon-kernel'
    implementation_version = '1.0'
    language = 'scala'
    language_version = '0.1'
    banner = "spylon-kernel - evaluates Scala statements and expressions."
    language_info = {
        'mimetype': 'text/x-scala',
        'name': 'scala',
        # ------ If different from 'language':
        'codemirror_mode': "text/x-scala",
        'pygments_lexer': 'scala',
        # 'version'       : "x.y.z",
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
        super(SpylonKernel, self).__init__(*args, **kwargs)
        self.register_magics(ScalaMagic)
        self.register_magics(InitSparkMagic)
        self.tempdir = mkdtemp()
        self._is_complete_ready = False
        magic = self.line_magics['scala']
        assert isinstance(magic, ScalaMagic)
        magic._after_start_interpreter.append(self._initialize_pipes)
        magic._after_start_interpreter.append(lambda: setattr(self, "_is_complete_ready", True))

    def __del__(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    @property
    def pythonmagic(self):
        return self.line_magics['python']

    def get_usage(self):
        return ("This is spylon-kernel. It implements a Scala interpreter.")

    def set_variable(self, name, value):
        """
        Set a variable in the kernel language.
        """
        # python_magic = self.line_magics['python']
        # python_magic.env[name] = value

    def get_variable(self, name):
        """
        Get a variable from the kernel language.
        """
        # python_magic = self.line_magics['python']
        # return python_magic.env.get(name, None)

    def do_execute_direct(self, code, silent=False):
        magic = self.line_magics['scala']
        return magic.eval(code.strip(), raw=False)

    def get_completions(self, info):
        magic = self.line_magics['scala']
        return magic.get_completions(info)

    def get_kernel_help_on(self, info, level=0, none_on_fail=False):
        magic = self.line_magics['scala']
        return magic.get_help_on(info, level, none_on_fail)

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
        if code.startswith(self.magic_prefixes['magic']) or not self._is_complete_ready:
            ## force requirement to end with an empty line
            if code.endswith("\n"):
                return {'status' : 'complete', 'indent': ''}
            else:
                return {'status' : 'incomplete', 'indent': ''}
        # The scala interpreter can take a while to be alive, only use the fancy method when we dont need to lazily
        # instantiate the interpreter
        # otherwise, how to know is complete?
        magic = self.line_magics['scala']
        assert isinstance(magic, ScalaMagic)
        interp = magic._get_scala_interpreter()
        status = interp.is_complete(code)
        # TODO: Better indent
        return {'status': status, 'indent': ' ' * 4 if status == 'incomplete' else ''}

    def _initialize_pipes(self):
        STDOUT = os.path.abspath(os.path.join(self.tempdir, 'stdout'))
        STDERR = os.path.abspath(os.path.join(self.tempdir, 'stderr'))
        # Start up the pipes on the JVM side
        magic = self.line_magics['scala']

        self.log.critical("Before Java redirected")
        code = 'Console.set{pipe}(new PrintStream(new FileOutputStream(new File("{filename}"), true)))'
        code = '\n'.join([
            'import java.io.{PrintStream, FileOutputStream, File}',
            'import scala.Console',
            code.format(pipe="Out", filename=STDOUT),
            code.format(pipe="Err", filename=STDERR)
        ])
        o = magic.eval(code, raw=True)
        self.log.critical("Console redirected")

        start_watcher_thread(STDOUT, self, "Write")
        start_watcher_thread(STDERR, self, "Error")


def start_watcher_thread(filename, self, method):
    t = threading.Thread(target=_monitor_pipe, args=(filename, weakref.ref(self), method))
    t.daemon = True
    t.start()


def _monitor_pipe(filename, weak_ref, method_name):
    """

    Parameters
    ----------
    filename : str
    weak_fn : weakref.ref
        Weakref to a function pointer.
    """
    fd = open(filename, 'r')
    while True:
        self = weak_ref()
        if self is None:
            break
        fn = getattr(self, method_name)
        line = fd.readline()
        if line:
            fn(line)
        else:
            time.sleep(0.1)
        del self
    print("Shutting down thread")

# TODO: Comm api style thing.  Basically we just need a server listening on a port that we can push stuff to.

# localhost:PORT/output
# {
#     "output_id": "string",
#     "mimetype": "plain",
#     "data": object()
# }


