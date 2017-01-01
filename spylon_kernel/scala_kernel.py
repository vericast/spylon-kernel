from __future__ import absolute_import, print_function, division

import sys

import time
from metakernel import MetaKernel
from metakernel.process_metakernel import TextOutput

from spylon_kernel._scala_interpreter import ScalaException
from .init_spark_magic import InitSparkMagic
from .scala_magic import ScalaMagic
from tempfile import mkdtemp
import shutil
import os
from tornado import ioloop
from tornado import gen
import tornado.concurrent
from tornado.queues import Queue
from tornado.platform.asyncio import to_asyncio_future
import asyncio


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
        self._scalamagic = self.line_magics['scala']
        assert isinstance(self._scalamagic, ScalaMagic)
        self._scalamagic._after_start_interpreter.append(self._initialize_pipes)
        self._scalamagic._after_start_interpreter.append(lambda: setattr(self, "_is_complete_ready", True))

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

    async def execute_scala_async(self, code, future):
        intp = self._scalamagic._get_scala_interpreter()
        loop = asyncio.get_event_loop()
        try:
            result = await loop.run_in_executor(intp.executor, intp.interpret, code)
            # await to_asyncio_future(gen.sleep(0))
            self.log.critical("DONE??")
            future.set_result(result)
        except Exception as e:
            future.set_exception(e)
        return

    def do_execute_direct(self, code, silent=False):
        loop = asyncio.get_event_loop()
        try:
            fut = asyncio.Future()
            asyncio.ensure_future(self.execute_scala_async(code, fut))
            res = loop.run_until_complete(fut)


            #self.log.critical("res, %s", res)
            #assert type(res) == type(8)#
            if res:
                return TextOutput(res)
        except ScalaException as e:
            return self.Error(e.scala_message)

        #ioloop.IOLoop.instance().spawn_callback(self.execute_scala_async, code)
        #
        #
        # flag = True
        # res = None
        #
        # def callback(result):
        #     nonlocal flag
        #     nonlocal res
        #     flag = False
        #     res = result.result()
        #
        # ioloop.IOLoop.current().add_future(fut, callback=callback)
        #
        # while flag:
        #     time.sleep(0.1)
        # self.log.critical("Done, %s", res)
        #
        # return res

        #return magic.eval(code.strip(), raw=False)

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

        loop = asyncio.get_event_loop()
        loop.create_task(self._poll_file(STDOUT, self.Write))
        loop.create_task(self._poll_file(STDERR, self.Error))
        #loop.create_task(self._loop_alive())

        ioloop.IOLoop.current().spawn_callback(self._loop_alive)
        # self._poll_file, STDOUT, self.Write)
        # ioloop.IOLoop.current().spawn_callback(self._poll_file, STDERR, self.Error)

    @gen.coroutine
    def _loop_alive(self):
        """This is a little hack to ensure that during the tornado eventloop we also run one iteration of the asyncio
        eventloop.

        """
        loop = asyncio.get_event_loop()
        while True:
            loop.call_soon(loop.stop)
            loop.run_forever()
            yield gen.sleep(0.01)

    async def _poll_file(self, filename, fn):
        """

        Parameters
        ----------
        filename : str
        fn : (str) -> None
            Function to deal with string output.
        """
        fd = open(filename, 'r')
        while True:
            line = fd.readline()
            if line:
                self.log.critical("READ LINE from %s, %s", filename, line)
                fn(line)
                self.log.critical("AFTER PUSH")
                await asyncio.sleep(0)
            else:
                await asyncio.sleep(0.01)



# TODO: Comm api style thing.  Basically we just need a server listening on a port that we can push stuff to.

# localhost:PORT/output
# {
#     "output_id": "string",
#     "mimetype": "plain",
#     "data": object()
# }


