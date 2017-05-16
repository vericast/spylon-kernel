"""Scala interpreter supporting async I/O with the managing Python process."""
import asyncio
import atexit
import logging
import os
import pathlib
import shutil
import signal
import tempfile

from asyncio import Future
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Union, List, Any

import spylon.spark

# Global singletons
SparkState = namedtuple('SparkState', 'spark_session spark_jvm_helpers')
spark_state = None
scala_intp = None

# Default Spark application name
DEFAULT_APPLICATION_NAME = "spylon-kernel"


def init_spark(conf=None, application_name=None):
    """Initializes a SparkSession

    Parameters
    ----------
    conf: spylon.spark.SparkConfiguration, optional
        Spark configuration to apply to the session
    application_name: str, optional
        Name to give the session

    Returns
    -------
    SparkState namedtuple
    """
    global spark_state
    # If we have already initialized a spark session stop
    if spark_state:
        return spark_state

    # Ensure we have the correct classpath settings for the repl to work.
    os.environ.setdefault('SPARK_SUBMIT_OPTS', '-Dscala.usejavacp=true')

    if conf is None:
        conf = spylon.spark.launcher.SparkConfiguration()
    if application_name is None:
        application_name = DEFAULT_APPLICATION_NAME

    # Create a temp directory that gets cleaned up on exit
    output_dir = os.path.abspath(tempfile.mkdtemp())

    def cleanup():
        shutil.rmtree(output_dir, True)

    atexit.register(cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    # SparkContext will detect this configuration and register it with the RpcEnv's
    # file server, setting spark.repl.class.uri to the actual URI for executors to
    # use. This is sort of ugly but since executors are started as part of SparkContext
    # initialization in certain cases, there's an initialization order issue that prevents
    # this from being set after SparkContext is instantiated.
    conf.conf.set("spark.repl.class.outputDir", output_dir)

    # Create a new spark context using the configuration
    spark_context = conf.spark_context(application_name)

    # pyspark is in the python path after create the context
    from pyspark.sql import SparkSession
    from spylon.spark.utils import SparkJVMHelpers

    # Create the singleton SparkState
    spark_session = SparkSession(spark_context)
    spark_jvm_helpers = SparkJVMHelpers(spark_session._sc)
    spark_state = SparkState(spark_session, spark_jvm_helpers)
    return spark_state

def get_web_ui_url(sc):
    """Gets the URL of the Spark web UI for a given SparkContext.

    Parameters
    ----------
    sc : SparkContext

    Returns
    -------
    url : str
        URL to the Spark web UI
    """
    # Dig into the java spark conf to actually be able to resolve the spark configuration
    # noinspection PyProtectedMember
    conf = sc._jsc.getConf()
    if conf.getBoolean("spark.ui.reverseProxy", False):
        proxy_url = conf.get("spark.ui.reverseProxyUrl", "")
        if proxy_url:
            web_ui_url = "{proxy_url}/proxy/{application_id}".format(
                proxy_url=proxy_url, application_id=sc.applicationId)
        else:
            web_ui_url = "Spark Master Public URL"
    else:
        # For spark 2.0 compatibility we have to retrieve this from the scala side.
        joption = sc._jsc.sc().uiWebUrl()
        if joption.isDefined():
            web_ui_url = joption.get()
        else:
            web_ui_url = ""

    # Legacy compatible version for YARN
    yarn_proxy_spark_property = "spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES"
    if sc.master.startswith("yarn"):
        web_ui_url = conf.get(yarn_proxy_spark_property)

    return web_ui_url


# noinspection PyProtectedMember
def initialize_scala_interpreter():
    """
    Instantiates the scala interpreter via py4j and pyspark.

    Notes
    -----
    Portions of this have been adapted out of Apache Toree and Apache Zeppelin.

    Returns
    -------
    ScalaInterpreter
    """
    # Initialize Spark first if it isn't already
    spark_session, spark_jvm_helpers = init_spark()

    # Get handy JVM references
    jvm = spark_session._jvm
    jconf = spark_session._jsc.getConf()
    io = jvm.java.io

    # Build a print writer that'll be used to get results from the
    # Scala REPL
    bytes_out = jvm.org.apache.commons.io.output.ByteArrayOutputStream()
    jprint_writer = io.PrintWriter(bytes_out, True)

    # Set the Spark applicaiton name if it is not already set
    jconf.setIfMissing("spark.app.name", DEFAULT_APPLICATION_NAME)

    # Set the location of the Spark package on HDFS, if available
    exec_uri = jvm.System.getenv("SPARK_EXECUTOR_URI")
    if exec_uri is not None:
        jconf.set("spark.executor.uri", exec_uri)

    # Configure the classpath and temp directory created by init_spark
    # as the shared path for REPL generated artifacts
    output_dir = jconf.get("spark.repl.class.outputDir")
    jars = jvm.org.apache.spark.util.Utils.getUserJars(jconf, True).mkString(":")
    interp_arguments = spark_jvm_helpers.to_scala_list(
        ["-Yrepl-class-based", "-Yrepl-outdir", output_dir,
         "-classpath", jars, "-deprecation:false"
         ]
    )
    settings = jvm.scala.tools.nsc.Settings()
    settings.processArguments(interp_arguments, True)

    # Since we have already instantiated our SparkSession on the Python side,
    # share it with the Scala Main REPL class as well
    Main = jvm.org.apache.spark.repl.Main
    jspark_session = spark_session._jsparkSession
    # Equivalent to Main.sparkSession = jspark_session
    getattr(Main, "sparkSession_$eq")(jspark_session)
    getattr(Main, "sparkContext_$eq")(jspark_session.sparkContext())

    # Instantiate a Scala interpreter
    intp = jvm.scala.tools.nsc.interpreter.IMain(settings, jprint_writer)
    intp.initializeSynchronous()

    # Ensure that sc and spark are bound in the interpreter context.
    intp.interpret("""
        @transient val spark = if (org.apache.spark.repl.Main.sparkSession != null) {
            org.apache.spark.repl.Main.sparkSession
        } else {
            org.apache.spark.repl.Main.createSparkSession()
        }
        @transient val sc = {
            val _sc = spark.sparkContext
            _sc
        }
        """)
    # Import Spark packages for convenience
    intp.interpret("import org.apache.spark.SparkContext._")
    intp.interpret("import spark.implicits._")
    intp.interpret("import spark.sql")
    intp.interpret("import org.apache.spark.sql.functions._")
    # Clear the print writer stream
    bytes_out.reset()

    return ScalaInterpreter(jvm, intp, bytes_out)


def _scala_seq_to_py(jseq):
    """Generator for all elements in a Scala sequence.

    Parameters
    ----------
    jseq : Scala Seq
        Scala sequence

    Yields
    ------
    any
        One element per sequence
    """
    n = jseq.size()
    for i in range(n):
        yield jseq.apply(i)


class ScalaException(Exception):
    def __init__(self, scala_message, *args, **kwargs):
        super(ScalaException, self).__init__(scala_message, *args, **kwargs)
        self.scala_message = scala_message


class ScalaInterpreter(object):
    """Wrapper for a Scala interpreter.

    Notes
    -----
    Users should not instantiate this class themselves.  Use `get_scala_interpreter` instead.

    Parameters
    ----------
    jvm : py4j.java_gateway.JVMView
    jimain : py4j.java_gateway.JavaObject
        Java object representing an instance of `scala.tools.nsc.interpreter.IMain`
    jbyteout : py4j.java_gateway.JavaObject
        Java object representing an instance of `org.apache.commons.io.output.ByteArrayOutputStream`
        This is used to return output data from the REPL.
    log : logging.Logger
        Logger for this instance
    loop : asyncio.AbstractEventLoop, optional
        Asyncio eventloop
    web_ui_url : str
        URL of the Spark web UI associated with this interpreter
    """
    executor = ThreadPoolExecutor(1)

    def __init__(self, jvm, jimain, jbyteout, loop: Union[None, asyncio.AbstractEventLoop]=None):
        self.jvm = jvm
        self.jimain = jimain
        self.jbyteout = jbyteout
        self.log = logging.getLogger(self.__class__.__name__)

        if loop is None:
            # TODO: We may want to use new_event_loop here to avoid stopping
            # and starting the main one.
            loop = asyncio.get_event_loop()
        self.loop = loop

        # Store the state here so that clients of the instance
        # can access them (for now ...)
        self.sc = spark_state.spark_session._sc
        self.spark_session = spark_state.spark_session

        # noinspection PyProtectedMember
        self.web_ui_url = get_web_ui_url(self.sc)
        self._jcompleter = None

        # ???
        # jinterpreter_package = getattr(getattr(self.jvm.scala.tools.nsc.interpreter, 'package$'), "MODULE$")
        # self.iMainOps = jinterpreter_package.IMainOps(jimain)

        # Create a temp directory that will contain the stdout/stderr
        # files written by Scala and read by Python
        tempdir = tempfile.mkdtemp()
        atexit.register(shutil.rmtree, tempdir, True)

        # Handlers for dealing with stdout and stderr.
        self._stdout_handlers = []
        self._stderr_handlers = []
        self._initialize_stdout_err(tempdir)

    def register_stdout_handler(self, handler):
        """Registers a handler for the Scala stdout stream.

        Parameters
        ----------
        handler: callable(str) -> None
            Function to handle a stdout from the interpretter
        """
        self._stdout_handlers.append(handler)

    def register_stderr_handler(self, handler):
        """Registers a handler for the Scala stderr stream.

        Parameters
        ----------
        handler: callable(str) -> None
            Function to handle a stdout from the interpretter
        """
        self._stderr_handlers.append(handler)

    def _initialize_stdout_err(self, tempdir):
        """Redirects stdout/stderr in the Scala interpreter to two files in the
        given tempdir and begins async tasks to poll those files for lines of text.

        Parameters
        ----------
        tempdir : str
            Temporary directory
        """
        stdout_file = os.path.abspath(os.path.join(tempdir, 'stdout'))
        stderr_file = os.path.abspath(os.path.join(tempdir, 'stderr'))

        code = '@transient val _std{pipe} = new PrintStream(new FileOutputStream(new File(new java.net.URI("{filename}")), true))'
        code = '\n'.join([
            'import java.io.{PrintStream, FileOutputStream, File}',
            'import scala.Console',
            code.format(pipe="out", filename=pathlib.Path(stdout_file).as_uri()),
            code.format(pipe="err", filename=pathlib.Path(stderr_file).as_uri())
        ])
        self.interpret(code, redirect_streams=False)

        self.loop.create_task(self._poll_file(stdout_file, self.handle_stdout))
        self.loop.create_task(self._poll_file(stderr_file, self.handle_stderr))

    def handle_stdout(self, line):
        """Passes a line of Scala stdout to registered handlers.

        Parameters
        ----------
        line : str
            Line of text
        """
        for handler in self._stdout_handlers:
            handler(line)

    def handle_stderr(self, line):
        """Passes a line of Scala stderr to registered handlers.

        Parameters
        ----------
        line : str
            Line of text
        """
        for handler in self._stderr_handlers:
            handler(line)

    async def _poll_file(self, filename, fn):
        """Busy-polls a file for lines of text and passes them to
        the provided callback function when available.

        Parameters
        ----------
        filename : str
            Filename to poll for lines of text
        fn : callable(str) -> None
            Callback function that handles lines of text
        """
        fd = open(filename, 'r')
        while True:
            chars = fd.read(4096)
            if chars:
                fn(chars)
                await asyncio.sleep(0, loop=self.loop)
            else:
                await asyncio.sleep(0.01, loop=self.loop)

    def _interpret_sync(self, code, synthetic=False):
        """Synchronously interprets a Block of scala code and returns the
        string output from the Scala REPL.

        If you want to get the result as a Python object, follow this with a
        call to `last_result`.

        Parameters
        ----------
        code : str
            Scala code to interpret
        synthetic : bool, optional
            Use synthetic Scala classes (?)

        Returns
        -------
        str
            String output from the scala REPL

        Raises
        ------
        ScalaException
            When there is a problem interpreting the code
        """
        try:
            res = self.jimain.interpret(code, synthetic)
            pyres = self.jbyteout.toByteArray().decode("utf-8")
            # The scala interpreter returns a sentinel case class member here
            # which is typically matched via pattern matching.  Due to it
            # having a very long namespace, we just resort to simple string
            # matching here.
            result = res.toString()
            if result == "Success":
                return pyres
            elif result == 'Error':
                raise ScalaException(pyres)
            elif result == 'Incomplete':
                raise ScalaException(pyres or '<console>: error: incomplete input')
            return pyres
        finally:
            self.jbyteout.reset()

    async def _interpret_async(self, code, future):
        """Asynchronously interprets a block of Scala code and sets the
        output or exception as the result of the future.

        Parameters
        ----------
        code : str
            Scala code to interpret
        future : Future
            Future result or exception
        """
        try:
            result = await self.loop.run_in_executor(self.executor, self._interpret_sync, code)
            future.set_result(result)
        except Exception as e:
            future.set_exception(e)

    def interpret(self, code, redirect_streams=True):
        """Interprets a block of Scala code.

        Follow this with a call `last_result` to retrieve the result as a
        Python object.

        Parameters
        ----------
        code : str
            Scala code to interpret
        redirect_streams : bool, optional
            Force stdout/stderr to the polled files before executing
            `code`

        Returns
        -------
        str
            String output from the scala REPL

        Raises
        ------
        ScalaException
            When there is a problem interpreting the code
        """
        if redirect_streams:
            # Ensure stdout and stderr are going to the temp files before
            # executing the code.
            code = 'Console.setOut(_stdout)\nConsole.setErr(_stderr)\n' + code
        fut = asyncio.Future(loop=self.loop)
        asyncio.ensure_future(self._interpret_async(code, fut), loop=self.loop)
        res = self.loop.run_until_complete(fut)
        return res

    def last_result(self):
        """Retrieves the JVM result object from the preceeding call to `interpret`.

        If the result is a supported primitive type, convers it to a Python object.
        Otherwise, returns a py4j view onto that object.

        Returns
        -------
        object
        """
        # TODO : when evaluating multiline expressions this returns the first result
        lr = self.jimain.lastRequest()
        res = lr.lineRep().call("$result", spark_state.spark_jvm_helpers.to_scala_list([]))
        return res

    def bind(self, name, value, jtyp="Any"):
        """Set a variable in the Scala REPL to a Python valued type.

        Parameters
        ----------
        name : str
        value : Any
        jtyp : str
            String representation of the Java type that we want to cast this as.

        Returns
        -------
        bool
            True if the value is of one of the compatible types, False if not
        """
        modifiers = spark_state.spark_jvm_helpers.to_scala_list(["@transient"])
        # Ensure that the value that we are trying to set here is a compatible type on the java side
        # Import is here due to lazily instantiating the SparkContext
        from py4j.java_gateway import JavaClass, JavaObject, JavaMember
        compatible_types = (
            int, str, bytes, bool, list, dict, JavaClass, JavaMember, JavaObject
        )
        if isinstance(value, compatible_types):
            self.jimain.bind(name, jtyp, value, modifiers)
            return True
        return False

    @property
    def jcompleter(self):
        """Scala code completer.

        Returns
        -------
        scala.tools.nsc.interpreter.PresentationCompilerCompleter
        """
        if self._jcompleter is None:
            jClass = self.jvm.scala.tools.nsc.interpreter.PresentationCompilerCompleter
            self._jcompleter = jClass(self.jimain)
        return self._jcompleter

    def complete(self, code, pos):
        """Performs code completion for a block of Scala code.

        Parameters
        ----------
        code : str
            Scala code to perform completion on
        pos : int
            Cursor position

        Returns
        -------
        List[str]
            Candidates for code completion
        """
        c = self.jcompleter
        jres = c.complete(code, pos)
        return list(_scala_seq_to_py(jres.candidates()))

    def is_complete(self, code):
        """Determines if a chunk of code is a complete block of Scala.

        Parameters
        ----------
        code : str
            Code to evaluate for completeness

        Returns
        -------
        str
            One of 'complete', 'incomplete' or 'invalid'
        """
        try:
            res = self.jimain.parse().apply(code)
            output_class = res.getClass().getName()
            _, status = output_class.rsplit("$", 1)
            if status == 'Success':
                return 'complete'
            elif status == 'Incomplete':
                return 'incomplete'
            else:
                return 'invalid'
        finally:
            self.jbyteout.reset()

    def get_help_on(self, obj):
        """Gets the signature for the given object.

        Due to the JVM having no runtime docstring information, the level of
        detail is rather limited.

        Parameters
        ----------
        obj : str
            Object to fetch info about

        Returns
        -------
        str
            typeAt hint from Scala
        """
        code = obj + '// typeAt {} {}'.format(0, len(obj))
        scala_type = self.complete(code, len(code))
        # When using the // typeAt hint we will get back a list made by
        # "" :: type :: Nil
        # according to https://github.com/scala/scala/blob/2.12.x/src/repl/scala/tools/nsc/interpreter/PresentationCompilerCompleter.scala#L52
        assert len(scala_type) == 2
        # TODO: Given that we have a type here we can interpret some java class reflection to see if we can get some
        #       better results for the function in question
        return scala_type[-1]

def get_scala_interpreter():
    """Get the scala interpreter instance.

    If the instance has not yet been created, create it.

    Returns
    -------
    scala_intp : ScalaInterpreter
    """
    global scala_intp
    if scala_intp is None:
        scala_intp = initialize_scala_interpreter()

    return scala_intp
