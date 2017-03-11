import asyncio
import atexit
import logging
import os
import pathlib
import shutil
import signal
import tempfile

from asyncio import Future
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Union, List, Any

import spylon.spark

spark_session = None
spark_jvm_helpers = None
scala_intp = None


def init_spark_session(conf: spylon.spark.SparkConfiguration=None,
                       application_name: str="ScalaMetaKernel"):
    """Initialize the Spark session.

    Parameters
    ----------
    conf: optional
        Spark configuration to apply to the session
    application_name: optional
        Name to give the session


    """
    # Ensure we have the correct classpath settings for the repl to work.
    os.environ.setdefault('SPARK_SUBMIT_OPTS', '-Dscala.usejavacp=true')

    global spark_session
    # If we have already initialized a spark session. Don't carry on.
    if spark_session:
        return
    if conf is None:
        conf = spylon.spark.launcher.SparkConfiguration()
    # SparkContext will detect this configuration and register it with the RpcEnv's
    # file server, setting spark.repl.class.uri to the actual URI for executors to
    # use. This is sort of ugly but since executors are started as part of SparkContext
    # initialization in certain cases, there's an initialization order issue that prevents
    # this from being set after SparkContext is instantiated.
    output_dir = os.path.abspath(tempfile.mkdtemp())

    def cleanup():
        shutil.rmtree(output_dir, True)

    atexit.register(cleanup)
    signal.signal(signal.SIGTERM, cleanup)
    conf.conf.set("spark.repl.class.outputDir", output_dir)

    spark_context = conf.spark_context(application_name)
    from pyspark.sql import SparkSession
    spark_session = SparkSession(spark_context)
    from spylon.spark.utils import SparkJVMHelpers
    global spark_jvm_helpers
    # noinspection PyProtectedMember
    spark_jvm_helpers = SparkJVMHelpers(spark_session._sc)


def get_web_ui_url(sc):
    """Get the web ui for a spark context

    Parameters
    ----------
    sc : SparkContext

    Returns
    -------
    url : str
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
    Portions of this have been adapted out of Apache Toree and Apache Zeppelin

    Returns
    -------
    SparkInterpreter
    """
    if spark_session is None:
        init_spark_session()

    from spylon.spark.utils import SparkJVMHelpers
    assert isinstance(spark_jvm_helpers, SparkJVMHelpers)
    from pyspark.sql import SparkSession
    assert isinstance(spark_session, SparkSession)

    jvm = spark_session._jvm
    jconf = spark_session._jsc.getConf()
    bytes_out = jvm.org.apache.commons.io.output.ByteArrayOutputStream()

    io = jvm.java.io

    jprintWriter = io.PrintWriter(bytes_out, True)

    execUri = jvm.System.getenv("SPARK_EXECUTOR_URI")
    jconf.setIfMissing("spark.app.name", "Spark shell")
    if (execUri is not None):
        jconf.set("spark.executor.uri", execUri)
    output_dir = jconf.get("spark.repl.class.outputDir")

    jars = jvm.org.apache.spark.util.Utils.getUserJars(jconf, True).mkString(":")
    interpArguments = spark_jvm_helpers.to_scala_list(
        ["-Yrepl-class-based", "-Yrepl-outdir", output_dir,
         "-classpath", jars
         ]
    )

    settings = jvm.scala.tools.nsc.Settings()
    settings.processArguments(interpArguments, True)

    # Since we have already instantiated our spark context on the python side,
    # set it in the Main repl class as well
    Main = jvm.org.apache.spark.repl.Main
    jspark_session = spark_session._jsparkSession
    # equivalent to Main.sparkSession = jspark_session
    getattr(Main, "sparkSession_$eq")(jspark_session)
    getattr(Main, "sparkContext_$eq")(jspark_session.sparkContext())

    def start_imain():
        intp = jvm.scala.tools.nsc.interpreter.IMain(settings, jprintWriter)
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
        intp.interpret("import org.apache.spark.SparkContext._")
        intp.interpret("import spark.implicits._")
        intp.interpret("import spark.sql")
        intp.interpret("import org.apache.spark.sql.functions._")
        bytes_out.reset()
        return intp

    imain = start_imain()
    return SparkInterpreter(jvm, imain, bytes_out)


def _scala_seq_to_py(jseq):
    n = jseq.size()
    for i in range(n):
        yield jseq.apply(i)


class ScalaException(Exception):
    def __init__(self, scala_message, *args, **kwargs):
        super(ScalaException, self).__init__(scala_message, *args, **kwargs)
        self.scala_message = scala_message


tOutputHandler = Callable[[List[Any]], None]


class SparkInterpreter(object):
    """Wrapper for a scala interpreter.

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
    loop : asyncio.AbstractEventLoop, optional
        Asyncio eventloop

    """
    executor = ThreadPoolExecutor(4)

    def __init__(self, jvm, jimain, jbyteout, loop: Union[None, asyncio.AbstractEventLoop]=None):
        self.spark_session = spark_session
        # noinspection PyProtectedMember
        self.sc = spark_session._sc
        self.web_ui_url = get_web_ui_url(self.sc)
        self._jcompleter = None
        self.jvm = jvm
        self.jimain = jimain
        if loop is None:
            # TODO: We may want to use new_event_loop here to avoid stopping and starting the main one.
            loop = asyncio.get_event_loop()
        self.loop = loop
        self.log = logging.getLogger(self.__class__.__name__)

        jinterpreter_package = getattr(getattr(self.jvm.scala.tools.nsc.interpreter, 'package$'), "MODULE$")
        self.iMainOps = jinterpreter_package.IMainOps(jimain)
        self.jbyteout = jbyteout

        tempdir = tempfile.mkdtemp()
        atexit.register(shutil.rmtree, tempdir, True)
        self.tempdir = tempdir
        # Handlers for dealing with stout and stderr.  This allows us to insert additional behavior for magics
        self._stdout_handlers = []
        self._stderr_handlers = []
        self._initialize_stdout_err()

    def register_stdout_handler(self, handler: tOutputHandler):
        self._stdout_handlers.append(handler)

    def register_stderr_handler(self, handler: tOutputHandler):
        self._stderr_handlers.append(handler)

    def _initialize_stdout_err(self):
        stdout_file = os.path.abspath(os.path.join(self.tempdir, 'stdout'))
        stderr_file = os.path.abspath(os.path.join(self.tempdir, 'stderr'))
        # Start up the pipes on the JVM side

        self.log.info("Before Java redirected")
        self.log.debug("stdout/err redirected to %s", self.tempdir)
        code = 'Console.set{pipe}(new PrintStream(new FileOutputStream(new File(new java.net.URI("{filename}")), true)))'
        code = '\n'.join([
            'import java.io.{PrintStream, FileOutputStream, File}',
            'import scala.Console',
            code.format(pipe="Out", filename=pathlib.Path(stdout_file).as_uri()),
            code.format(pipe="Err", filename=pathlib.Path(stderr_file).as_uri())
        ])
        o = self.interpret(code)
        self.log.info("Console redirected, %s", o)

        self.loop.create_task(self._poll_file(stdout_file, self.handle_stdout))
        self.loop.create_task(self._poll_file(stderr_file, self.handle_stderr))

    def handle_stdout(self, *args) -> None:
        for handler in self._stdout_handlers:
            handler(*args)

    def handle_stderr(self, *args) -> None:
        for handler in self._stderr_handlers:
            handler(*args)

    async def _poll_file(self, filename: str, fn: Callable[[Any], None]):
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
                # processing a line from the file and running our processing function.
                fn(line)
                # self.log.critical("AFTER PUSH")
                await asyncio.sleep(0, loop=self.loop)
            else:
                await asyncio.sleep(0.01, loop=self.loop)

    def _interpret_sync(self, code: str, synthetic=False):
        """Interpret a block of scala code.

        If you want to get the result as a python object, follow this with a
        call to `last_result()`

        Parameters
        ----------
        code : str
        synthetic : bool

        Returns
        -------
        reploutput : str
            String output from the scala REPL.
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
                raise ScalaException(pyres)
            return pyres
        finally:
            self.jbyteout.reset()

    async def _interpret_async(self, code: str, future: Future):
        """Async execute for running a block of scala code.

        Parameters
        ----------
        code : str
        future : Future
            future used to hold the result of the computation.
        """
        try:
            result = await self.loop.run_in_executor(self.executor, self._interpret_sync, code)
            future.set_result(result)
        except Exception as e:
            future.set_exception(e)
        return

    def interpret(self, code: str):
        """Interpret a block of scala code.

        If you want to get the result as a python object, follow this will a call to `last_result()`

        Parameters
        ----------
        code : str

        Returns
        -------
        reploutput : str
            String output from the scala REPL.
        """
        fut = asyncio.Future(loop=self.loop)
        asyncio.ensure_future(self._interpret_async(code, fut), loop=self.loop)
        res = self.loop.run_until_complete(fut)
        return res

    def last_result(self):
        """Retrieves the jvm result object from the previous call to interpret.

        If the result is a supported primitive type it is converted to a python object, otherwise it returns a py4j
        view onto that object.

        Returns
        -------
        object
        """
        # TODO : when evaluating multiline expressions this returns the first result
        lr = self.jimain.lastRequest()
        res = lr.lineRep().call("$result", spark_jvm_helpers.to_scala_list([]))
        return res

    def bind(self, name: str, value: Any, jtyp: str="Any"):
        """Set a variable in the scala repl environment to a python valued type.

        Parameters
        ----------
        name : str
        value : Any
        jtyp : str
            String representation of the Java type that we want to cast this as.

        """
        modifiers = spark_jvm_helpers.to_scala_list(["@transient"])
        # Ensure that the value that we are trying to set here is a compatible type on the java side
        # Import is here due to lazily instantiating the SparkContext
        from py4j.java_gateway import JavaClass, JavaObject, JavaMember
        compatible_types = (
            int, str, bytes, bool, list, dict, JavaClass, JavaMember, JavaObject
        )
        if isinstance(value, compatible_types):
            self.jimain.bind(name, "Any", value, modifiers)

    @property
    def jcompleter(self):
        if self._jcompleter is None:
            jClass = self.jvm.scala.tools.nsc.interpreter.PresentationCompilerCompleter
            self._jcompleter = jClass(self.jimain)
        return self._jcompleter

    def complete(self, code: str, pos: int) -> List[str]:
        """Performs code completion for a block of scala code.

        Parameters
        ----------
        code : str
            Scala code to perform completion on
        pos : int
            Cursor position

        Returns
        -------
        List[str]
        """
        c = self.jcompleter
        jres = c.complete(code, pos)
        return list(_scala_seq_to_py(jres.candidates()))

    def is_complete(self, code):
        """Determine if a hunk of code is a complete block of scala.

        Parameters
        ----------
        code : str

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

    def get_help_on(self, info):
        """For a given symbol attempt to get some minor help on it in terms of function signature.

        Due to the JVM having no runtime docstring information, the level of detail we can retrieve is rather limited.

        Parameters
        ----------
        info : str
            object name to try and get information for

        Returns
        -------
        str

        """
        code = info + '// typeAt {} {}'.format(0, len(info))
        scala_type = self.complete(code, len(code))
        # When using the // typeAt hint we will get back a list made by
        # "" :: type :: Nil
        # according to https://github.com/scala/scala/blob/2.12.x/src/repl/scala/tools/nsc/interpreter/PresentationCompilerCompleter.scala#L52
        assert len(scala_type) == 2
        # TODO: Given that we have a type here we can interpret some java class reflection to see if we can get some
        #       better results for the function in question
        return scala_type[-1]

    def printHelp(self):
        return self.jimain.helpSummary()


def get_scala_interpreter():
    """Get the scala interpreter instance.

    If the instance has not yet been created, create it.

    Returns
    -------
    scala_intp : SparkInterpreter
    """
    global scala_intp
    if scala_intp is None:
        scala_intp = initialize_scala_interpreter()

    return scala_intp
