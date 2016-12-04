from __future__ import absolute_import, print_function, division

import os
import traceback

import spylon.spark.launcher
import tempfile
import shutil
import atexit
import signal
from metakernel import MetaKernel
import sys
from metakernel import Magic


spark_session = None
spark_jvm_helpers = None
scala_intp = None


class InitSparkContext(Magic):

    def cell_init_spark(self):
        """
        %%init_spark - start up a spark context with a custom configuration

        Example:
            %%init_spark
            launcher.jars = ["file://some/jar.jar"]
            launcher.master = "local[4]"
            launcher.conf.spar.executor.cores = 8

        This will evaluate the launcher args using spylon.
        """
        globals_dict = {'launcher': spylon.spark.launcher.SparkConfiguration()}
        eval(self.code, globals=globals_dict)
        conf = globals_dict['launcher']

        init_spark_session(conf)


def init_spark_session(conf=None, application_name="ScalaMetaKernel"):
    global spark_session
    if conf is None:
        conf = spylon.spark.launcher.SparkConfiguration()
    spark_coontext = conf.spark_context(application_name)
    from pyspark.sql import SparkSession
    spark_session = SparkSession(spark_coontext)
    from spylon.spark.utils import SparkJVMHelpers
    global spark_jvm_helpers
    spark_jvm_helpers = SparkJVMHelpers(spark_session._sc)
    # TODO : Capturing the STDERR / STDOUT from the java process requires us to hook in with gdb and duplicate the pipes
    #        This is not particularly pretty


def initialize_scala_kernel():
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
    n = io.BufferedReader._java_lang_class.cast(None)
    Option = jvm.scala.Option
    scala_none = Option.apply(n)
    k = jvm.org.apache.spark.repl.SparkILoop

    jprintWriter = io.PrintWriter(bytes_out, True)

    # iloop = k(scala_none, jprintWriter)
    # jin = io.BufferedReader(io.InputStreamReader(io.ByteArrayInputStream()));

    # iloop = k(Option.apply(jin), jprintWriter)


    """
    val jars = Utils.getUserJars(conf, isShell=true).mkString(File.pathSeparator)
    val interpArguments = List(
        "-Yrepl-class-based",
        "-Yrepl-outdir", s
    "${outputDir.getAbsolutePath}",
    "-classpath", jars
    ) ++ args.toList

    val
    settings = new GenericRunnerSettings(scalaOptionError)
    settings.processArguments(interpArguments, true)
    """

    execUri = jvm.System.getenv("SPARK_EXECUTOR_URI")
    jconf.setIfMissing("spark.app.name", "Spark shell")
    # // SparkContext will detect this configuration and register it with the RpcEnv's
    # // file server, setting spark.repl.class.uri to the actual URI for executors to
    # // use. This is sort of ugly but since executors are started as part of SparkContext
    # // initialization in certain cases, there's an initialization order issue that prevents
    # // this from being set after SparkContext is instantiated.

    output_dir = os.path.abspath(tempfile.mkdtemp())
    def cleanup():
        shutil.rmtree(output_dir, True)
    atexit.register(cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    jconf.set("spark.repl.class.outputDir", output_dir)
    if (execUri is not None):
      jconf.set("spark.executor.uri", execUri)


    jars = jvm.org.apache.spark.util.Utils.getUserJars(jconf, True).mkString(":")
    interpArguments = spark_jvm_helpers.to_scala_list(
        ["-Yrepl-class-based", "-Yrepl-outdir", output_dir,
         "-classpath", jars
         ]
    )


    # settings = jvm. scala.tools.nsc.GenericRunnerSettings()
    settings = jvm.scala.tools.nsc.Settings()
    settings.processArguments(interpArguments, True)

    # start the interpreter
    # getattr(iloop, "settings_$eq")(settings)

    def start_imain():
        intp = jvm.scala.tools.nsc.interpreter.IMain(settings, jprintWriter)
        intp.initializeSynchronous()
        # TODO:


        """
        System.setOut(new PrintStream(new File("output-file.txt")));

        """

        # Copied directly from Spark
        intp.interpret("""
            @transient val spark = if (org.apache.spark.repl.Main.sparkSession != null) {
                org.apache.spark.repl.Main.sparkSession
              } else {
                org.apache.spark.repl.Main.createSparkSession()
              }
            @transient val sc = {
              val _sc = spark.sparkContext
              if (_sc.getConf.getBoolean("spark.ui.reverseProxy", false)) {
                val proxyUrl = _sc.getConf.get("spark.ui.reverseProxyUrl", null)
                if (proxyUrl != null) {
                  println(s"Spark Context Web UI is available at ${proxyUrl}/proxy/${_sc.applicationId}")
                } else {
                  println(s"Spark Context Web UI is available at Spark Master Public URL")
                }
              } else {
                _sc.uiWebUrl.foreach {
                  webUrl => println(s"Spark context Web UI available at ${webUrl}")
                }
              }
              println("Spark context available as 'sc' " +
                s"(master = ${_sc.master}, app id = ${_sc.applicationId}).")
              println("Spark session available as 'spark'.")
              _sc
            }
            """)
        intp.interpret("import org.apache.spark.SparkContext._")
        intp.interpret("import spark.implicits._")
        intp.interpret("import spark.sql")
        intp.interpret("import org.apache.spark.sql.functions._")
        bytes_out.reset()
        return intp


    def start_manual():
        iloop.createInterpreter()
        iloop.intp().initializeSynchronous()

        Future = getattr(getattr(jvm.scala.concurrent, "Future$"), "MODULE$")
        method_name = "scala$tools$nsc$interpreter$ILoop$$globalFuture_$eqscala$tools$nsc$interpreter$ILoop$$globalFuture_$eq"
        m = getattr(iloop,
                "scala$tools$nsc$interpreter$ILoop$$globalFuture_$eqscala$tools$nsc$interpreter$ILoop$$globalFuture_$eq")
        C = iloop.getClass()
        mm = list(C.getDeclaredMethods())
        dummyFuture = Future.successful(True)
        FutureTrait = jvm.scala.concurrent.Future

        casted = FutureTrait._java_lang_class.cast(dummyFuture)
        m(casted)

        iloop.loadFiles(settings)
        iloop.loopPostInit()
        iloop.printWelcome()

    def start_auto():
        iloop.process(settings)

    imain = start_imain()

    return _SparkILoopWrapper(jvm, imain, bytes_out)


def _scala_seq_to_py(jseq):
    n = jseq.size()
    for i in range(n):
        yield jseq.apply(i)


class ScalaException(Exception):

    def __init__(self, scala_message, *args, **kwargs):
        super(ScalaException, self).__init__(*args, **kwargs)
        self.scala_message = scala_message


class _SparkILoopWrapper(object):

    def __init__(self, jvm, jiloop, jbyteout):
        self._jcompleter = None
        self.jvm = jvm
        self.jiloop = jiloop

        interpreterPkg = getattr(getattr(self.jvm.scala.tools.nsc.interpreter, 'package$'), "MODULE$")
        # = spark_jvm_helpers.import_scala_package_object("scala.tools.nsc.interpreter")
        dir(interpreterPkg)
        self.iMainOps = interpreterPkg.IMainOps(jiloop)
        self.jbyteout = jbyteout

    def interpret(self, code, synthetic=False):
        try:
            res = self.jiloop.interpret(code, synthetic)
            pyres = self.jbyteout.toByteArray()

            result = res.toString().encode("utf-8")
            if result == "Success":
                return pyres
            elif result == 'Error':
                raise ScalaException(pyres)
            elif result == 'Incomplete':
                raise ScalaException(pyres)
            return pyres.decode("utf-8")
        finally:
            self.jbyteout.reset()

    @property
    def jcompleter(self):
        if self._jcompleter is None:
            jClass = self.jvm.scala.tools.nsc.interpreter.PresentationCompilerCompleter
            self._jcompleter = jClass(self.jiloop)
        return self._jcompleter


    def complete(self, code, pos):
        """

        Parameters
        ----------
        code : str
        pos : int

        Returns
        -------
        List[str]
        """
        c = self.jcompleter
        print(dir(self.jcompleter))
        jres = c.complete(code, pos)
        return list(_scala_seq_to_py(jres.candidates()))

    def is_complete(self, code):
        try:
            res = self.jiloop.parse.apply(code)
            # TODO: Finish this up.

        finally:
            self.jbyteout.reset()

    def get_help_on(self, info):
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
        return self.jiloop.helpSummary()



def get_scala_interpreter():
    global scala_intp
    if scala_intp is None:
        scala_intp = initialize_scala_kernel()

    return scala_intp


class TextOutput(object):
    """Wrapper for text output wose repr is the text itself.

    This avoids the repr(output) quoting our output strings.

    Notes
    -----
    Adapted from eclairjs-kernel
    """
    def __init__(self, output):
        self.output = output

    def __repr__(self):
        return self.output

class MetaKernelScala(MetaKernel):
    implementation = 'SparkScala Jupyter'
    implementation_version = '1.0'
    language = 'scala'
    language_version = '0.1'
    banner = "MetaKernel Scala - evaluates Scala statements and expressions."
    language_info = {
        'mimetype': 'text/x-scala',
        'name': 'scala',
        # ------ If different from 'language':
        'codemirror_mode': "text/x-scala",
        'pygments_lexer': 'scala',
        # 'version'       : "x.y.z",
        'file_extension': '.scala',
        'help_links': MetaKernel.help_links,
    }
    kernel_json = {
        "argv": [
            sys.executable, "-m", "metakernel_scalaspark", "-f", "{connection_file}"],
        "display_name": "MetaKernel Scala Spark",
        "env": {
            "SPARK_SUBMIT_OPTS": "-Dscala.usejavacp=true",
            "PYTHONUNBUFFERED": "1",
        },
        "language": "scala",
        "name": "metakernel_scala"
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._interp = None

    @property
    def pythonmagic(self):
        return self.line_magics['python']

    def get_usage(self):
        return ("This is MetaKernel Scala Spark. It implements a Scala interpreter.")

    def set_variable(self, name, value):
        """
        Set a variable in the kernel language.
        """




        # python_magic = self.line_magics['python']
        # python_magic.env[name] = value

    def _get_scala_interpreter(self):
        if self._interp is None:
            self.Display("Intitializing scala interpreter....")
            self._interp = get_scala_interpreter()
            self.Display("Scala interpreter initialized.")
            self.pythonmagic.env['spark'] = spark_session
            self.Display("Registered spark session in scala and python context as `spark`")
        return self._interp



    def get_variable(self, name):
        """
        Get a variable from the kernel language.
        """
        # python_magic = self.line_magics['python']
        # return python_magic.env.get(name, None)

    def do_execute_direct(self, code, silent=False):
        intp = self._get_scala_interpreter()
        try:
            return TextOutput(intp.interpret(code.strip()))
        except ScalaException as e:
            return self.Error(e.scala_message)
        #python_magic = self.line_magics['python']
        #return python_magic.eval(code.strip())

    def get_completions(self, info):
        intp = self._get_scala_interpreter()
        # raise Exception(repr(info))
        return intp.complete(info['code'], info['help_pos'])

        python_magic = self.line_magics['python']
        return python_magic.get_completions(info)

    def get_kernel_help_on(self, info, level=0, none_on_fail=False):
        # python_magic = self.line_magics['python']
        # return python_magic.get_help_on(info, level, none_on_fail)
        intp =self._get_scala_interpreter()
        self.log.critical(info['help_obj'])
        # Calling this twice produces differnt output
        code = intp.complete(info['help_obj'], len(info['help_obj']))
        code = intp.complete(info['help_obj'], len(info['help_obj']))
        self.log.critical(code)
        return '\n'.join(code)


def register_magics(kernel):
    kernel.register_magics(InitSparkContext)


#TODO: Comm api style thing.  Basically we just need a server listening on a port that we can push stuff to.

# localhost:PORT/output
# {
#     "output_id": "string",
#     "mimetype": "plain",
#     "data": object()
# }


