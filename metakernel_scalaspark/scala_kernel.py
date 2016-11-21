from __future__ import absolute_import, print_function, division

import os
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

    iloop = k(scala_none, jprintWriter)
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
    settings = jvm. scala.tools.nsc.Settings()
    settings.processArguments(interpArguments, True)

    # start the interpreter
    getattr(iloop, "settings_$eq")(settings)

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
    start_manual()

    return _SparkILoopWrapper(jvm, iloop, bytes_out)


def _scala_seq_to_py(jseq):
    n = jseq.size()
    for i in range(n):
        yield jseq.apply(i)


class _SparkILoopWrapper(object):

    def __init__(self, jvm, jiloop, jbyteout):
        self._jcompleter = None
        self.jvm = jvm
        self.jiloop = jiloop
        self.jbyteout = jbyteout

    def interpret(self, code, synthetic=False):
        try:
            res = self.jiloop.interpret(code, synthetic)

            result = res.toString
            if result == "Success":
                return self.jbyteout.toByteArray()
            elif result == 'Error':
                raise Exception(self.jbyteout.toByteArray())
            elif result == 'Incomplete':
                raise Exception(self.jbyteout.toByteArray())

        finally:
            self.jbyteout.reset()

    def jcompleter(self):
        if self._jcompleter is None:
            jClass = self.jvm.scala.tools.nsc.interpreter.PresentationCompilerCompleter
            self._jcompleter = jClass(self.jiloop.intp())
        return self._jcompleter


    def complete(self, code, pos):
        """

        :param code:
        :param pos:
        :return:
        """
        jres = self._jcompleter.completer(code, pos)
        return list(_scala_seq_to_py(jres.candidates))

    def is_complete(self, code):
        try:
            res = self.jiloop.parse.apply(code)

        finally:
            self.jbyteout.reset()

def get_scala_interpreter():
    global scala_intp
    if scala_intp is None:
        scala_intp = initialize_scala_kernel()
    return scala_intp



class MetaKernelScala(MetaKernel):
    implementation = 'SparkScala Jupyter'
    implementation_version = '1.0'
    language = 'scala'
    language_version = '0.1'
    banner = "MetaKernel Scala - evaluates Scaal statements and expressions"
    language_info = {
        'mimetype': 'text/x-scala',
        'name': 'scala',
        # ------ If different from 'language':
        # 'codemirror_mode': {
        #    "version": 2,
        #    "name": "ipython"
        # }
        # 'pygments_lexer': 'language',
        # 'version'       : "x.y.z",
        'file_extension': '.scala',
        'help_links': MetaKernel.help_links,
    }
    kernel_json = {
        "argv": [
            sys.executable, "-m", "metakernel_scalaspark", "-f", "{connection_file}"],
        "display_name": "MetaKernel Python",
        "language": "scala",
        "name": "metakernel_scala"
    }

    def get_usage(self):
        return ("This is MetaKernel Scala Spark. It implements a Scala interpreter.")

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
        intp = get_scala_interpreter()
        return intp.interpret(code.strip())
        python_magic = self.line_magics['python']
        return python_magic.eval(code.strip())

    def get_completions(self, info):
        intp = get_scala_interpreter()
        return intp.complete(info['code'], info['cursor'])

        python_magic = self.line_magics['python']
        return python_magic.get_completions(info)

    def get_kernel_help_on(self, info, level=0, none_on_fail=False):
        # python_magic = self.line_magics['python']
        # return python_magic.get_help_on(info, level, none_on_fail)

        code = info + '*typeAt *{} *{} *'.format(0, len(info))
        return self.intp.complete(code, len(code))


def register_magics(kernel):
    kernel.register_magics(InitSparkContext)


if __name__ == '__main__':
    initialize_scala_kernel()
    # MetaKernelScala.run_as_main()




#TODO: Comm api style thing.  Basically we just need a server listening on a port that we can push stuff to.

# localhost:PORT/output
# {
#     "output_id": "string",
#     "mimetype": "plain",
#     "data": object()
# }


