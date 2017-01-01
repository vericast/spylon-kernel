import spylon.spark
from metakernel import Magic
from spylon_kernel._scala_interpreter import init_spark_session

try:
    import jedi
    from jedi import Interpreter
    from jedi.api.helpers import completion_parts
    from jedi.parser.user_context import UserContext
except ImportError:
    jedi = None


class InitSparkMagic(Magic):

    def __init__(self, kernel):
        super(InitSparkMagic, self).__init__(kernel)
        self.env = globals()['__builtins__'].copy()
        self.env['launcher'] = spylon.spark.launcher.SparkConfiguration()

    def cell_init_spark(self):
        """
        %%init_spark - start up a spark context with a custom configuration

        Example:
            %%init_spark
            launcher.jars = ["file://some/jar.jar"]
            launcher.master = "local[4]"
            launcher.conf.spark.executor.cores = 8

        This will evaluate the launcher args using spylon.
        """
        if "__builtins__" not in self.env:
            ## __builtins__ get generated after an eval:
            eval("1", self.env)

        globals_dict = self.env
        exec(self.code, globals_dict)
        conf = globals_dict['launcher']
        init_spark_session(conf)
        self.evaluate = False
        self.kernel.Display()

    def get_completions(self, info):
        '''Get Python completions'''
        # https://github.com/davidhalter/jedi/blob/master/jedi/utils.py
        if jedi is None:
            return []

        text = info['code']
        interpreter = Interpreter(text, [self.env])

        position = (info['line_num'], info['column'])
        path = UserContext(text, position).get_path_until_cursor()
        path, dot, like = completion_parts(path)
        before = text[:len(text) - len(like)]

        completions = interpreter.completions()
        completions = [before + c.name_with_symbols for c in completions]

        return [c[info['start']:] for c in completions]


def register_magics(kernel):
    kernel.register_magics(InitSparkMagic)
