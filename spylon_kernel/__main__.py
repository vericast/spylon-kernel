"""Entrypoint for running the kernel process."""
from spylon_kernel import SpylonKernel
from tornado.ioloop import IOLoop

if __name__ == '__main__':
    IOLoop.configure("tornado.platform.asyncio.AsyncIOLoop")
    SpylonKernel.run_as_main()
