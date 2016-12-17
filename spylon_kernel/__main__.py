from spylon_kernel import SpylonKernel

if __name__ == '__main__':

    ## For testing purposes we want to be able to run our kernel with coverage on.
    try:
        import coverage
        coverage.process_startup()
    except ImportError:
        pass

    SpylonKernel.run_as_main()
