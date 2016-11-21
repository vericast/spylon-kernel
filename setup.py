from setuptools import setup, find_packages

version = '0.0.1'

setup(
    name='metakernel-scala-spark',
    description='Jupyter metakernel for apache spark and scala',
    version=str(version),
    url='',
    requirements=['spylon', 'metakernel'],
    packages=list(find_packages()),
)
