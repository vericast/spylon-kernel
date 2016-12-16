from setuptools import setup, find_packages

version = '0.0.1'

setup(
    name='spylon-kernel',
    description='Jupyter metakernel for apache spark and scala',
    version=str(version),
    url='http://github.com/mariusvniekerk/spylon-kernel',
    requirements=['spylon', 'metakernel'],
    packages=list(find_packages()),
)
