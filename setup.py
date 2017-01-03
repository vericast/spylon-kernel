from setuptools import setup, find_packages

version = '0.0.1'

try:
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
except ImportError:
    long_description = open('README.md').read()

setup(
    name='spylon-kernel',
    description='Jupyter metakernel for apache spark and scala',
    long_description=long_description,
    version=str(version),
    url='http://github.com/mariusvniekerk/spylon-kernel',
    requirements=['spylon', 'metakernel'],
    packages=list(find_packages()),
    author='Marius van Niekerk',
    author_email='marius.v.niekerk+spylon@gmail.com'
    maintainer='Marius van Niekerk',
    author_email='marius.v.niekerk+spylon@gmail.com'
    license="MIT",
)
