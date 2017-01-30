from setuptools import setup, find_packages
import versioneer

try:
    try:
        import pypandoc
        long_description = pypandoc.convert('README.md', 'rst')
    except ImportError:
        long_description = open('README.md').read()
    with open("README.rst", 'w') as fo:
        fo.write(long_description)
except:
    pass

setup(
    name='spylon-kernel',
    description='Jupyter metakernel for apache spark and scala',
    long_description=open('README.rst').read(),
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    url='http://github.com/maxpoint/spylon-kernel',
    install_requires=['spylon[spark]', 'metakernel', 'jedi', 'tornado', 'ipykernel'],
    packages=list(find_packages()),
    author='Marius van Niekerk',
    author_email='marius.v.niekerk+spylon@gmail.com',
    maintainer='Marius van Niekerk',
    maintainer_email='marius.v.niekerk+spylon@gmail.com',
    license="BSD 3-clause",
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
    ],
)
