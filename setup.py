from setuptools import setup
from distutils.util import convert_path

version = {}
ver_path = convert_path('version.py')
with open(ver_path) as ver_file:
    exec(ver_file.read(), version)
      
setup(
   version=version['__version__'],
   # And so on...
)