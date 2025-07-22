# setup.py - Para compilar Cython
from setuptools import setup, Extension
from Cython.Build import cythonize

setup(
    ext_modules=cythonize("payment_core.pyx",
        compiler_directives={'language_level': "3"})
)