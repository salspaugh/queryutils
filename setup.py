import os
import setuptools

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setuptools.setup(
    name = "queryutils",
    version = "0.0.1",
    author = "Sara Alspaugh",
    author_email = "saraalspaugh@gmail.com",
    description = ("Utilities for handling queries for my analysis."),
    license = "BSD",
    #keywords = "",
    #url = "",
    packages=['queryutils'],
    long_description=read('README.md'),
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Programming Language :: Other",
        "Topic :: Software Development :: Compilers",
        "License :: OSI Approved :: BSD License",
    ],
    #requires=["ply"]
)
