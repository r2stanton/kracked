from setuptools import setup, find_packages
import os

VERSION = '1.0.0'
DESCRIPTION = 'Efficiency-first framework for pulling, storing, and aggregating Kraken data using the v2 API.'
LONG_DESCRIPTION = 'Efficiency-first framework for pulling, storing, and aggregating Kraken data using the v2 API.'

# Setting up
setup(
    name="kracked",
    version=VERSION,

    description=DESCRIPTION,
    long_description_content_type="text/markdown",
    long_description=LONG_DESCRIPTION,
    packages=find_packages(),

    install_requires=['numpy', 'pandas', 'ccxt',
                      'websocket-client', 'toml', 'pyarrow'],

    keywords=['cryptocurrency', 'crypto', 'algorithmic trading', 'quantitative finance',
              'exchange', 'Kraken'],
    classifiers=[
        "Development Status :: 1 - Planning",
        "Programming Language :: Python :: 3",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: Unix",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ]
)
