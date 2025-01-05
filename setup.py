from setuptools import setup, find_packages

setup(
    name="aep_tools",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "requests",
        "click",
    ],
    entry_points={
        "console_scripts": [
            "aep-cli=aep_tools.cli:cli",
        ],
    },
)
