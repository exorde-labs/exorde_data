from setuptools import find_packages, setup

setup(
    name="exorde_translation",
    version="0.0.1",
    packages=find_packages(include=["src"]),
    install_requires=[
        "argostranslate==1.9.6",
        "fasttext==0.9.2",
        "fasttext-langdetect==1.0.5",
        "exorde_data@git+https://github.com/exorde-labs/exorde_data@complete#egg=exorde_data&subdirectory=exorde_data"
    ],
)
