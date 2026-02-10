from setuptools import find_packages, setup

setup(
    name="li8fn3bk0jnl6h7inw9x",
    version="0.0.1",
    packages=find_packages(),
    install_requires=[
        "selenium==4.2.0",
        "exorde_data",
        "python-dotenv"
    ],
    extras_require={"dev": ["pytest", "pytest-cov", "pytest-asyncio"]},
)
