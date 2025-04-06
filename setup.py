from setuptools import setup, find_packages

setup(
    name="flare",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pyarrow",
        "confluent-kafka",
        "typer",
        "rich",
        "prometheus-client",
        "duckdb",
        "pyiceberg",
    ],
    entry_points={
        "console_scripts": [
            "flare=flare.cli.main:app",
        ],
    },
    python_requires=">=3.10",
    author="Flare Team",
    description="High-performance Apache Arrow Flight gateway for Kafka",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
