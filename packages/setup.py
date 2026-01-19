"""
Setup script for databricks_tag_logger package
"""
from setuptools import setup, find_packages

setup(
    name="databricks-tag-logger",
    version="1.0.0",
    description="Dynamic tag correlation for Databricks serverless workloads",
    author="Databricks",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "pyspark>=3.4.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "black>=23.0.0",
            "mypy>=1.0.0",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
