from setuptools import setup, find_packages

setup(
    name="databricks-storage-utils",
    version="0.1.0",
    python_requires=">=3.9",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
)