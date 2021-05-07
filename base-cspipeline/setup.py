from setuptools import find_packages, setup

VERSION = "0.1.4"
DESCRIPTION = "Basic ETL Pipeline construction for Airflow"
LONG_DESCRIPTION = "TODO"

AUTHORS = (
    "Vincent Livant, Luke Vu, Jack Davis, Kevin Sokoli, Farhan Warsame, Rajath Shetty, Oscar Vergara, Nick Suchecki",
)

setup(
    name="cspipeline",
    version=VERSION,
    author=AUTHORS,
    author_email="",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    packages=find_packages(),
    keywords=["airflow", "ETL", "pipeline", "courts", "court data"],
    classifiers=["Development Status :: 3 - Alpha", "Intended Audience :: Education"],
)
