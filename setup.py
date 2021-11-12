from setuptools import find_packages, setup

setup(
    name='conectionazure',
    packages=find_packages(),
    version='0.1.0',
    description='First',
    author="Artur Jacques Nürnberg",
    install_requires=["pandas", "requests", "azure-storage-file-datalake", "azure-identity"],
    setup_requires=['pytest-runner'],
    tests_require=['pytest==4.4.1'],
    test_suite='tests',
)