from setuptools import setup, find_packages


setup(
    name='emit',
    version='0.4.0',
    packages=find_packages(),
    package_data={'': ['README.md', '*Makefile*', '*static/*']},
    include_package_data=True,
    install_requires=[
        'pika>=0.10.0',
        'python-dateutil>=2.5.3'],
    url='https://github.com/godaddy/py-emit')
