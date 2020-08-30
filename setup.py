from setuptools import setup

setup(
    name='tote',
    version='0.1',
    description='A duplication eliminating encrypted archive system.',
    url='https://github.com/sarah-happy/tote/',
    author='Sarah Happy',
    author_email='happy@sarah-happy.ca',
    license='MIT',
    packages=['tote'],
    zip_safe=False,
    scripts=['bin/tote'],
)

