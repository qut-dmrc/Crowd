from setuptools import setup
setup(
    name = 'crowd',
    version = '0.1.0',
    packages = ['crowd'],
    python_requires = ">=3.8",
    entry_points = {
        'console_scripts': [
            'crowd = crowd.__main__:main'
        ]
    })