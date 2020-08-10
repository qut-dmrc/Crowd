from setuptools import setup
setup(
    name = 'crowd',
    version = '0.1.0',
    packages = ['crowd'],
    entry_points = {
        'console_scripts': [
            'crowd = crowd.__main__:main'
        ]
    })