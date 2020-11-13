from setuptools import setup

install_requires = [
    "google-cloud-bigquery"
]

setup(
    name='crowd',
    version='0.1.0',
    packages=['crowd'],
    python_requires=">=3.8",
    install_requires=install_requires,
    entry_points={
        'console_scripts': [
            'crowd = crowd.__main__:main'
        ]
    })
