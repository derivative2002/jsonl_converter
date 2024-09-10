from setuptools import setup, find_packages

setup(
    name='jsonl_converter',
    version='0.1',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        'pathlib',
    ],
    entry_points={
        'console_scripts': [
            'jsonl_converter=converter:main',
        ],
    },
)
