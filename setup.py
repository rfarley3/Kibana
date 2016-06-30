from setuptools import setup

setup(
    name='kibana',
    packages=['kibana'],
    version='0.5',
    description='Kibana configuration index (.kibana) command line interface and python API (visualization import/export and mappings refresh)',
    author='Ryan Farley',
    author_email='rfarley@mitre.org',
    url='https://github.com/rfarley3/Kibana',
    download_url='https://github.com/rfarley3/Kibana/tarball/0.4',
    keywords=['kibana', 'config', 'import', 'export', 'mappings'],
    classifiers=[],
    install_requires=(
        'elasticsearch',
        'argparse',
        'requests',
    ),
    entry_points={
        'console_scripts': [
            'dotkibana = kibana.__main__:main',
        ]
    },
)
