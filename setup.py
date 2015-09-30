#!/usr/bin/env python3

import os
import uuid
from setuptools import setup, find_packages
from pip.req import parse_requirements

def read(*paths):
    """Build a file path from *paths* and return the contents."""
    with open(os.path.join(*paths), 'r') as f:
        return f.read()

def read_version():
    rec = read('hadoopinspector/_version.py')
    fields = rec.split('=')
    version = fields[1].strip()[1:-1]
    assert version.count('.') == 2
    return version


version          = read_version()
DESCRIPTION      = 'A measurement and inspection tool to manage Hadoop data quality, manageability and health'
REQUIREMENTS     = [str(ir.req) for ir in parse_requirements('requirements.txt', session=uuid.uuid1())]

setup(name             = 'hadoop-inspector',
      version          = version           ,
      description      = DESCRIPTION       ,
      long_description=(read('README.rst') + '\n\n' +
                        read('CHANGELOG.rst')),
      keywords         = "data quality management health",
      author           = 'Will Farmer, Ken Farmer'      ,
      author_email     = 'willzfarmer@gmail.com, kenfar@gmail.com',
      url              = 'http://github.com/kenfar/hadoop-inspector',
      license          = 'BSD'             ,
      classifiers=[
            'Development Status :: 4 - Beta'                         ,
            'Environment :: Console'                                 ,
            'Intended Audience :: Information Technology'            ,
            'Intended Audience :: Science/Research'                  ,
            'License :: OSI Approved :: BSD License'                 ,
            'Programming Language :: Python :: 2'                    ,
            'Operating System :: POSIX'                              ,
            'Topic :: Database'                                      ,
            'Topic :: Scientific/Engineering :: Information Analysis',
            ],
      scripts      = ['scripts/hadoopinspector_demogen.py',
                      'scripts/hadoopinspector_runner.py' ],
      install_requires = REQUIREMENTS,
      packages     = find_packages(),
     )
