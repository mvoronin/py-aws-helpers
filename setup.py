from setuptools import setup

setup(name='pyawshelpers',
      version='0.1',
      description='Python helper classes and functions for AWS API',
      url='http://github.com/mvoronin/py-aws-helpers',
      author='Mikhail Voronin',
      author_email='contact@mvoronin.pro',
      license='Apache License, Version 2.0',
      classifiers=[
          'Development Status :: 4 - Beta',
          'License :: OSI Approved :: MIT License',

          'Programming Language :: Python :: 2',
          'Programming Language :: Python :: 2.7'
      ],
      packages=['awshelpers'],
      zip_safe=False,
      install_requires=['boto'],)
