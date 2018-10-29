# ============================================================
#
# Copyright (C) 2010-2018 by Johannes Wienke
#
# This file may be licensed under the terms of the
# GNU Lesser General Public License Version 3 (the ``LGPL''),
# or (at your option) any later version.
#
# Software distributed under the License is distributed
# on an ``AS IS'' basis, WITHOUT WARRANTY OF ANY KIND, either
# express or implied. See the LGPL for the specific language
# governing rights and limitations.
#
# You should have received a copy of the LGPL along with this
# program. If not, go to http://www.gnu.org/licenses/lgpl.html
# or write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
#
# ============================================================

from setuptools import setup

setup(name='rsb-spread-python',
      version='1.0.0-dev',
      description='Spread transport for rsb-python',
      author='Johannes Wienke',
      author_email='jwienke@techfak.uni-bielefeld.de',
      license='LGPLv3+',
      url='https://code.cor-lab.org/projects/rsb',
      keywords=['middleware', 'bus', 'robotics'],
      classifiers=[
          'Programming Language :: Python',
          'Development Status :: 5 - Production/Stable',
          'Environment :: Other Environment',
          'Intended Audience :: Developers',
          'Intended Audience :: Science/Research',
          'License :: OSI Approved :: '
          'GNU Library or Lesser General Public License (LGPL)',
          'Operating System :: OS Independent',
          'Topic :: Communications',
          'Topic :: Scientific/Engineering',
          'Topic :: Software Development :: Libraries',
          'Topic :: Software Development :: Libraries :: Python Modules',
      ],

      install_requires=['rsb-python>=1.0.dev', 'SpreadModule>=1.6'],
      extras_require={
          'dev': ['pytest', 'pytest-timeout', 'pytest-cov',
                  'tox']
      },

      dependency_links=[
          'http://github.com/open-rsx/rsb-python/tarball/master'
          '#egg=rsb-python-1.0.dev',
          'https://github.com/open-rsx/spread-python3/archive/v1.6.tar.gz'
          '#egg=SpreadModule-1.6',
      ],

      packages=['rsb.transport.spread'])
