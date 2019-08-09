#!/usr/bin/env bash
set -e

VERSION=$(curl -v --silent https://www.dataiku.com/dss/trynow/linux/ 2>&1 | grep -e 'var cur_version' | sed -n 's/^.*"\(.*\)".*$/\1/p')

mkdir -p lib

cd lib

wget "https://cdn.downloads.dataiku.com/public/dss/$VERSION/dataiku-dss-$VERSION.tar.gz"

tar -zxf "dataiku-dss-$VERSION.tar.gz"

tee "dataiku-dss-$VERSION/python/setup.py" << END
from setuptools import setup

setup(
    name='dataiku',
    version='$VERSION',
    packages=['dataiku', 'dataiku.sql', 'dataiku.base', 'dataiku.core', 'dataiku.spark', 'dataiku.doctor',
              'dataiku.doctor.utils', 'dataiku.doctor.crossval', 'dataiku.doctor.clustering',
              'dataiku.doctor.prediction', 'dataiku.doctor.posttraining', 'dataiku.doctor.deep_learning',
              'dataiku.doctor.preprocessing', 'dataiku.dsscli', 'dataiku.metric', 'dataiku.apinode',
              'dataiku.apinode.admin', 'dataiku.apinode.predict', 'dataiku.cluster', 'dataiku.webapps',
              'dataiku.customui', 'dataiku.exporter', 'dataiku.insights', 'dataiku.notebook', 'dataiku.scenario',
              'dataiku.connector', 'dataiku.container', 'dataiku.runnables', 'dataiku.customstep', 'dataiku.fsprovider',
              'dataiku.customformat', 'dataiku.customrecipe', 'dataiku.customwebapp', 'dataiku.customtrigger',
              'dataikuapi', 'dataikuapi.dss', 'dataikuapi.apinode_admin'],
    url='https://www.dataiku.com',
    license='',
    author='',
    author_email='',
    description=''
)
END

cd "dataiku-dss-$VERSION/python"
python setup.py -q sdist
CUR_DIR=$(pwd)

echo ""
echo ">>>>>>>>>>> USE THIS PIP COMMAND <<<<<<<<<<<<<<<"
echo ""
echo "pip install dataiku --upgrade --no-index --find-links file://$CUR_DIR/dist"
