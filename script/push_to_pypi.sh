#!/usr/bin/env bash

set -e
DIR=`dirname $0`
cd $DIR/..
rm -r dist pyhouse.egg-info build || true

python3 setup.py sdist bdist_wheel
python3 -m twine upload dist/*

rm -r dist pyhouse.egg-info build
