#!/usr/bin/env bash

DIR=`dirname $0`

set -ex
mypy --config-file ${DIR}/../mypy.ini ${DIR}/../pyhouse
flake8 --max-line-length=120
python -m unittest discover ${DIR}/../tests/