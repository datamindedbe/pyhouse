#!/usr/bin/env bash

cd "$(dirname "$0")/.."

set -ex
mypy --config-file mypy.ini pyhouse
flake8 --max-line-length=120
python -m unittest discover tests