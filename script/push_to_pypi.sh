#!/usr/bin/env bash
set -e

cd "$(dirname "$0")/.."

rm -r dist pyhouse.egg-info build || true

python3 setup.py sdist bdist_wheel
python3 -m twine upload dist/*

rm -r dist pyhouse.egg-info build
