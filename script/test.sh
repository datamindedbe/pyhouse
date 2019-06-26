#!/usr/bin/env bash

set -ex

mypy -p pyhouse

flake8 --max-line-length=120

