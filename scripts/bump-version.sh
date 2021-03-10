#!/bin/bash
set -eu

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $SCRIPT_DIR/..

OLD_VERSION="$1"
NEW_VERSION="$2"

sed -i -e "s/^__version__ = "'".*"'"\$/__version__ = "'"'"$NEW_VERSION"'"'"/" rb/__init__.py

echo "New version: $NEW_VERSION"
