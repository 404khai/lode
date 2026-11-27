#!/usr/bin/env bash
# Runs the ceph/s3-tests cases listed in tests.txt against an Adit on
# localhost:9000 started with test/s3compat/adit.yaml.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
: "${S3TESTS_DIR:=$here/.s3-tests}"
S3TESTS_COMMIT=5b3987116eca0b4b8fa57d6b10f3db57bab196b6

if [ ! -d "$S3TESTS_DIR/.git" ]; then
  git clone -q https://github.com/ceph/s3-tests.git "$S3TESTS_DIR"
fi
git -C "$S3TESTS_DIR" fetch -q --depth 1 origin "$S3TESTS_COMMIT"
git -C "$S3TESTS_DIR" checkout -q "$S3TESTS_COMMIT"
if [ ! -x "$S3TESTS_DIR/.venv/bin/python" ]; then
  python3 -m venv "$S3TESTS_DIR/.venv"
  "$S3TESTS_DIR/.venv/bin/pip" -q install -r "$S3TESTS_DIR/requirements.txt"
fi

tests=()
while IFS= read -r t; do tests+=("s3tests/functional/test_s3.py::$t"); done < <(grep -vE '^[[:space:]]*(#|$)' "$here/tests.txt")
cd "$S3TESTS_DIR"
S3TEST_CONF="$here/s3tests.conf" .venv/bin/python -m pytest -q -p no:cacheprovider --tb=short "${tests[@]}"
