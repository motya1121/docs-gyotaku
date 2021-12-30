#!/bin/bash

export DDBTablename="docs-gyotaku"

cd src/DocsWatcher
python -c "import lambda_function; lambda_function.lambda_handler('hoge', 'puge')"
