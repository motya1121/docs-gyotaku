#!/bin/bash

cd src/DocsGyotakuCreator
python -c "import lambda_function; lambda_function.lambda_handler('hoge', 'puge')"
