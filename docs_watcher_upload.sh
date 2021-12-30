#!/bin/bash

LAMBDA_FUNC_NAME="docs-gyotaku-watcher"
profile="main"

# DocsGyotakuCreator
cd src/DocsWatcher
pip install -r requirement.txt -t .
rm -r *-info
zip -q -r ../DocsWatcher ./*
rm -r bin
rm -r certifi
rm -r charset_normalizer
rm -r idna
rm -r requests
rm -r bs4
rm -r soupsieve
rm -r urllib3
rm -r __pycache__
cd ../

aws lambda update-function-code --function-name ${LAMBDA_FUNC_NAME} --zip-file fileb://DocsWatcher.zip --region ap-northeast-1 --output text --profile ${profile}

rm DocsWatcher.zip
cd ..


