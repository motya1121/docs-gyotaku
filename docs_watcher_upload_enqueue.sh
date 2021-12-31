#!/bin/bash

LAMBDA_FUNC_NAME="docs-watcher-update-info"
profile="main"

# DocsGyotakuCreator
cd src/DocsUpdateInfoEnqueue
pip install -r requirement.txt -t .
rm -r *-info
zip -q -r ../DocsUpdateInfoEnqueue ./*
rm -r bin
rm -r certifi
rm -r charset_normalizer
rm -r idna
rm -r requests
rm -r urllib3
rm -r __pycache__
cd ../

aws lambda update-function-code --function-name ${LAMBDA_FUNC_NAME} --zip-file fileb://DocsUpdateInfoEnqueue.zip --region ap-northeast-1 --output text --profile ${profile}

rm DocsUpdateInfoEnqueue.zip
cd ..


