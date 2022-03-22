#!/bin/bash

. ./conf.txt
LAMBDA_FUNC_NAME="docs-gyotaku-creator"

# DocsGyotakuCreator
cd src/DocsGyotakuCreator
pip install -r requirement.txt -t .
rm -r *-info
zip -q -r ../DocsGyotakuCreator ./*
rm -r bin
rm -r certifi
rm -r charset_normalizer
rm -r idna
rm -r requests
rm -r urllib3
rm -r __pycache__
cd ../

aws lambda update-function-code --function-name ${LAMBDA_FUNC_NAME} --zip-file fileb://DocsGyotakuCreator.zip --region ap-northeast-1 --output text --profile ${profile}

rm DocsGyotakuCreator.zip
cd ..


