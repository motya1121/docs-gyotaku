#!/bin/bash

. ./conf.txt
LAMBDA_FUNC_NAME="docs-gyotaku-watcher"
DIR_NAME="DocsGyotakuWatcher"

# DocsGyotakuCreator
cd src/${DIR_NAME}
pip install -r requirement.txt -t .
rm -r *-info
zip -q -r ../${DIR_NAME} ./*
rm -r bin
rm -r certifi
rm -r charset_normalizer
rm -r idna
rm -r requests
rm -r bs4
rm -r soupsieve
rm -r urllib3
rm -r feedparser
cd ../

aws lambda update-function-code --function-name ${LAMBDA_FUNC_NAME} --zip-file fileb://${DIR_NAME}.zip --region ap-northeast-1 --output text --profile ${profile}

rm ${DIR_NAME}.zip
cd ..


