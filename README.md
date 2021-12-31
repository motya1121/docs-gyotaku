# docs-gyotaku


## deploy

create
```
aws cloudformation create-stack \
--stack-name DocsGyotaku \
--template-body file://CloudFormation/docs-gyotaku.yml \
--capabilities CAPABILITY_NAMED_IAM \
--parameters \
ParameterKey=S3BucketName,ParameterValue=docs-gyotaku-532648218247 \
ParameterKey=DDBGTableName,ParameterValue=docs-gyotaku\
--profile main
```

update
```
aws cloudformation update-stack \
--stack-name DocsGyotaku \
--template-body file://CloudFormation/docs-gyotaku.yml \
--capabilities CAPABILITY_NAMED_IAM \
--parameters \
ParameterKey=S3BucketName,ParameterValue=docs-gyotaku-532648218247 \
ParameterKey=DDBGTableName,ParameterValue=docs-gyotaku \
ParameterKey=DocsUpdateNotifyQueueName,ParameterValue=docs-gyotaku-notify-queue \
ParameterKey=CloudWatchAlermSNSArn,ParameterValue=arn:aws:sns:ap-northeast-1:532648218247:SendToSlackViaChatbot \
--profile main
```

delete
```
aws cloudformation delete-stack --stack-name DocsGyotaku --profile main
```
