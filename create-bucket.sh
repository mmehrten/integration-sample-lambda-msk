#!/bin/bash
if [ -e /tmp/bucket-name.txt ]; then
    echo "Bucket already created"
    exit 0
fi

BUCKET_ID=$(dd if=/dev/random bs=8 count=1 2>/dev/null | od -An -tx1 | tr -d ' \t\n')
BUCKET_NAME=lambda-artifacts-$BUCKET_ID
echo $BUCKET_NAME > /tmp/bucket-name.txt
aws s3 mb s3://$BUCKET_NAME