#!/usr/bin/env bash

INSTALL_COMMAND="pip3 install"
dependencies="warcio requests requests_file boto3 botocore py4j spark"

for dep in $dependencies; do
    $INSTALL_COMMAND $dep
done;

