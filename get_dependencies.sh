#!/usr/bin/env bash

INSTALL_COMMAND="sudo pip-3.4 install"
dependencies="warcio requests requests_file boto3 botocore py4j"

for dep in $dependencies; do
    $INSTALL_COMMAND $dep
done;

