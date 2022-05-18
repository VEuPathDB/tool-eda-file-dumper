#!/bin/sh
###############################################################
##
## This script checks for the environment necessary to build
## the project and then executes a clean compilation.
##
###############################################################

error=false

if [ "$GITHUB_USERNAME" = "" ]; then
    echo "Required environment variable \$GITHUB_USERNAME is not defined."
    error=true
fi
if [ "$GITHUB_TOKEN" = "" ]; then
    echo "Required environment variable \$GITHUB_TOKEN is not defined."
    error=true
fi

if [ "$error" = "true" ]; then
   exit 1
fi

mvn clean install --settings settings.xml
