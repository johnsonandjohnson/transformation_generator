#!/bin/bash

LOCAL_ANTLR4=$( ls /usr/local/lib | grep -m 1 antlr-4..*-complete.jar)

if [[ ! -f /usr/local/lib/${LOCAL_ANTLR4} ]]
then
    echo -e "Could not find antlr-4.*-complete.jar in /usr/local/lib"
    exit 1
fi

java -Xmx500M -cp "${LOCAL_ANTLR4}:$CLASSPATH" org.antlr.v4.Tool \
    -Dlanguage=Python3 \
    -visitor \
    -o transform_generator/parser/generated \
    antlr/TransformExp.g4
