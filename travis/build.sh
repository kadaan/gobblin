#!/bin/bash
set -e

if [ "$USEHADOOP2" = true ] ; then
  ./gradlew clean assemble -PuseHadoop2
else
  ./gradlew clean assemble
fi