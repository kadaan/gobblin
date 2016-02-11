#!/bin/bash
set -e

if [ "$USEHADOOP2" = true ] ; then
  ./gradlew test -PuseHadoop2 -PskipTestGroup=disabledOnTravis
else
  ./gradlew test -PskipTestGroup=disabledOnTravis
fi