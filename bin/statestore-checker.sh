#!/bin/bash

FWDIR="$(cd `dirname $0`/..; pwd)"
FWDIR_CONF=${FWDIR}/conf

GOBBLIN_JARS=""
for jar in $(ls -d $FWDIR/lib/*); do
  if [ "$GOBBLIN_JARS" != "" ]; then
    GOBBLIN_JARS+=":$jar"
  else
    GOBBLIN_JARS=$jar
  fi
done

CLASSPATH=$GOBBLIN_JARS
CLASSPATH+=":$FWDIR_CONF"

java -Dlogback.configurationFile=file://$FWDIR_CONF/logback-gobblin.xml \
    -cp $CLASSPATH \
    gobblin.runtime.util.JobStateToJsonConverter \
    $@
