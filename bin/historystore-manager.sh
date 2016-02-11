#!/bin/bash

FWDIR="$(cd `dirname $0`/..; pwd)"

GOBBLIN_JARS=""
for jar in $(ls -d $FWDIR/lib/*); do
  if [ "$GOBBLIN_JARS" != "" ]; then
    GOBBLIN_JARS+=":$jar"
  else
    GOBBLIN_JARS=$jar
  fi
done

CLASSPATH=$GOBBLIN_JARS
CLASSPATH+=":$FWDIR/conf"

action=$1
options=( "$@" );
args=( "$@" );
unset options[$1];
unset args[$1];

index=1
for var in "${options[@]}"
do
  if [[ $var == -D=* ]]; then
    unset options[$index];
  fi
  index=$((index+1))
done
index=1
for var in "${args[@]}"
do
  if [[ $var != -D=* ]]; then
    unset args[$index];
  fi
  index=$((index+1))
done

java "${options[@]}" -cp $CLASSPATH gobblin.metastore.util.DatabaseJobHistoryStoreSchemaManager $action "${args[@]}"
