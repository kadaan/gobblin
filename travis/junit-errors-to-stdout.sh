#!/bin/bash
IFS='
'
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
ROOTDIR="$1"
if [ -z "$ROOTDIR" ]; then
	ROOTDIR="."
fi
echo 'Formatting results...'
FILES=$(find "$ROOTDIR" -path '*/build/*/test-results/*.xml' | python <( echo '
#!/usr/bin/python

import sys
import fileinput
import xml.etree.ElementTree

for line in fileinput.input():
    suite = xml.etree.ElementTree.parse(line.rstrip()).getroot()
    errors = suite.get("errors")
    failures = suite.get("failures")
    if (errors is not None and int(errors) > 0) or (failures is not None and int(failures) > 0):
        sys.stdout.write(line)

sys.exit(0)
' ))
if [ -n "$FILES" ]; then
	for file in $FILES; do
		echo "Formatting $file"
		if [ -f "$file" ]; then
			echo '====================================================='
			xsltproc "$DIR/junit-xml-format-errors.xsl" "$file"
		fi
	done
	echo '====================================================='
else
	echo 'No */build/*/test-results/*.xml files found with failing tests.'
fi
