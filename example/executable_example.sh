#!/usr/bin/env bash

set -eo pipefail

RUNFILE=$1
OUTFILE=$2

# example of executable:
# - print date, node name to OUTFILE;
# - print runcard contents to OUTFILE (as example of input-dependent processing).

date > $OUTFILE
echo $HOSTNAME >> $OUTFILE
cat $RUNFILE >> $OUTFILE
