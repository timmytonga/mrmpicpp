#!/bin/bash

USAGE="Usage: ./$0 <splitFile>"
TMPDIR=$(mktemp -d) # create a temp dir 
SPLITSIZE="256M"

echo "Created $TMPDIR... Splitting files there and distributing work."

split --line-bytes=${SPLITSIZE} --numeric-suffixes $1 ${TMPDIR}/splitFile

echo "Running Mapreduce..."

time mpirun --prefix /pkg/openmpi/2.0.2 -np 8 --hostfile hosts ./wc $TMPDIR $2

echo "Finished."
echo "Cleaning Up..."
rm -rf ${TMPDIR}
