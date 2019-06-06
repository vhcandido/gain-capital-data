#!/bin/bash

y=2012
for m in {01..12};
do
	find . -path "./data/zip/$y/$m*zip" -exec sh -c 'dir="${1#*zip/}"; unzip -d "data/tick/${dir%/*}" "$1"' _ {} \;
done
