#!/bin/bash

y=2014
for m in {01..12};
do
	find . -path "./data/zip/$y/$m*zip" -exec sh -c 'dir="${1#*zip/}"; unzip -d "data/tick/${dir%/*}" "$1"' _ {} \;
done
#find . -path "./data/zip/2013/01*zip" -exec unzip {} -d data/tick/2013/01\ January/ \;
