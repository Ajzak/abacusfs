#!/bin/sh
#
# normalizes white colour in indexed TIFFs to (255,255,255), producing
# RGB 8-byte TIFFs
#
# usage: normalize-white.sh <src-dir> <dst-dir>

SRC=$1
DST=$2

if [ -z "$SRC" ] || [ -z "$DST" ]; then
	echo 'Usage: normalize-white.sh <src-dir> <dst-dir>'
	exit 42
fi

for F in $SRC/*.tif; do
	pct2rgb.py $F tmp.tif
	nearblack -white -o tmp.hfa tmp.tif
	gdal_translate tmp.hfa $DST/`basename $F`
	rm tmp.tif tmp.hfa
done
