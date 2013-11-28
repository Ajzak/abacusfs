#!/bin/sh
#
# png-to-jpg-1.sh <filename>

Q=$1
F=$2
P=$3
P2=$4

D=`dirname $F`/`basename $F .$P`.$P2

convert $F -background '#ff00ff' -flatten -quality $Q $D 2>/dev/null
