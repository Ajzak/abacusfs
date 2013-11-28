#!/bin/bash
#
# fmt-convert from-format to-format quality from-level to-level

SRC_FORMAT=$1
DEST_FORMAT=$2
CONVERT_SCRIPT=`dirname $0`/fmt-convert-1.sh
QUALITY=$3
FROM_LEVEL=$4
TO_LEVEL=$5

echo "Converting levels `seq -s\  $FROM_LEVEL $TO_LEVEL` from $SRC_FORMAT to $DEST_FORMAT, quality $QUALITY"

for ((LEVEL=FROM_LEVEL; $LEVEL <= $TO_LEVEL; LEVEL++)); do
	echo converting level $LEVEL
	find $LEVEL -name "*.$SRC_FORMAT" -exec $CONVERT_SCRIPT $QUALITY \{\} $SRC_FORMAT $DEST_FORMAT \;
done

