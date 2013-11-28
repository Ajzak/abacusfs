#!/bin/sh

for F in *.tif; do
	if gdalinfo $F | grep "Band 4" >/dev/null; then
		echo $F has alpha channel.
	else
		echo $F has no alpha channel. adding.
		T=`basename $F .tif`.tfw
		if ! [ -e $T ]; then
			echo Creating TFW
			listgeo -tfw $F
		fi
		if convert $F -alpha activate tmp.tif; then
			mv tmp.tif $F
		else
			echo Failed to alphaize $F
			exit 42
		fi
	fi
done
		
