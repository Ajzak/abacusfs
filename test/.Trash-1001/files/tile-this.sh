#!/bin/bash

COLOUR_GREEN="\033[1;32m";
COLOUR_RESET="\033[0m";

#Usage: green_echo "bubugaga"
green_echo() {
	echo -e "$COLOUR_GREEN$1$COLOUR_RESET";
}

#Usage: launch_job reservation-id jsdl-file
#uses global function JOB_IDS
launch_job() {
	JOB_ID=`xsub -r $1 -f $2 | grep "Job submitted" | awk '{print $4;}'`
	echo "Job $JOB_ID submitted"
	rm -f $2
	if [ -z "$JOB_IDS" ]; then
		JOB_IDS=$JOB_ID
	else
		JOB_IDS="$JOB_IDS $JOB_ID"
	fi
}

#Usage: wait_for_jobs space-separated-id-list
wait_for_jobs() {
	for JOB in $1; do
		xwait -j $JOB | grep -v "Property not valid: certificateLocation"
	done
}

#XTREEMFS_HOME=/home/8b432b5e-f812-42ca-b90a-d3dc1538dff5

NUM_RES=2
NUM_PROCS=$NUM_RES

RUN_LOCALLY=1

TILE_DELTA_17=0.000274658203125
TILE_PX=512
FINEST_LEVEL=14
COARSEST_LEVEL=11
TILE_DELTA=`echo $TILE_DELTA_17*2^$[17-$FINEST_LEVEL]|bc -l`


#echo
#green_echo "Cleaning destination directories"
#echo
#pushd $XTREEMFS_HOME
#rm -rf `seq 1 20`
#popd
#cp -f *.sh $XTREEMFS_HOME
#cp -f *.pl $XTREEMFS_HOME

if [ $RUN_LOCALLY -eq 0 ]; then
	echo
	green_echo "Making reservation"
	echo
	xreservation -Z
	sed "s/NUM_RES/$NUM_RES/" reservation.jsdl >reservation1.jsdl
	RESERVATION_ID=`xreservation -f reservation1.jsdl -n $NUM_RES -t 20`
	echo "Reservation ID: $RESERVATION_ID"
	rm -f reservation1.jsdl
fi

echo
green_echo "Launching reproject/tile jobs"
echo
JOB_IDS=""
for ((RANK=0; $RANK < $NUM_PROCS ; RANK++)); do
	if [ $RUN_LOCALLY -eq 1 ]; then
		./tile-images.pl . . $TILE_DELTA $TILE_PX $FINEST_LEVEL -90 -180 '+proj=tmerc +lat_0=0 +lon_0=15 +k=0.999900 +x_0=500000 +y_0=-5000000 +ellps=bessel +towgs84=668,-205,472 +units=m +no_defs' '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs' $RANK $NUM_PROCS	
	else
		sed "s/TILE_DELTA/$TILE_DELTA/g" reproject.jsdl \
			| sed "s/TILE_PX/$TILE_PX/g" \
			| sed "s/FINEST_LEVEL/$FINEST_LEVEL/g" \
			| sed "s/RANK/$RANK/g" \
			| sed "s/NUM_PROCS/$NUM_PROCS/g" >reproject$RANK.jsdl
		launch_job $RESERVATION_ID reproject$RANK.jsdl
	fi
done

if [ $RUN_LOCALLY -eq 0 ]; then
	echo
	green_echo "Waiting for $NUM_PROCS reproject/tile jobs to finish..."
	wait_for_jobs "$JOB_IDS"
	rm -f $XTREEMFS_HOME/reproject*.err $XTREEMFS_HOME/reproject*.out
fi
green_echo "...reprojecting done."

CURR_LEVEL=$FINEST_LEVEL
while [ $CURR_LEVEL -gt $COARSEST_LEVEL ]; do
	echo
	green_echo "Launching combine jobs for level $CURR_LEVEL"
	echo
	JOB_IDS=""
	if [ $CURR_LEVEL -eq $FINEST_LEVEL ]; then
		CURR_NUM_PROCS=$NUM_PROCS;
	else
		CURR_NUM_PROCS=1;
	fi
	for ((RANK=0; $RANK < $CURR_NUM_PROCS ; RANK++)); do
		if [ $RUN_LOCALLY -eq 1 ]; then
			./combine-txt.pl $CURR_LEVEL . tif $TILE_PX $TILE_DELTA -90 -180 $RANK $CURR_NUM_PROCS
		else
			sed "s/TILE_DELTA/$TILE_DELTA/g" combine.jsdl \
				| sed "s/TILE_PX/$TILE_PX/g" \
				| sed "s/CURR_LEVEL/$CURR_LEVEL/g" \
				| sed "s/RANK/$RANK/g" \
				| sed "s/NUM_PROCS/$CURR_NUM_PROCS/g" >combine$RANK.jsdl
			launch_job $RESERVATION_ID combine$RANK.jsdl
		fi
	done
	echo
	if [ $RUN_LOCALLY -eq 0 ]; then
		green_echo "Waiting for $CURR_NUM_PROCS combine jobs to finish..."
		wait_for_jobs "$JOB_IDS"
	fi
	green_echo "...combining level $CURR_LEVEL done."
	echo
	CURR_LEVEL=$[$CURR_LEVEL-1]
	TILE_DELTA=`echo $TILE_DELTA*2|bc -l`
done
#rm -f $XTREEMFS_HOME/combine*.err $XTREEMFS_HOME/combine*.out

echo
green_echo "Launching convert jobs"
echo
JOB_IDS=""

if [ $RUN_LOCALLY -eq 1 ]; then
	./fmt-convert.sh png jpg 10 $FINEST_LEVEL $FINEST_LEVEL
	./fmt-convert.sh png jpg 10 $COARSEST_LEVEL $[$FINEST_LEVEL-1]
else
	sed "s/FROM_LEVEL/$FINEST_LEVEL/g" convert.jsdl \
		| sed "s/TO_LEVEL/$FINEST_LEVEL/g" \
		| sed "s/RANK/0/g" >convert0.jsdl
	launch_job $RESERVATION_ID convert0.jsdl

	sed "s/FROM_LEVEL/$COARSEST_LEVEL/g" convert.jsdl \
		| sed "s/TO_LEVEL/$[$FINEST_LEVEL-1]/g" \
		| sed "s/RANK/1/g" >convert1.jsdl
	launch_job $RESERVATION_ID convert1.jsdl
	echo
	green_echo "Waiting for 2 convert jobs to finish"
	wait_for_jobs "$JOB_IDS"
fi

echo

#rm -f $XTREEMFS_HOME/convert*.err $XTREEMFS_HOME/convert*.out

#rm -f $XTREEMFS_HOME/*.sh $XTREEMFS_HOME/*.pl#\

if [ $RUN_LOCALLY -eq 0 ]; then
	xreservation -Z
fi

#green_echo "Updating public web page"
#rm -f /root/demos/gaea/html/index.html
#ln -s /root/demos/gaea/html/novo-mesto-updated.html /root/demos/gaea/html/index.html

#echo
#green_echo "Sending notification"
#../mon-notify/send-notification.sh core.xlab.si 55000 "gis-data-ready;Cegelnica-Brod Road Plan; ; ; ; "

#echo
#green_echo "All done!"
#echo


