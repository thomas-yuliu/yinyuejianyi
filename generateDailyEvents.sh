#!/bin/bash
#what this script will do is:
# generate a stub file
# duplicate its content and save to itself
# keep doubling size until reach the limit

filename=daily_user_track_event.txt;
finalfilelimit=2000000000;

rm $filename;
touch $filename;

stubfilelimit=100000;#100KB
filesize=$(wc -c <"$filename");
while [ $filesize -le $stubfilelimit ];
do
	userId=$RANDOM;
	((userId=$userId%720));
	trackId=$RANDOM;
	((trackId=$trackId%240));
	timestamp=1445047130;
	percentage=0.6;
	eventSource="browsing";
	nextEvent="null";
	
	echo $userId , $trackId , $timestamp , $percentage, $eventSource , $nextEvent >> $filename;
	
	filesize=$(wc -c <"$filename");
done;

interfilename=inter.txt
while [ $filesize -le $finalfilelimit ];
do
	#override interfilename
	echo "" > $interfilename
	
	#each time double size
	cat $filename >> $interfilename
	cat $interfilename >> $filename
	
	filesize=$(wc -c <"$filename");
done;