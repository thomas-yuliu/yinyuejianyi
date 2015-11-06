#!/bin/bash

filename=daily_user_track_event.txt;
filesize=$(wc -c <"$filename");
filelimit=2000000000;

rm $filename;
touch $filename;

while [ $filesize -le $filelimit ];
do
	userId=$RANDOM;
	$(($userId%=720));
	trackId=$RANDOM;
	$(($trackId%=240));
	timestamp=1445047130;
	percentage=0.6;
	eventSource="browsing";
	nextEvent="null";
	
	echo $userId , $trackId , $timestamp , $percentage, $eventSource , $nextEvent >> $filename;
	
	filesize=$(wc -c <"$filename");
done;
