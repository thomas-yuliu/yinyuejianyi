#!/bin/bash
#what this script will do is:
# generate a stub file
# keep on appending the stub file to the target file
# until reach the file size
# for the target file, keep on appending the stub file
# until it reaches the file size
# cp the target file to desired results

filename=daily_user_track_event_;
directory=/sparkproject/
finalfilelimit=2000000000;  #2GB

interfilename=inter.txt
rm $interfilename;
touch $interfilename;
interfilelimit=1000000;  #1KB
interfilesize=$(wc -c <"$interfilename");

while [ $interfilesize -le $interfilelimit ];
do
	userId=$RANDOM;
	((userId=$userId%720));
	trackId=$RANDOM;
	((trackId=$trackId%240));
	timestamp=1445047130;
	percentage=0.6;
	eventSource="browsing";
	nextEvent="null";
	
	echo $userId,$trackId,$timestamp,$percentage,$eventSource,$nextEvent >> $interfilename;
	
	interfilesize=$(wc -c <"$interfilename");
done;

rm $filename
touch $filename
filesize=$(wc -c <"$filename");
while [ $filesize -le $finalfilelimit ];
do
	cat $interfilename >> $filename
	
	filesize=$(wc -c <"$filename");
done;

convenience=00
suffix=".txt"
for i in {1..9}
do
	cp $filename $directory$filename$convenience$i$suffix
done;
zero=0
mv $filename $directory$filename$convenience$zero$suffix
