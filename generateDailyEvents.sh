#!/bin/bash

# theoretically, we should let each input file contain 
# a different set of user id for daily rating
# however, we put that logic to our code (read user ids from input
# but do not use it. instead, we mock a user id within that partition's range
# given we use 10 partitions/input files for 'production-testing' the code 
# each partition generates a random id within partitionId * (60M/10) <-> (partitionId+1) * (60M/10)
# this is for development schedule consideration. 
# this file generates user id within 0-6000, which is not used

# what this script will do is:
# generate a stub file with random scores
# create an intermediate file from the stub file
# create the target file with the intermediate file
# cp the target file to desired results

# we create the intermediate file by appending the stub file to it multi times 
# we create the target file by appending the stub file to it multi times
# until it reaches the file size

# arguments
script=$1

#
# target definition
#
directory=/sparkproject/
numOfTargetFile=3	#real number is this + 1
if [script = "daily"]
then
	filename=daily_user_track_event_;
	finalfilelimit=2000000000;  #2GB
elif [script = "acc"]
then
	filename=accumulatedRatings-;
	finalfilelimit=2200000000;  #2.2GB
else
then
	echo "daily or acc must be specified"
fi

#
# generate stub file
#
stubfilename=stub.txt
rm $stubfilename;
touch $stubfilename;
stubfilelimit=100000;  #100KB. we need a large pool. 
#otherwise every user is duplicated too many times, so every user too many events
#which is not realistic and cause memory problem in our app
stubfilesize=$(wc -c <"$stubfilename");

while [ $stubfilesize -le $stubfilelimit ];
do
	userId=$RANDOM;
	((userId=$userId%6000));
	trackId=$RANDOM;
	((trackId=$trackId%240));
	timestamp=1445047130;
	percentage=0.6;
	eventSource="browsing";
	nextEvent="null";
	
	if [script = "daily"]
	then
		echo $userId,$trackId,$timestamp,$percentage,$eventSource,$nextEvent >> $stubfilename;
	elif [script = "acc"]
	then
		rating=$RANDOM
		echo $userId,$trackId,$timestamp,$rating>> $stubfilename;
	fi
	stubfilesize=$(wc -c <"$stubfilename");
done;
echo 'stub file generated'

#
# generate intermediate file
#
intermediatefilename=intermediate.txt
rm $intermediatefilename
touch $intermediatefilename
for i in {1..2000} #make intermediate file 200MB
do
	cat $stubfilename >> $intermediatefilename
done;
echo 'intermediate file generated'

#
# generate target file
#
rm $filename
touch $filename
filesize=$(wc -c <"$filename");
while [ $filesize -le $finalfilelimit ];
do
	cat $intermediatefilename >> $filename
	
	filesize=$(wc -c <"$filename");
done;
echo 'target file generated'

#
# copy all target files
#
convenience=00
suffix=".txt"
if [script = "daily"]
then
	for ((i=1; i<=numOfTargetFile; i++));
	do
		cp $filename $directory$filename$convenience$i$suffix
	done;
	zero=0
	mv $filename $directory$filename$convenience$zero$suffix
elif [script = "acc"]
then
	acc_convenience="-00"
	for ((i=1; i<=numOfTargetFile; i++));
	do
		for ((j=1; j<=numOfTargetFile; j++));
		do
			cp $filename $directory$filename$convenience$i$acc_convenience$j$suffix
		done;
	done;
	zero=0
	mv $filename $directory$filename$convenience$zero$suffix
fi
echo 'all target files copied'
