#!/bin/bash

# how the script works:
# in a loop forever, per iteration, sleep 5 sec and produce some kafka msgs
# how many msgs to produce? we have a target rate to catch
# so per iteration, we do some calculation:
# check how long the script has run and how many msgs it has generated
# calculate how many msgs to produce to catch up with the target rate

# you have a target rate of events to input into streaming rec 'R' (in MB)
# you know how many kafka producer hosts you have 'P'
# so each producer host should generate 'R/P' events
# this is the param defined here
perHostRate=250 #MB

scriptStartSecond=$SECOND
howLongHasRun=0
howManyMsgsHasGenerated=0
while true; 
do
  #update howLongHasRun
  howLongHasRun=(($SECOND-$scriptStartSecond))
  #for this running length, how many msgs should have been generated
  targetNumOfMsgs=(($perHostRate*$howLongHasRun))
  #num of msgs to catch up
  numOfMsgsToCatchUp=(($targetNumOfMsgs-$howManyMsgsHasGenerated))
  
  #send those msgs
  sendMsg(numOfMsgsToCatchUp)
  
  #update howManyMsgsHasGenerated
  howManyMsgsHasGenerated=(($targetNumOfMsgs))
  sleep 5;
  
  #preventing int too large, start from scratch
  #will cause rate inaccurate but it is ok
  if [ "$howManyMsgsHasGenerated" -gt 5000000 ]
  then
  	scriptStartSecond=$SECOND
  	howLongHasRun=0
  	howManyMsgsHasGenerated=0
  fi
done

sendMsg(numOfMsgsToCatchUp){
  for ((i=1; i<=numOfMsgsToCatchUp; i++))
    do
	  sendMsg(numOfMsgsToCatchUp)
    done;
}