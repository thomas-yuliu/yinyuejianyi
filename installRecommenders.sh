#!/bin/bash

# to install recommenders on CentosOS6.5


# run from your desktop

gcloud compute copy-files daily_user_track_event_00*.txt try-deploy-2:/sparkproject --zone us-central1-b
gcloud compute copy-files ~/mavenWorkspace/spark-project/musicRecommender/config  try-deploy-2:/sparkproject --zone us-central1-b


# end run from your desktop


# run from the VM
sudo su -
yum install java-1.7.0-openjdk -y
export JAVA_HOME=/usr/lib/jvm/jre-1.7.0-openjdk.x86_64/
export INSTALL_LOCATION=/sparkproject
yum install wget -y
mkdir $INSTALL_LOCATION
cd $INSTALL_LOCATION
wget http://d3kbcqa49mib13.cloudfront.net/spark-1.5.1-bin-hadoop2.6.tgz
tar zxvf spark-1.5.1-bin-hadoop2.6.tgz

cd spark-1.5.1-bin-hadoop2.6
bin/spark-submit --driver-memory 4g --class mysparkproject.recommender2016.batchyRecommender.BatchRecommender --master local[12] ../musicRecommender-0.0.1-SNAPSHOT-jar-with-dependencies.jar



# for streaming
wget http://apache.mivzakim.net/kafka/0.8.2.0/kafka_2.10-0.8.2.0.tgz
tar -zxvf kafka_2.10-0.8.2.0.tgz
cd kafka_2.10-0.8.2.0
bin/zookeeper-server-start.sh config/zookeeper.properties

