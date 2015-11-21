#!/bin/bash

# to install recommenders on google compute engine


# run from your desktop

gcloud compute copy-files daily_user_track_event_00*.txt try-deploy-2:/sparkproject --zone us-central1-b
gcloud compute copy-files ~/mavenWorkspace/spark-project/musicRecommender/config  try-deploy-2:/sparkproject --zone us-central1-b




# to install recommenders on CentosOS6.4
sudo su -
yum install java-1.7.0-openjdk -y
export JAVA_HOME=/usr/lib/jvm/jre-1.7.0-openjdk.x86_64/
export INSTALL_LOCATION=/sparkproject
yum install wget -y
mkdir $INSTALL_LOCATION
mkdir ($INSTALL_LOCATION)/sparkeventlogs
cd $INSTALL_LOCATION
wget http://d3kbcqa49mib13.cloudfront.net/spark-1.5.1-bin-hadoop2.6.tgz
tar zxvf spark-1.5.1-bin-hadoop2.6.tgz

cd spark-1.5.1-bin-hadoop2.6


# run from the VM
# standalone cluster batch rec
cd spark-1.5.1-bin-hadoop2.6
bin/spark-submit --class mysparkproject.recommender2016.batchyRecommender.BatchRecommender --master spark://c6501.ambari.apache.org:7077  --executor-memory 1G --total-executor-cores 3 ../musicRecommender-0.0.1-SNAPSHOT-jar-with-dependencies.jar

# standalone cluster model trainer
bin/spark-submit --class mysparkproject.recommender2016.batchyModelTrainer.batchyModelTrainer --master spark://c6501.ambari.apache.org:7077  --executor-memory 1G --total-executor-cores 3 ../musicRecommender-0.0.1-SNAPSHOT-jar-with-dependencies.jar

# standalone streaming rec
bin/spark-submit --class mysparkproject.recommender2016.streamingRecommender.StreamingRecommender --master spark://c6501.ambari.apache.org:7077 --executor-memory 1G --total-executor-cores 3 ../musicRecommender-0.0.1-SNAPSHOT-jar-with-dependencies.jar




# for streaming
sudo su -
export INSTALL_LOCATION=/sparkproject
cd $INSTALL_LOCATION
wget http://apache.mivzakim.net/kafka/0.8.2.0/kafka_2.10-0.8.2.0.tgz
tar -zxvf kafka_2.10-0.8.2.0.tgz
cd kafka_2.10-0.8.2.0

#zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# kafka server
bin/kafka-server-start.sh config/server.properties

# create topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

＃producer
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test 





# on local vagrant VM only
# first on workspace
cp ~/mavenWorkspace/spark-project/musicRecommender/target/musicRecommender-0.0.1-SNAPSHOT-jar-with-dependencies.jar ~/ambari-vagrant/centos6.4
cp ~/mavenWorkspace/spark-project/musicRecommender/config/recommenderConfig.json  ~/ambari-vagrant/centos6.4
cp ~/mavenWorkspace/spark-project/musicRecommender/installRecommenders.sh  ~/ambari-vagrant/centos6.4
# then on vm 
sudo su -
export INSTALL_LOCATION=/sparkproject
cd $INSTALL_LOCATION

cp /vagrant/musicRecommender-0.0.1-SNAPSHOT-jar-with-dependencies.jar .
cp /vagrant/installRecommenders.sh .
mkdir config
cp /vagrant/recommenderConfig.json config
#cp /vagrant/acc* .
#cp /vagrant/daily* .
mkdir $INSTALL_LOCATION/sparkeventlogs
cp /vagrant/spark-defaults.conf /sparkproject/spark-1.5.1-bin-hadoop2.6/conf
cp /vagrant/spark-env.sh /sparkproject/spark-1.5.1-bin-hadoop2.6/conf
cp /vagrant/log4j.properties /sparkproject/spark-1.5.1-bin-hadoop2.6/conf
cp /vagrant/slaves /sparkproject/spark-1.5.1-bin-hadoop2.6/conf

#on master only
export INSTALL_LOCATION=/sparkproject
cp /vagrant/slaves $INSTALL_LOCATION/spark-1.5.1-bin-hadoop2.6/conf
sbin/start-all.sh
#manually enter password 'vagrant' for all hosts for now

# vm make space
rm -rf /sparkproject/spark-1.5.1-bin-hadoop2.6/work/*

#clean up
rm -f accumulatedRatings-new*



＃local batch rec
bin/spark-submit --driver-memory 4g --class mysparkproject.recommender2016.batchyRecommender.BatchRecommender --master local[12] ../musicRecommender-0.0.1-SNAPSHOT-jar-with-dependencies.jar test

# local streaming
bin/spark-submit --driver-memory 4g --class mysparkproject.recommender2016.streamingRecommender.StreamingRecommender --master local[12] ../musicRecommender-0.0.1-SNAPSHOT-jar-with-dependencies.jar 

