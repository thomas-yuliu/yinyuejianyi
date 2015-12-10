#!/bin/bash

# to install recommenders on google compute engine



# on google engine
resize your machine first
https://cloud.google.com/compute/docs/disks/persistent-disks#repartitionrootpd

then enable port 8080 accessible on the master node:
Name	Source tag / IP range	Allowed protocols / ports	Target tags

client-to-spark-master	0.0.0.0/0	tcp:8080	Apply to all targets


then enable ssh key less access among nodes:

eval `ssh-agent`
ssh-add ~/.ssh/google_compute_engine
gcloud compute ssh --ssh-flag="-A" instance-1


sudo su -
export INSTALL_LOCATION=/sparkproject
mkdir $INSTALL_LOCATION
chmod 777 $INSTALL_LOCATION
yum install java-1.7.0-openjdk -y
exit
export INSTALL_LOCATION=/sparkproject
cd $INSTALL_LOCATION
mkdir config
mkdir sparkeventlogs
mkdir batchrec
export JAVA_HOME=/usr/lib/jvm/jre-1.7.0-openjdk.x86_64/
yum install wget -y
wget http://d3kbcqa49mib13.cloudfront.net/spark-1.5.1-bin-hadoop2.6.tgz
tar zxvf spark-1.5.1-bin-hadoop2.6.tgz

#run copy from host to vm below

export INSTALL_LOCATION=/sparkproject
cd $INSTALL_LOCATION
./generateDailyEvents.sh daily
./generateDailyEvents.sh acc


#on master only
export INSTALL_LOCATION=/sparkproject
cd $INSTALL_LOCATION/spark-1.5.1-bin-hadoop2.6
export JAVA_HOME=/usr/lib/jvm/jre-1.7.0-openjdk.x86_64/
sbin/stop-all.sh
sbin/start-all.sh



# run from your desktop
gcloud compute copy-files ~/mavenWorkspace/spark-project/musicRecommender/config/recommenderConfig.json  instance-1:/sparkproject/config --zone us-central1-b

gcloud compute copy-files ~/mavenWorkspace/spark-project/musicRecommender/generateDailyEvents.sh  instance-1:/sparkproject --zone us-central1-b

gcloud compute copy-files ~/mavenWorkspace/spark-project/musicRecommender/target/musicRecommender-0.0.1-SNAPSHOT-jar-with-dependencies.jar instance-1:/sparkproject --zone us-central1-b

gcloud compute copy-files ~/deployment/spark-1.3.1-bin-hadoop2.6/conf/slaves instance-1:/sparkproject/spark-1.5.1-bin-hadoop2.6/conf/ --zone us-central1-b

gcloud compute copy-files ~/deployment/spark-1.3.1-bin-hadoop2.6/conf/log4j.properties instance-1:/sparkproject/spark-1.5.1-bin-hadoop2.6/conf/ --zone us-central1-b

gcloud compute copy-files ~/deployment/spark-1.3.1-bin-hadoop2.6/conf/spark-defaults.conf instance-1:/sparkproject/spark-1.5.1-bin-hadoop2.6/conf/ --zone us-central1-b

gcloud compute copy-files ~/deployment/spark-1.3.1-bin-hadoop2.6/conf/spark-env.sh instance-1:/sparkproject/spark-1.5.1-bin-hadoop2.6/conf/ --zone us-central1-b



gcloud compute copy-files ~/mavenWorkspace/spark-project/musicRecommender/config/recommenderConfig.json  instance-2:/sparkproject/config --zone us-central1-b

gcloud compute copy-files ~/mavenWorkspace/spark-project/musicRecommender/generateDailyEvents.sh  instance-2:/sparkproject --zone us-central1-b

gcloud compute copy-files ~/deployment/spark-1.3.1-bin-hadoop2.6/conf/slaves instance-2:/sparkproject/spark-1.5.1-bin-hadoop2.6/conf/ --zone us-central1-b

gcloud compute copy-files ~/deployment/spark-1.3.1-bin-hadoop2.6/conf/log4j.properties instance-2:/sparkproject/spark-1.5.1-bin-hadoop2.6/conf/ --zone us-central1-b

gcloud compute copy-files ~/deployment/spark-1.3.1-bin-hadoop2.6/conf/spark-defaults.conf instance-2:/sparkproject/spark-1.5.1-bin-hadoop2.6/conf/ --zone us-central1-b

gcloud compute copy-files ~/deployment/spark-1.3.1-bin-hadoop2.6/conf/spark-env.sh instance-2:/sparkproject/spark-1.5.1-bin-hadoop2.6/conf/ --zone us-central1-b



gcloud compute copy-files ~/mavenWorkspace/spark-project/musicRecommender/config/recommenderConfig.json  instance-3:/sparkproject/config --zone us-central1-b

gcloud compute copy-files ~/mavenWorkspace/spark-project/musicRecommender/generateDailyEvents.sh  instance-3:/sparkproject --zone us-central1-b

gcloud compute copy-files ~/deployment/spark-1.3.1-bin-hadoop2.6/conf/slaves instance-3:/sparkproject/spark-1.5.1-bin-hadoop2.6/conf/ --zone us-central1-b

gcloud compute copy-files ~/deployment/spark-1.3.1-bin-hadoop2.6/conf/log4j.properties instance-3:/sparkproject/spark-1.5.1-bin-hadoop2.6/conf/ --zone us-central1-b

gcloud compute copy-files ~/deployment/spark-1.3.1-bin-hadoop2.6/conf/spark-defaults.conf instance-3:/sparkproject/spark-1.5.1-bin-hadoop2.6/conf/ --zone us-central1-b

gcloud compute copy-files ~/deployment/spark-1.3.1-bin-hadoop2.6/conf/spark-env.sh instance-3:/sparkproject/spark-1.5.1-bin-hadoop2.6/conf/ --zone us-central1-b



gcloud compute copy-files ~/mavenWorkspace/spark-project/musicRecommender/config/recommenderConfig.json  instance-4:/sparkproject/config --zone us-central1-b

gcloud compute copy-files ~/mavenWorkspace/spark-project/musicRecommender/generateDailyEvents.sh  instance-4:/sparkproject --zone us-central1-b

gcloud compute copy-files ~/deployment/spark-1.3.1-bin-hadoop2.6/conf/slaves instance-4:/sparkproject/spark-1.5.1-bin-hadoop2.6/conf/ --zone us-central1-b

gcloud compute copy-files ~/deployment/spark-1.3.1-bin-hadoop2.6/conf/log4j.properties instance-4:/sparkproject/spark-1.5.1-bin-hadoop2.6/conf/ --zone us-central1-b

gcloud compute copy-files ~/deployment/spark-1.3.1-bin-hadoop2.6/conf/spark-defaults.conf instance-4:/sparkproject/spark-1.5.1-bin-hadoop2.6/conf/ --zone us-central1-b

gcloud compute copy-files ~/deployment/spark-1.3.1-bin-hadoop2.6/conf/spark-env.sh instance-4:/sparkproject/spark-1.5.1-bin-hadoop2.6/conf/ --zone us-central1-b



# run from the GCE
# standalone cluster batch rec
export INSTALL_LOCATION=/sparkproject
cd $INSTALL_LOCATION/spark-1.5.1-bin-hadoop2.6
bin/spark-submit --class mysparkproject.recommender2016.batchyRecommender.BatchRecommender --master spark://instance-1:7077  --executor-memory 6G --total-executor-cores 4 ../musicRecommender-0.0.1-SNAPSHOT-jar-with-dependencies.jar

# standalone cluster model trainer
export INSTALL_LOCATION=/sparkproject
cd $INSTALL_LOCATION/spark-1.5.1-bin-hadoop2.6
bin/spark-submit --class mysparkproject.recommender2016.batchyModelTrainer.batchyModelTrainer --master spark://instance-1:7077  --executor-memory 6G --total-executor-cores 4 ../musicRecommender-0.0.1-SNAPSHOT-jar-with-dependencies.jar

# standalone streaming rec
export INSTALL_LOCATION=/sparkproject
cd $INSTALL_LOCATION/spark-1.5.1-bin-hadoop2.6
bin/spark-submit --class mysparkproject.recommender2016.streamingRecommender.StreamingRecommender --master spark://instance-1:7077 --executor-memory 6G --total-executor-cores 4 ../musicRecommender-0.0.1-SNAPSHOT-jar-with-dependencies.jar



============================================================================================================

# to install recommenders on CentosOS6.4
cd ~/ambari-vagrant/centos6.4
vagrant ssh c6401

sudo su -
yum install java-1.7.0-openjdk -y
export JAVA_HOME=/usr/lib/jvm/jre-1.7.0-openjdk.x86_64/
export INSTALL_LOCATION=/sparkproject
yum install wget -y
mkdir $INSTALL_LOCATION
mkdir $INSTALL_LOCATION/sparkeventlogs
mkdir $INSTALL_LOCATION/batchrec
cd $INSTALL_LOCATION
wget http://d3kbcqa49mib13.cloudfront.net/spark-1.5.1-bin-hadoop2.6.tgz
tar zxvf spark-1.5.1-bin-hadoop2.6.tgz



# run from the VM
# standalone cluster batch rec
export INSTALL_LOCATION=/sparkproject
cd $INSTALL_LOCATION/spark-1.5.1-bin-hadoop2.6
bin/spark-submit --class mysparkproject.recommender2016.batchyRecommender.BatchRecommender --master spark://c6401.ambari.apache.org:7077  --executor-memory 5G --total-executor-cores 3 ../musicRecommender-0.0.1-SNAPSHOT-jar-with-dependencies.jar

# standalone cluster model trainer
export INSTALL_LOCATION=/sparkproject
cd $INSTALL_LOCATION/spark-1.5.1-bin-hadoop2.6
bin/spark-submit --class mysparkproject.recommender2016.batchyModelTrainer.batchyModelTrainer --master spark://c6401.ambari.apache.org:7077  --executor-memory 5G --total-executor-cores 3 ../musicRecommender-0.0.1-SNAPSHOT-jar-with-dependencies.jar

# standalone streaming rec
export INSTALL_LOCATION=/sparkproject
cd $INSTALL_LOCATION/spark-1.5.1-bin-hadoop2.6
bin/spark-submit --class mysparkproject.recommender2016.streamingRecommender.StreamingRecommender --master spark://c6401.ambari.apache.org:7077 --executor-memory 5G --total-executor-cores 3 ../musicRecommender-0.0.1-SNAPSHOT-jar-with-dependencies.jar

# test mappartition
bin/spark-submit --class TestNoMapPartition --master spark://c6401.ambari.apache.org:7077 --executor-memory 5G --total-executor-cores 3 ../musicRecommender-0.0.1-SNAPSHOT-jar-with-dependencies.jar




# installing kafka for streaming
sudo su -
export INSTALL_LOCATION=/sparkproject
cd $INSTALL_LOCATION
wget http://apache.mivzakim.net/kafka/0.8.2.0/kafka_2.10-0.8.2.0.tgz
tar -zxvf kafka_2.10-0.8.2.0.tgz
cd kafka_2.10-0.8.2.0

#zookeeper
sudo su -
export INSTALL_LOCATION=/sparkproject
cd $INSTALL_LOCATION
cd kafka_2.10-0.8.2.0
bin/zookeeper-server-start.sh config/zookeeper.properties > $INSTALL_LOCATION/zookeeper.log

# kafka server
sudo su -
export INSTALL_LOCATION=/sparkproject
cd $INSTALL_LOCATION
cd kafka_2.10-0.8.2.0
#need to copy multi config/server.properties
cp /vagrant/server1.properties config/server1.properties
cp /vagrant/server2.properties config/server2.properties
cp /vagrant/server3.properties config/server3.properties
cp /vagrant/server4.properties config/server4.properties
bin/kafka-server-start.sh config/server1.properties > $INSTALL_LOCATION/kafka.log
bin/kafka-server-start.sh config/server2.properties > $INSTALL_LOCATION/kafka.log
bin/kafka-server-start.sh config/server3.properties > $INSTALL_LOCATION/kafka.log
bin/kafka-server-start.sh config/server4.properties > $INSTALL_LOCATION/kafka.log

# create topic
sudo su -
export INSTALL_LOCATION=/sparkproject
cd $INSTALL_LOCATION
cd kafka_2.10-0.8.2.0
bin/kafka-topics.sh --create --zookeeper localhost:2181/kafka --replication-factor 1 --partitions 2 --topic test

＃producer
sudo su -
export INSTALL_LOCATION=/sparkproject
cd $INSTALL_LOCATION
cd kafka_2.10-0.8.2.0
echo "xxx" | bin/kafka-console-producer.sh --broker-list localhost:9092,localhost:9093,localhost:9094 --sync --topic test

# consumer
sudo su -
export INSTALL_LOCATION=/sparkproject
cd $INSTALL_LOCATION
cd kafka_2.10-0.8.2.0
bin/kafka-console-consumer.sh --zookeeper localhost:2181  --topic test



# on local vagrant VM only
# first on workspace
cp ~/mavenWorkspace/spark-project/musicRecommender/target/musicRecommender-0.0.1-SNAPSHOT-jar-with-dependencies.jar ~/ambari-vagrant/centos6.4
cp ~/mavenWorkspace/spark-project/musicRecommender/config/recommenderConfig.json  ~/ambari-vagrant/centos6.4
cp ~/mavenWorkspace/spark-project/musicRecommender/generateDailyEvents.sh  ~/ambari-vagrant/centos6.4
cp ~/deployment/spark-1.3.1-bin-hadoop2.6/conf/slaves ~/ambari-vagrant/centos6.4/
cp ~/deployment/spark-1.3.1-bin-hadoop2.6/conf/log4j.properties ~/ambari-vagrant/centos6.4/
cp ~/deployment/spark-1.3.1-bin-hadoop2.6/conf/spark-defaults.conf ~/ambari-vagrant/centos6.4/
cp ~/deployment/spark-1.3.1-bin-hadoop2.6/conf/spark-env.sh ~/ambari-vagrant/centos6.4/
#cp /sparkproject/testMapPartition.txt ~/ambari-vagrant/centos6.4/

# then on vm 
sudo su -
export INSTALL_LOCATION=/sparkproject
cd $INSTALL_LOCATION
yes | cp /vagrant/musicRecommender-0.0.1-SNAPSHOT-jar-with-dependencies.jar $INSTALL_LOCATION

yes | cp /vagrant/generateDailyEvents.sh $INSTALL_LOCATION

./generateDailyEvents.sh daily
./generateDailyEvents.sh acc
mkdir config
yes | cp /vagrant/recommenderConfig.json $INSTALL_LOCATION/config

yes | cp /vagrant/spark-defaults.conf $INSTALL_LOCATION/spark-1.5.1-bin-hadoop2.6/conf

yes | cp /vagrant/spark-env.sh $INSTALL_LOCATION/spark-1.5.1-bin-hadoop2.6/conf

yes | cp /vagrant/log4j.properties $INSTALL_LOCATION/spark-1.5.1-bin-hadoop2.6/conf

yes | cp /vagrant/slaves $INSTALL_LOCATION/spark-1.5.1-bin-hadoop2.6/conf

#yes | cp /vagrant/testMapPartition.txt $INSTALL_LOCATION

#cp /vagrant/acc* .
#cp /vagrant/daily* .
mkdir $INSTALL_LOCATION/sparkeventlogs
export JAVA_HOME=/usr/lib/jvm/jre-1.7.0-openjdk.x86_64/

#on master only
export INSTALL_LOCATION=/sparkproject
cd $INSTALL_LOCATION/spark-1.5.1-bin-hadoop2.6
sbin/start-all.sh
#manually enter password 'vagrant' for all hosts for now

sbin/stop-all.sh

# vm make space
rm -rf $INSTALL_LOCATION/spark-1.5.1-bin-hadoop2.6/work/*
rm -rf /tmp/spark*

#clean up input files
rm -f accumulatedRatings-new*



＃local batch rec
bin/spark-submit --driver-memory 4g --class mysparkproject.recommender2016.batchyRecommender.BatchRecommender --master local[12] ../musicRecommender-0.0.1-SNAPSHOT-jar-with-dependencies.jar test

# local streaming
bin/spark-submit --driver-memory 4g --class mysparkproject.recommender2016.streamingRecommender.StreamingRecommender --master local[12] ../musicRecommender-0.0.1-SNAPSHOT-jar-with-dependencies.jar 

