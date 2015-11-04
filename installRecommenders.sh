#!/bin/bash

# to install recommenders on CentosOS6.5

export INSTALL_LOCATION=/sparkproject

sudo su -
yum install java-1.7.0-openjdk -y
export JAVA_HOME=/usr/lib/jvm/jre-1.7.0-openjdk.x86_64/
yum install wget -y
mkdir $INSTALL_LOCATION
cd $INSTALL_LOCATION
wget http://d3kbcqa49mib13.cloudfront.net/spark-1.5.1-bin-hadoop2.6.tgz
tar zxvf spark-1.5.1-bin-hadoop2.6.tgz

bin/spark-submit --driver-memory 4g --class mysparkproject.recommender2016.batchyRecommender.BatchRecommender --master local[12] ../musicRecommender-0.0.1-SNAPSHOT-jar-with-dependencies.jar
