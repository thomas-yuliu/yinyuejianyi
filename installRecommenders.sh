#!/bin/bash

# to install recommenders on CentosOS6.5

INSTALL_LOCATION = /sparkproject

sudo su -
yum install wget -y
mkdir $INSTALL_LOCATION
wget http://apache.arvixe.com/spark/spark-1.5.1/spark-1.5.1.tgz -O $INSTALL_LOCATION/spark-1.5.1.tgz
cd $INSTALL_LOCATION
tar zxvf spark-1.5.1.tgz
