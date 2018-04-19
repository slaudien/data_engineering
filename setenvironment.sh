#!/bin/bash

export SPARK_HOME=/home/spark

export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.6-src.zip:$PYTHONPATH

export JAVA_HOME=/etc/alternatives/java_sdk

export PATH=$HOME/sbt/bin:$PATH