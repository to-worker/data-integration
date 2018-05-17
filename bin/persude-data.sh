#!/bin/sh

current_dir=`dirname $0`
. ${current_dir}/env.sh

java -Djava.ext.dirs=lib/ -cp data-integration-1.0-SNAPSHOT.jar com.zqykj.tldw.tool.kafka.PersudeProducer
