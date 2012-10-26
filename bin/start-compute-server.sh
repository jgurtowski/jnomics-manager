#!/bin/bash

SCRIPT_PATH=`dirname "$0"`

nohup java -Djkserver_keystore=${SCRIPT_PATH}/../cert/keystore.jks -Djava.library.path=/home/james/sources/hadoop/lib/native/Linux-amd64-64 -cp ${SCRIPT_PATH}/../conf:${SCRIPT_PATH}/../lib/jnomics.jar edu.cshl.schatz.jnomics.manager.server.JnomicsComputeServer > /tmp/jnomics-compute-service.log &
