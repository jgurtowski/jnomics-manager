#!/bin/bash

SCRIPT_PATH=`dirname "$0"`


classp=`find ${SCRIPT_PATH}/../lib/*.jar ${SCRIPT_PATH}/../dist/*.jar 2> /dev/null | awk  '{ s=s$1":"} END{print s}'`
nohup java -Djkserver_keystore=${SCRIPT_PATH}/../cert/keystore.jks -cp ${SCRIPT_PATH}/../conf:${classp} edu.cshl.schatz.jnomics.manager.server.JnomicsDataServer > /home/jnomics/jnomics-manager/log/jnomics-data-server.log &
