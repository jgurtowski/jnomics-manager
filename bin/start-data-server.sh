#!/bin/bash

SCRIPT_PATH=`dirname "$0"`

nohup java -Djkserver_keystore=${SCRIPT_PATH}/../cert/keystore.jks -cp ${SCRIPT_PATH}/../conf:${SCRIPT_PATH}/../lib/jnomics.jar edu.cshl.schatz.jnomics.manager.server.JnomicsDataServer > /tmp/jnomics-data-server.log  &
