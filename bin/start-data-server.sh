#!/bin/bash

SCRIPT_PATH=`dirname "$0"`

nohup java -cp ${SCRIPT_PATH}/../conf:${SCRIPT_PATH}/../lib/jnomics.jar edu.cshl.schatz.jnomics.manager.server.JnomicsDataServer > /tmp/jnomics-data-server.log