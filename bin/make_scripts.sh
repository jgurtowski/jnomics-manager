#!/bin/bash

SCRIPT_PATH=`dirname "$0"`


classp=`find ${SCRIPT_PATH}/../lib/*.jar ${SCRIPT_PATH}/../dist/*.jar | awk  '{ s=s$i":"} END{print s}'`

java -cp $classp edu.cshl.schatz.jnomics.manager.client.CreateKbaseScripts $1 $2

