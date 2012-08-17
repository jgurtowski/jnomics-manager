#!/bin/bash

SCRIPT_PATH=`dirname "$0"`

bash ${SCRIPT_PATH}/start-compute-server.sh
bash ${SCRIPT_PATH}/start-data-server.sh