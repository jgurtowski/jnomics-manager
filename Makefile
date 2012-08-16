SERVICE=jnomics
SERVICE_DIR=$(TARGET)/services/$(SERVICE)
SERVICE_BIN_DIR=$(SERVICE_DIR)/bin
SERVICE_CONF_DIR=$(SERVICE_DIR)/conf
SERVICE_LIB_DIR=$(SERVICE_DIR)/lib

TARGET ?= /kb/deployment

JAVA_HOME:=/kb/runtime/java
ANT_HOME:=/kb/runtime/ant
THRIFT_HOME:=/kb/runtime/thrift
PATH:=${JAVA_HOME}/bin:${ANT_HOME}/bin:${THRIFT_HOME}/bin:${PATH}

all:

deploy: deploy-jnomics

make-dest-dir:
	mkdir -p $(SERVICE_BIN_DIR)
	mkdir -p $(SERVICE_CONF_DIR)
	mkdir -p $(SERVICE_LIB_DIR)

build-jnomics:
	ant jar

deploy-jnomics: make-dest-dir build-jnomics
	cp bin/jnomics.jar $(SERVICE_LIB_DIR)
	cp conf/jnomics-kbase-client.properties $(SERVICE_CONF_DIR)
	cp conf/jnomics-kbase-server.properties $(SERVICE_CONF_DIR)
	cp bin/jkbase $(SERVICE_BIN_DIR)
	cp docs/KBASE-DEPLOY-README $(SERVICE_DIR)