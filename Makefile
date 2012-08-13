SERVICE = jnomics
SERVICE_DIR = $(TARGET)/services/$(SERVICE)
SERVICE_BIN_DIR = $(SERVICE_DIR)/bin
SERVICE_CONF_DIR = $(SERVICE_DIR)/conf

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

build-jnomics:
	ant jar

deploy-jnomics: make-dest-dir build-jnomics
	cp bin/jnomics.jar $(SERVICE_BIN_DIR)
