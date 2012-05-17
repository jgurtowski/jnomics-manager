SERVICE = jnomics
SERVICE_DIR = $(TARGET)/services/$(SERVICE)
SERVICE_BIN_DIR = $(SERVICE_DIR)/bin
SERVICE_CONF_DIR = $(SERVICE_DIR)/conf

TARGET ?= /kb/deployment

JAVA_HOME:=/kb/runtime/java
ANT_HOME:=/kb/runtime/ant
PATH:=${JAVA_HOME}/bin:${ANT_HOME}/bin:${PATH}

all:

deploy: deploy-jnomics deploy-kbase-api

make-dest-dir:
	mkdir -p $(SERVICE_BIN_DIR)
	mkdir -p $(SERVICE_CONF_DIR)

build-jnomics:
	ant jar

deploy-jnomics: make-dest-dir build-jnomics
	cp jnomics-tools.jar $(SERVICE_BIN_DIR)

build-kbase-api:
	cd jnomics-kbase-api
	ant jar

deploy-kbase-api: make-dest-dir build-kbase-api
	cp jnomics-kbase-api/jnomics-kbase.jar $(SERVICE_BIN_DIR)


