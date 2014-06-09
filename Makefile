TARGET ?= /kb/deployment

SERVICE=jnomics
SERVICE_DIR=$(TARGET)/services/$(SERVICE)
DEPLOYMENT_DIR=$(TARGET)
CLIENT_BIN_DIR=$(DEPLOYMENT_DIR)/bin
CLIENT_LIB_DIR=$(DEPLOYMENT_DIR)/lib
CLIENT_CONF_DIR=$(DEPLOYMENT_DIR)/conf
CLIENT_DOCS_DIR=$(DEPLOYMENT_DIR)/docs
CLIENT_CERT_DIR=$(DEPLOYMENT_DIR)/cert
SERVICE_BIN_DIR=$(SERVICE_DIR)/bin
SERVICE_CONF_DIR=$(SERVICE_DIR)/conf
SERVICE_LIB_DIR=$(SERVICE_DIR)/lib
SERVICE_DOCS_DIR=$(SERVICE_DIR)/webroot

#JAVA_HOME:=/kb/runtime/java
#ANT_HOME:=/kb/runtime/ant
#THRIFT_HOME:=/kb/runtime/thrift
#PATH:=${JAVA_HOME}/bin:${ANT_HOME}/bin:${THRIFT_HOME}/bin:${PATH}

all:

deploy-client: deploy

deploy: deploy-jnomics deploy-libs deploy-docs

test:
	cd kbase-test && ./test_var_service.sh
	cd kbase-test && ./test_rna_service.sh

deploy-docs: make-dest-dir
	cp -r docs/html/* $(SERVICE_DOCS_DIR)

make-dest-dir:
	mkdir -p $(SERVICE_BIN_DIR)
	mkdir -p $(SERVICE_CONF_DIR)
	mkdir -p $(SERVICE_LIB_DIR)
	mkdir -p $(CLIENT_BIN_DIR)
	mkdir -p $(CLIENT_LIB_DIR)
	mkdir -p $(CLIENT_CONF_DIR)
	mkdir -p $(CLIENT_DOCS_DIR)
	mkdir -p $(CLIENT_CERT_DIR)
	mkdir -p $(SERVICE_DOCS_DIR)

build-jnomics: thrift-9
	ant
	bin/make_scripts.sh jk bin

deploy-jnomics: deploy-libs
	cp conf/jnomics-kbase-client.properties.template conf/jnomics-kbase-client.properties
	cp conf/jnomics-kbase-server.properties.template conf/jnomics-kbase-server.properties
	cp conf/jnomics-kbase-client.properties $(CLIENT_CONF_DIR)
	cp conf/jnomics-kbase-server.properties $(SERVICE_CONF_DIR)
	cp bin/jk-* $(CLIENT_BIN_DIR)
	cp bin/jkbase $(CLIENT_BIN_DIR)
	cp bin/start-data-server.sh $(SERVICE_BIN_DIR)
	cp bin/start-compute-server.sh $(SERVICE_BIN_DIR)
	cp cert/truststore.jks $(CLIENT_CERT_DIR)

deploy-libs: build-jnomics make-dest-dir 
	cp dist/jnomics-manager-*.jar $(CLIENT_LIB_DIR)
	cp lib/*.jar $(CLIENT_LIB_DIR)
	cp dist/jnomics-manager-*.jar $(SERVICE_LIB_DIR)
	cp lib/*.jar $(SERVICE_LIB_DIR)



thrift-9:
	thrift_major_version=$(shell $(KB_RUNTIME)/thrift/bin/thrift -version | awk '{split($$3,a,"."); print a[2]}' )
ifneq ($(thrift_major_version), "9")
	CP_OLD=$CLASSPATH
	CLASSPATH=""
	wget http://mirror.symnds.com/software/Apache/thrift/0.9.1/thrift-0.9.1.tar.gz
	tar zxvf thrift-0.9.1.tar.gz
	cd thrift-0.9.1; JAVA_PREFIX=$(KB_RUNTIME)/thrift-0.9.1/lib ./configure --prefix=$(KB_RUNTIME)/thrift-0.9.1 --without-go --without-python --without-erlang --without-c_glib; make; make install;cd ..
	rm -f $(KB_RUNTIME)/thrift
	ln -s $(KB_RUNTIME)/thrift-0.9.1 $(KB_RUNTIME)/thrift
	CLASSPATH=$CP_OLD
endif

clean: 
	ant clean
