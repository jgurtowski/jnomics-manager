package edu.cshl.schatz.jnomics.kbase.thrift.client;

import edu.cshl.schatz.jnomics.kbase.thrift.api.Authentication;
import edu.cshl.schatz.jnomics.kbase.thrift.api.JnomicsCompute;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.Properties;

/**
 * User: james
 */
public abstract class ClientThriftHandler extends ClientHandler{

    private Properties properties;
    private TTransport thriftTransport;

    public ClientThriftHandler(Properties properties){
        this.properties = properties;
    }

    protected JnomicsCompute.Client getThriftClient() throws TTransportException {
        String thriftComputeHost = properties.getProperty("compute-server-host");
        int thriftComputePort = Integer.parseInt(properties.getProperty("compute-server-port"));
        thriftTransport = new TSocket(thriftComputeHost, thriftComputePort);
        thriftTransport.open();
        TProtocol protocol = new TBinaryProtocol(thriftTransport);
        JnomicsCompute.Client client= new JnomicsCompute.Client(protocol);
        return client;
    }

    protected Authentication getAuth(){
        return new Authentication(properties.getProperty("username"),
                properties.getProperty("password"));
    }

    protected void closeTransport(){
        thriftTransport.close();
    }
}


