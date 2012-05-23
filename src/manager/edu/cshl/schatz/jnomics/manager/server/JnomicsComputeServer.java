package edu.cshl.schatz.jnomics.manager.server;


import edu.cshl.schatz.jnomics.manager.api.JnomicsCompute;
import edu.cshl.schatz.jnomics.manager.common.JnomicsApiConfig;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.util.Properties;

public class JnomicsComputeServer {


    public static void main(String []args) throws TTransportException, IOException {

        Properties prop = JnomicsApiConfig.get();

        int port = Integer.parseInt(prop.getProperty("compute-server-port"));

        JnomicsComputeHandler handler = new JnomicsComputeHandler(prop);
        JnomicsCompute.Processor processor = new JnomicsCompute.Processor(handler);

        TServerTransport serverTransport = new TServerSocket(port);
        TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

        System.out.println("Starting server port "+ port +"...");
        server.serve();
    }
}