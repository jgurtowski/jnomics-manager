package edu.cshl.schatz.jnomics.manager.server;


import edu.cshl.schatz.jnomics.manager.api.JnomicsCompute;
import edu.cshl.schatz.jnomics.manager.common.JnomicsApiConfig;
import org.apache.thrift.ProcessFunction;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.*;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;
import org.apache.thrift.server.TNonblockingServer;

import java.io.IOException;
import java.util.Properties;

public class JnomicsComputeServer {

    public static class JnomicsComputeProcessorHax<I extends JnomicsCompute.Iface>
            extends JnomicsCompute.Processor<I> implements TProcessor {

        public JnomicsComputeProcessorHax(I iface) {
            super(iface);
        }

        @Override
        public boolean process(TProtocol in, TProtocol out) throws TException {
            System.err.println("Processing from client: " + ((TSocket)in.getTransport()).getSocket().getInetAddress());
            return super.process(in,out);
        }
    }

    public static void main(String []args) throws TTransportException, IOException {

        Properties prop = JnomicsApiConfig.getServerProperties();

        int port = Integer.parseInt(prop.getProperty("compute-server-port"));

        JnomicsComputeHandler handler = new JnomicsComputeHandler(prop);
        JnomicsCompute.Processor processor = new JnomicsComputeProcessorHax(handler);
        //JnomicsCompute.Processor processor = new JnomicsCompute.Processor(handler);

        TServerTransport serverTransport = new TServerSocket(port);

        //TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(port);
        //TServer server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).processor(processor));
        TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

        System.out.println("Starting server port "+ port +"...");
        server.serve();
    }
}