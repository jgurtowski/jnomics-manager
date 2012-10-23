package edu.cshl.schatz.jnomics.manager.server;

import edu.cshl.schatz.jnomics.manager.api.JnomicsData;
import edu.cshl.schatz.jnomics.manager.common.JnomicsApiConfig;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;

/**
 * User: james
 */
public class JnomicsDataServer {


    private static int DEFAULTPORT = 43345;



    public static class JnomicsDataProcessorHax<I extends JnomicsData.Iface>
            extends JnomicsData.Processor<I> implements TProcessor {

        public JnomicsDataProcessorHax(I iface) {
            super(iface);
        }

        @Override
        public boolean process(TProtocol in, TProtocol out) throws TException {
            System.err.println("Processing from client: " + ((TSocket)in.getTransport()).getSocket().getInetAddress());
            return super.process(in,out);
        }
    }

    public static void main(String []args) throws TTransportException, IOException, InterruptedException {

        Properties prop = JnomicsApiConfig.getServerProperties();

        int port = Integer.parseInt(prop.getProperty("data-server-port",Integer.toString(DEFAULTPORT)));
        String host = prop.getProperty("data-server-host");
        String keyStore = System.getProperty("jkserver_keystore");
        if(null == keyStore || !new File(keyStore).exists()){
            throw new IOException("Cannot find key store: " + keyStore);
        }
        
        JnomicsDataHandler handler = new JnomicsDataHandler(prop);
        Thread garbageCollectorThread = new Thread(new JnomicsHandleGarbageCollector(handler));
        JnomicsData.Processor processor = new JnomicsDataProcessorHax(handler);

        TSSLTransportFactory.TSSLTransportParameters params = new TSSLTransportFactory.TSSLTransportParameters();
        params.setKeyStore(keyStore,"kbasekeystore");
        TServerTransport serverTransport = TSSLTransportFactory.getServerSocket(port,10000,
                InetAddress.getByName(host),params);

        TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

        System.out.println("Starting server port "+ port +"...");
        garbageCollectorThread.start();
        server.serve();
        garbageCollectorThread.interrupt();
    }
}