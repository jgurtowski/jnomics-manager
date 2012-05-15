package edu.cshl.schatz.jnomics.kbase.thrift.client;

import edu.cshl.schatz.jnomics.kbase.thrift.api.Authentication;
import edu.cshl.schatz.jnomics.kbase.thrift.api.JnomicsCompute;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * User: james
 */
public class ClientComputeHandler implements ClientHandler {

    private Properties properties;
    private static final Options opts = new Options();
    private static final Map<String,ClientHandler> taskMap = new HashMap<String,ClientHandler>();
    static{
        opts.addOption("task",true,"list contents of directory");
        
        taskMap.put("alignBowtie",new ClientBowtieHandler());
    }

    public ClientComputeHandler(Properties props){
        properties = props;
    }

    public static class ClientBowtieHandler implements ClientHandler{

        private static final Options opts = new Options();
        static{
            opts.addOption("in",true,"Input file");
            opts.addOption("out",true,"Output directory");
            opts.addOption("organism",true,"Organism for alignment index");
        }
        
        @Override
        public void handle(String[] args) throws Exception {
            BasicParser parser = new BasicParser();

        }
    }

    @Override
    public void handle(String[] args) throws Exception {
        String thriftComputeHost = properties.getProperty("compute-server-host");
        int thriftComputePort = Integer.parseInt(properties.getProperty("compute-server-port"));
        TTransport transport = new TSocket(thriftComputeHost, thriftComputePort);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        JnomicsCompute.Client client = new JnomicsCompute.Client(protocol);

        Authentication auth = new Authentication(properties.getProperty("username"),properties.getProperty("password"));


        System.out.println(client.alignBowtie("reads.pe","motley","reads_align",auth));
        /*BasicParser parser = new BasicParser();
        CommandLine cli = parser.parse(opts,args);
        HelpFormatter formatter = new HelpFormatter();
        
        String val;
        if(cli.hasOption("task")){
            val = cli.getOptionValue("task");
            if(taskMap.containsKey(val)){
                taskMap.get(val).handle(args);
            }else{
                formatter.printHelp("Unknown task "+ val,opts);
            }
        }else{
            formatter.printHelp("Specify a task",opts);
            System.out.println("available tasks:");
            for(String t: taskMap.keySet()){
                System.out.println(t);
            }
        } */
    }
}
