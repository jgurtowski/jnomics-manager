package edu.cshl.schatz.jnomics.manager.client;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * User: james
 */
public class ClientComputeHandler extends ClientHandler {


    private Properties properties;
    private static final Map<String,Class<? extends ClientHandler>> taskMap = new HashMap<String, Class<? extends ClientHandler>>();
    static{
        taskMap.put("bowtie", ClientBowtieHandler.class);
        taskMap.put("bwa", ClientBWAHandler.class);
        taskMap.put("snp", ClientSnpHandler.class);
        taskMap.put("status", ClientJobStatusHandler.class);
        taskMap.put("listjobs", ClientJobListHandler.class);
        taskMap.put("pair_reads", ClientPairReadsHandler.class);
        taskMap.put("single_reads", ClientSingleReadsHandler.class);
        taskMap.put("gatk", ClientGatkHandler.class);
    }

    public ClientComputeHandler(Properties props){
        properties = props;
    }

    @Override
    protected JnomicsArgument[] getArguments() {
        return new JnomicsArgument[0];
    }

    @Override
    public void handle(List<String> args) throws Exception {
        if(args.size() >= 1 && taskMap.containsKey(args.get(0))){
            Constructor ctor = taskMap.get(args.get(0)).getDeclaredConstructor();
            ClientHandler handler = (ClientHandler)ctor.newInstance();
            args.remove(args.get(0));
            handler.handle(args);
        }else{
            System.out.println("Available Tasks:");
            System.out.println();
            for(String tStrings: taskMap.keySet()){
                System.out.println(
                        String.format("%-30s %-30s",tStrings,taskMap.get(tStrings).newInstance().getDescription())
                );
            }
        }
    }
}
