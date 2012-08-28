package edu.cshl.schatz.jnomics.manager.client;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: james
 */
public class ClientComputeHandler extends ClientHandler {

    private static final Map<String,Class<? extends ClientHandler>> taskMap = new HashMap<String, Class<? extends ClientHandler>>();
    static{
        taskMap.put("bowtie", ClientBowtieHandler.class);
        taskMap.put("bwa", ClientBWAHandler.class);
        taskMap.put("snp", ClientSnpHandler.class);
        taskMap.put("status", ClientJobStatusHandler.class);
        taskMap.put("list_jobs", ClientJobListHandler.class);
        taskMap.put("pair_reads", ClientPairReadsHandler.class);
        taskMap.put("single_reads", ClientSingleReadsHandler.class);
        taskMap.put("gatk", ClientGatkHandler.class);
        taskMap.put("vcf_merge", ClientMergeVCFHandler.class);
        taskMap.put("list_genomes", ClientGenomeListHandler.class);
        taskMap.put("samtools_pipeline", ClientSamtoolsPipelineHandler.class);
    }
    
    private String[] getHelpOrder(){
        return new String[]{
                "Alignment:",
                "bowtie",
                "bwa",
                "",
                "Variation:",
                "snp",
                "samtools_pipeline",
                "gatk",
                "",
                "Utility:",
                "list_jobs",
                "status",
                "list_genomes",
                "vcf_merge",
                "pair_reads",
                "single_reads"
        };
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
            for(String tStrings: getHelpOrder()){
                if(!taskMap.containsKey(tStrings))
                    System.out.println(tStrings);
                else
                    System.out.println(
                            String.format("%-30s %-30s",tStrings,taskMap.get(tStrings).newInstance().getDescription())
                    );
            }
        }
    }
}
