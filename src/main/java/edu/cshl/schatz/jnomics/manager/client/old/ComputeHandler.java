package edu.cshl.schatz.jnomics.manager.client.old;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * User: james
 */
public class ComputeHandler extends HandlerBase {

    private static final Map<String,Class<? extends HandlerBase>> taskMap = new HashMap<String, Class<? extends HandlerBase>>();
    static{
        taskMap.put("bowtie", BowtieHandler.class);
        taskMap.put("bwa", BWAHandler.class);
        taskMap.put("snp", SnpHandler.class);
        taskMap.put("status", JobStatusHandler.class);
        taskMap.put("list_jobs", JobListHandler.class);
        taskMap.put("pair_reads", PairReadsHandler.class);
        taskMap.put("single_reads", SingleReadsHandler.class);
        taskMap.put("gatk", ClientGatkHandler.class);
        taskMap.put("vcf_merge", MergeVCFHandler.class);
        taskMap.put("list_genomes", GenomeListHandler.class);
        taskMap.put("samtools_pipeline", SamtoolsPipelineHandler.class);
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
    public void handle(List<String> args, Properties properties) throws Exception {
        if(args.size() >= 1 && taskMap.containsKey(args.get(0))){
            Constructor ctor = taskMap.get(args.get(0)).getDeclaredConstructor();
            HandlerBase handler = (HandlerBase)ctor.newInstance();
            args.remove(args.get(0));
            handler.handle(args, properties);
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
