package edu.cshl.schatz.jnomics.manager.client.old;

import edu.cshl.schatz.jnomics.manager.api.Authentication;
import edu.cshl.schatz.jnomics.manager.api.JnomicsData;

import java.util.List;
import java.util.Properties;

/**
 * User: james
 */

public class GenomeListHandler extends HandlerBase {

    public GenomeListHandler(){

    }

    public JnomicsArgument[] getArguments(){
        return new JnomicsArgument[]{
                new JnomicsArgument("a",false,false,"list all genomes"),
                new JnomicsArgument("kb",false,false,"list only kbase genomes"),
                new JnomicsArgument("h", false,false, "help")
        };
    }

    @Override
    public void handle(List<String> args, Properties properties) throws Exception {
        JnomicsData.Client client = JnomicsThriftClient.getFsClient(properties);
        Authentication auth = JnomicsThriftClient.getAuthentication(properties);
        List<String> genomes = client.listGenomes(auth);
        boolean all = false, kbase =false;
        if(args.contains("-a")){
            all = true;
        }
        if(args.contains("-kb")){
            kbase = true;
        }
        if(args.contains("-h")){
            for(JnomicsArgument arg: getArguments()){
                System.out.println("-"+arg.getName()+"\t"+arg.getDescription());
            }
            System.exit(1);
        }
        
        
        System.out.println("Available Genomes:");
        if(0 == genomes.size()){
            System.out.println("None available");
        }else{
            for(String g: genomes){
                if(!all && !kbase && g.startsWith("kb_"))
                    continue;
                if(kbase && !g.startsWith("kb_"))
                    continue;
                if(g.startsWith("kb_")){
                    System.out.println(g.replace("kb_","kb|"));
                }else{
                    System.out.println(g);
                }
            }
        }
    }

    @Override
    public String getDescription() {
        return "List available genomes";
    }
}