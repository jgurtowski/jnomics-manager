package edu.cshl.schatz.jnomics.manager.client;

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
        return new JnomicsArgument[0];
    }

    @Override
    public void handle(List<String> args, Properties properties) throws Exception {
        JnomicsData.Client client = JnomicsThriftClient.getFsClient(properties);
        Authentication auth = JnomicsThriftClient.getAuthentication(properties);
        List<String> genomes = client.listGenomes(auth);
        System.out.println("Available Genomes:");
        if(0 == genomes.size()){
            System.out.println("None available");
        }else{
            for(String g: genomes){
                System.out.println(g);
            }
        }
    }

    @Override
    public String getDescription() {
        return "List available genomes";
    }
}