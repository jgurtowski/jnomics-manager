package edu.cshl.schatz.jnomics.manager.client;

import edu.cshl.schatz.jnomics.manager.api.Authentication;
import edu.cshl.schatz.jnomics.manager.api.JnomicsCompute;
import edu.cshl.schatz.jnomics.manager.api.JnomicsData;
import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftJobID;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.util.List;

/**
 * User: james
 */

public class ClientGenomeListHandler extends ClientHandler{

    public ClientGenomeListHandler(){

    }

    public JnomicsArgument[] getArguments(){
        return new JnomicsArgument[0];
    }

    @Override
    public void handle(List<String> args) throws Exception {

        JnomicsData.Client client = JnomicsThriftClient.getFsClient();
        Authentication auth = JnomicsThriftClient.getAuthentication();
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