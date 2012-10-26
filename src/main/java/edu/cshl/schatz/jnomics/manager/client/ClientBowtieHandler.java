package edu.cshl.schatz.jnomics.manager.client;

import edu.cshl.schatz.jnomics.manager.api.Authentication;
import edu.cshl.schatz.jnomics.manager.api.JnomicsCompute;
import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftJobID;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.util.List;
import java.util.Properties;

/**
 * User: james
 */

public class ClientBowtieHandler extends ClientHandler{

    public ClientBowtieHandler(){

    }

    public JnomicsArgument[] getArguments(){
        return new JnomicsArgument[]{
                new JnomicsArgument("in",true,true,"Input file"),
                new JnomicsArgument("out",true,true,"Output dir"),
                new JnomicsArgument("organism",true,true,"Organism for alignment index"),
                new JnomicsArgument("opts",false,true,"Pass Alignment options to bowite")
        };
    }

    @Override
    public void handle(List<String> args) throws Exception {
        HelpFormatter formatter = new HelpFormatter();
        Options options = getOptions();
        CommandLine cli = null;
        try{
            cli = parseArguments(args);
        }catch(JnomicsArgumentException e){
            formatter.printHelp(e.toString(),options);
            return;
        }

        JnomicsCompute.Client client = JnomicsThriftClient.getComputeClient();
        Authentication auth = JnomicsThriftClient.getAuthentication();
        
        JnomicsThriftJobID jobID = client.alignBowtie(cli.getOptionValue("in"),
                cli.getOptionValue("organism"),
                cli.getOptionValue("out"),
                nullToString(cli.getOptionValue("opts")),
                auth);

        System.out.println("Submitted Job: " + jobID.getJob_id());
    }

    @Override
    public String getDescription() {
        return "Run Bowtie aligner";
    }
}