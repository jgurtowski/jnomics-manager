package edu.cshl.schatz.jnomics.manager.client;

import edu.cshl.schatz.jnomics.manager.api.Authentication;
import edu.cshl.schatz.jnomics.manager.api.JnomicsCompute;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.util.List;

/**
 * User: james
 */

public class ClientMergeVCFHandler extends ClientHandler{


    public JnomicsArgument[] getArguments(){
        return new JnomicsArgument[]{
                new JnomicsArgument("in",true,true,"Input file"),
                new JnomicsArgument("alignments", true,true,"Alignment output directory"),
                new JnomicsArgument("out",true,true,"output.vcf"),
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

        if(client.mergeVCF(cli.getOptionValue("in"),
                cli.getOptionValue("alignments"),
                cli.getOptionValue("out"),auth)){
            System.out.println("Successfully merged into : " + cli.getOptionValue("out"));
        }else{
            System.out.println("Failed to Merge : "+  cli.getOptionValue("out"));
        }
    }

    @Override
    public String getDescription() {
        return "Merge VCF files";
    }
}