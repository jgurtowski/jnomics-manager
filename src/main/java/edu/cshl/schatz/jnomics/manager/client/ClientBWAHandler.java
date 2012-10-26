package edu.cshl.schatz.jnomics.manager.client;

import edu.cshl.schatz.jnomics.manager.api.Authentication;
import edu.cshl.schatz.jnomics.manager.api.JnomicsCompute;
import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftJobID;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.util.List;

/**
 * User: james
 */

public class ClientBWAHandler extends ClientHandler{

    public ClientBWAHandler(){

    }

    public JnomicsArgument[] getArguments(){
        return new JnomicsArgument[]{
                new JnomicsArgument("in",true,true,"Input file"),
                new JnomicsArgument("out",true,true,"Output dir"),
                new JnomicsArgument("organism",true,true,"Organism for alignment index"),
                new JnomicsArgument("align_opts",false,true,"Pass options to BWA align"),
                new JnomicsArgument("sampe_opts",false,true,"Pass options to BWA sampe")
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

        JnomicsThriftJobID jobID = client.alignBWA(cli.getOptionValue("in"),
                cli.getOptionValue("organism"),
                cli.getOptionValue("out"),
                nullToString(cli.getOptionValue("align_opts")),
                nullToString(cli.getOptionValue("sampe_opts")),
                auth);

        System.out.println("Submitted Job: " + jobID.getJob_id());
    }

    @Override
    public String getDescription() {
        return "Run BWA aligner";
    }
}