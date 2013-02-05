package edu.cshl.schatz.jnomics.manager.client.old;

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

public class SingleReadsHandler extends HandlerBase {


    public SingleReadsHandler(){

    }

    public JnomicsArgument[] getArguments(){
        return new JnomicsArgument[]{
                new JnomicsArgument("in",true,true,"Fastq file with reads"),
                new JnomicsArgument("out",true,true,"Converted File")
        };
    }

    @Override
    public void handle(List<String> args, Properties properties) throws Exception {
        HelpFormatter formatter = new HelpFormatter();
        Options options = getOptions();
        CommandLine cli = null;
        try{
            cli = parseArguments(args);
        }catch(JnomicsArgumentException e){
            formatter.printHelp(e.toString(),options);
            return;
        }

        JnomicsCompute.Client client = JnomicsThriftClient.getComputeClient(properties);
        Authentication auth = JnomicsThriftClient.getAuthentication(properties);

        JnomicsThriftJobID jobID = client.singleReads(cli.getOptionValue("file"),
                cli.getOptionValue("out"),auth);

        System.out.println("Submitted Job: " + jobID.getJob_id());
    }

    @Override
    public String getDescription() {
        return "Convert Sequencing reads (fastq)";
    }
}