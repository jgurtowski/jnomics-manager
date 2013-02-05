package edu.cshl.schatz.jnomics.manager.client.old;

import edu.cshl.schatz.jnomics.manager.api.Authentication;
import edu.cshl.schatz.jnomics.manager.api.JnomicsCompute;
import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftJobID;
import edu.cshl.schatz.jnomics.manager.common.KBaseIDTranslator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.util.List;
import java.util.Properties;

/**
 * User: james
 */

public class SnpHandler extends HandlerBase {

    public SnpHandler(){

    }

    public JnomicsArgument[] getArguments(){
        return new JnomicsArgument[]{
                new JnomicsArgument("in",true,true,"Input file"),
                new JnomicsArgument("out",true,true,"Output dir"),
                new JnomicsArgument("organism",true,true,"Organism for alignment index")
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
        String organism = KBaseIDTranslator.translate(cli.getOptionValue("organism"));
        JnomicsThriftJobID jobID = client.snpSamtools(cli.getOptionValue("in"),
                organism,
                cli.getOptionValue("out"),
                auth);

        System.out.println("Submitted Job: " + jobID.getJob_id());
    }

    @Override
    public String getDescription() {
        return "Call SNPs with Samtools";
    }
}