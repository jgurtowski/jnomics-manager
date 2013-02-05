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

public class BWAHandler extends HandlerBase {

    public BWAHandler(){

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
        JnomicsThriftJobID jobID = client.alignBWA(cli.getOptionValue("in"),
                organism,
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