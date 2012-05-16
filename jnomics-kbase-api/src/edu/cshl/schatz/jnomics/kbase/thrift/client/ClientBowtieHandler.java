package edu.cshl.schatz.jnomics.kbase.thrift.client;

import edu.cshl.schatz.jnomics.kbase.thrift.api.JnomicsThriftJobID;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.util.List;
import java.util.Properties;

/**
 * User: james
 */

public class ClientBowtieHandler extends ClientThriftHandler{

    public ClientBowtieHandler(Properties properties) {
        super(properties);
    }

    public JnomicsArgument[] getArguments(){
        return new JnomicsArgument[]{
                new JnomicsArgument("in",true,true,"Input file"),
                new JnomicsArgument("out",true,true,"Output dir"),
                new JnomicsArgument("organism",true,true,"Organism for alignment index")
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
        JnomicsThriftJobID jobID = getThriftClient().alignBowtie(cli.getOptionValue("in"),
                cli.getOptionValue("organism"),
                cli.getOptionValue("out"),
                getAuth());

        System.out.println("Submitted Job: " + jobID.getJob_id());
    }
}