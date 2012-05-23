package edu.cshl.schatz.jnomics.manager.client;

import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftJobID;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.util.List;
import java.util.Properties;

/**
 * User: james
 */

public class ClientPairReadsHandler extends ClientThriftHandler{

    public ClientPairReadsHandler(Properties properties) {
        super(properties);
    }

    public JnomicsArgument[] getArguments(){
        return new JnomicsArgument[]{
                new JnomicsArgument("1",true,true,"First file in pair"),
                new JnomicsArgument("2",true,true,"Second file in pair"),
                new JnomicsArgument("out",true,true,"Converted File")
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
        JnomicsThriftJobID jobID = getThriftClient().pairReads(cli.getOptionValue("1"),
                cli.getOptionValue("2"),cli.getOptionValue("out"),getAuth());

        closeTransport();

        System.out.println("Submitted Job: " + jobID.getJob_id());
    }

    @Override
    public String getDescription() {
        return "Join paired end read files";
    }
}