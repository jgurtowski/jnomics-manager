package edu.cshl.schatz.jnomics.kbase.thrift.client;

import edu.cshl.schatz.jnomics.kbase.thrift.api.JnomicsThriftJobID;
import edu.cshl.schatz.jnomics.kbase.thrift.api.JnomicsThriftJobStatus;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.util.List;
import java.util.Properties;

/**
 * User: james
 */

public class ClientJobStatusHandler extends ClientThriftHandler{

    public ClientJobStatusHandler(Properties properties) {
        super(properties);
    }

    public JnomicsArgument[] getArguments(){
        return new JnomicsArgument[]{
                new JnomicsArgument("job",true,true,"Job ID to retrieve status"),
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
        JnomicsThriftJobStatus status = getThriftClient().getJobStatus(
                new JnomicsThriftJobID(cli.getOptionValue("job")),
                getAuth());

        closeTransport();

        System.out.println(status);
    }
}