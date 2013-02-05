package edu.cshl.schatz.jnomics.manager.client.old;

import edu.cshl.schatz.jnomics.manager.api.Authentication;
import edu.cshl.schatz.jnomics.manager.api.JnomicsCompute;
import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftJobID;
import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftJobStatus;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.util.List;
import java.util.Properties;

/**
 * User: james
 */

public class JobStatusHandler extends HandlerBase {

    public JobStatusHandler(){

    }

    public JnomicsArgument[] getArguments(){
        return new JnomicsArgument[]{
                new JnomicsArgument("job",true,true,"Job ID to retrieve status"),
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

        JnomicsThriftJobStatus status = client.getJobStatus(
                new JnomicsThriftJobID(cli.getOptionValue("job")),
                auth);

        System.out.printf("%30s %30s\n","ID:",status.getJob_id());
        System.out.printf("%30s %30s\n","Username:",status.getUsername());
        System.out.printf("%30s %30s\n","Complete:",status.isComplete());
        System.out.printf("%30s %30s\n","Running State:",status.getRunning_state());
        System.out.printf("%30s %30s\n","Map Progress:",status.getMapProgress());
        System.out.printf("%30s %30s\n","Reduce Progress:",status.getReduceProgress());
    }

    @Override
    public String getDescription() {
        return "Get status of a job using its handle";
    }
}