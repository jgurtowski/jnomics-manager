package edu.cshl.schatz.jnomics.manager.client.compute;

import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftJobID;
import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftJobStatus;
import edu.cshl.schatz.jnomics.manager.client.Utility;
import edu.cshl.schatz.jnomics.manager.client.ann.Flag;
import edu.cshl.schatz.jnomics.manager.client.ann.Parameter;

import java.util.List;
import java.util.Properties;

/**
 * User: james
 */
public class JobStatus extends ComputeBase{

    @Flag(shortForm = "-h",longForm = "--help")
    public boolean help;

    @Parameter(shortForm="-job", longForm = "--job", description = "job id")
    public String job;
    
    @Override
    public void handle(List<String> remainingArgs, Properties properties) throws Exception {
        super.handle(remainingArgs, properties);

        if(help){
            System.out.println(Utility.helpFromParameters(this.getClass()));
            return;
        }else if(null == job){
            System.out.println("Missing -job parameter");
        }else{
            JnomicsThriftJobStatus status = client.getJobStatus(new JnomicsThriftJobID(job), auth);

            System.out.printf("%30s %30s\n","ID:",status.getJob_id());
            System.out.printf("%30s %30s\n","Username:",status.getUsername());
            System.out.printf("%30s %30s\n","Complete:",status.isComplete());
            System.out.printf("%30s %30s\n","Running State:",status.getRunning_state());
            System.out.printf("%30s %30s\n","Map Progress:",status.getMapProgress());
            System.out.printf("%30s %30s\n","Reduce Progress:",status.getReduceProgress());
            return;
        }

        System.out.println(Utility.helpFromParameters(this.getClass()));
    }
}
